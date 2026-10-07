import { addEvent, addDelivery, cloneSimulationState, getConsumer, getGroup, getMessage, nextId, result } from "./simulation";
import { rebalanceGroup } from "./rebalance";
import { commitOffsets } from "./offsets";
import type {
  Consumer,
  DeliveryAttempt,
  KafkaMessage,
  KafkaSimulationState,
  PollOptions,
  SimulationEvent,
  SimulationResult,
} from "./types";

export type ProcessOptions = {
  partition?: number;
  offset?: number;
};

// @Consumer
// This is the conceptual equivalent of consumer.poll(...). It advances only a
// consumer's current position. It does not commit the group offset.
export function pollConsumer(
  state: KafkaSimulationState,
  groupId: string,
  consumerId: string,
  options: PollOptions = {},
): SimulationResult<KafkaMessage[]> {
  const next = cloneSimulationState(state);
  const group = getGroup(next, groupId);
  const consumer = getConsumer(next, groupId, consumerId);
  if (!group) throw new Error(`Consumer group ${groupId} does not exist`);
  if (!consumer) throw new Error(`Consumer ${consumerId} does not exist in ${groupId}`);
  if (consumer.status !== "running") return result(next, []);

  const maxRecords = Math.max(1, Math.floor(options.maxRecords ?? 1));
  const partitions = options.partition === undefined
    ? [...consumer.assignedPartitions].sort((left, right) => left - right)
    : consumer.assignedPartitions.includes(options.partition) ? [options.partition] : [];
  const messages: KafkaMessage[] = [];
  const events: SimulationEvent[] = [];

  for (const partitionId of partitions) {
    if (messages.length >= maxRecords) break;
    const current = consumer.currentPositions[partitionId] ?? group.committedOffsets[partitionId] ?? 0;
    const message = getMessage(next, partitionId, current);
    if (!message) continue;
    if (group.assignments[partitionId] !== consumerId) continue;
    // A recorded application failure requires an explicit retry. Normal
    // playback must not silently turn the failed attempt into success.
    if (getLatestDeliveryAttempt(next, groupId, partitionId, message.offset)?.applicationStatus === "failed") continue;
    consumer.currentPositions[partitionId] = current + 1;
    consumer.pendingOffsets[partitionId] ??= [];
    if (!consumer.pendingOffsets[partitionId].includes(message.offset)) {
      consumer.pendingOffsets[partitionId].push(message.offset);
    }
    const previous = next.deliveries.filter(
      (delivery) => delivery.groupId === groupId && delivery.partition === message.partition && delivery.offset === message.offset,
    );
    const attempt = Math.max(0, ...previous.map((delivery) => delivery.attempt)) + 1;
    const delivery: DeliveryAttempt = {
      id: nextId(next, "delivery", "delivery"),
      groupId,
      consumerId,
      messageId: message.id,
      partition: message.partition,
      offset: message.offset,
      attempt,
      status: attempt > 1 ? "duplicate" as const : "read" as const,
      applicationStatus: "pending",
      readAt: next.clock,
    };
    addDelivery(next, delivery);
    messages.push(message);
    events.push(
      addEvent(
        next,
        "consumer-polled",
        `${consumerId} read ${next.topic.name}[${message.partition}] offset ${message.offset}`,
        "consumer poll and current processing position",
        consumerId,
        { groupId, consumerId, messageId: message.id, partition: message.partition, offset: message.offset, attempt },
      ),
    );
    if (attempt > 1) {
      events.push(
        addEvent(
          next,
          "duplicate-delivery",
          `${message.id} was delivered again (attempt ${attempt})`,
          "at-least-once processing can duplicate work",
          message.id,
          { groupId, consumerId, partition: message.partition, offset: message.offset, attempt },
        ),
      );
    }
  }
  consumer.lastPollAt = next.clock;
  return result(next, messages, events);
}

// @Consumer
// Processing is intentionally separate from reading and committing so the
// learner can pause between those steps and model delivery failures.
export function processRecords(
  state: KafkaSimulationState,
  groupId: string,
  consumerId: string,
  options: ProcessOptions = {},
): SimulationResult<KafkaMessage[]> {
  const next = cloneSimulationState(state);
  const consumer = getConsumer(next, groupId, consumerId);
  const group = getGroup(next, groupId);
  if (!group) throw new Error(`Consumer group ${groupId} does not exist`);
  if (!consumer) throw new Error(`Consumer ${consumerId} does not exist in ${groupId}`);
  if (consumer.status !== "running") return result(next, []);
  const partitions = options.partition === undefined
    ? Object.keys(consumer.pendingOffsets).map(Number).sort((left, right) => left - right)
    : [options.partition];
  const processed: KafkaMessage[] = [];
  const events: SimulationEvent[] = [];
  for (const partition of partitions) {
    if (group.assignments[partition] !== consumerId || !consumer.assignedPartitions.includes(partition)) continue;
    const pending = consumer.pendingOffsets[partition] ?? [];
    const offsets = options.offset === undefined ? [...pending] : pending.filter((offset) => offset === options.offset);
    for (const offset of offsets) {
      const message = getMessage(next, partition, offset);
      if (!message) continue;
      if (offset < (group.committedOffsets[partition] ?? 0)) continue;
      const delivery = latestDelivery(next, groupId, consumerId, partition, offset);
      if (!delivery || delivery.applicationStatus !== "pending") continue;
      if (getLatestDeliveryAttempt(next, groupId, partition, offset)?.id !== delivery.id) continue;
      consumer.pendingOffsets[partition] = (consumer.pendingOffsets[partition] ?? []).filter((candidate) => candidate !== offset);
      consumer.processedOffsets[partition] ??= [];
      if (!consumer.processedOffsets[partition].includes(offset)) consumer.processedOffsets[partition].push(offset);
      if (delivery) {
        delivery.status = "processed";
        delivery.applicationStatus = "succeeded";
        delivery.failureReason = undefined;
        delivery.processedAt = next.clock;
      }
      processed.push(message);
      events.push(
        addEvent(
          next,
          "record-processed",
          `${consumerId} processed offset ${offset}`,
          "record processing is separate from offset commit",
          message.id,
          { groupId, consumerId, messageId: message.id, partition, offset, attempt: delivery.attempt },
        ),
      );
    }
  }
  return result(next, processed, events);
}

export function processRecord(
  state: KafkaSimulationState,
  groupId: string,
  consumerId: string,
  partition: number,
  offset?: number,
): SimulationResult<KafkaMessage[]> {
  return processRecords(state, groupId, consumerId, { partition, offset });
}

export type RecordProcessingTarget = { partition: number; offset: number };

function ownedUncommittedRecord(state: KafkaSimulationState, groupId: string, consumerId: string, target: RecordProcessingTarget) {
  if (!Number.isInteger(target.partition) || target.partition < 0 || !Number.isInteger(target.offset) || target.offset < 0) return undefined;
  const group = getGroup(state, groupId);
  const consumer = getConsumer(state, groupId, consumerId);
  const message = getMessage(state, target.partition, target.offset);
  if (!group || !consumer || !message || consumer.status !== "running"
    || group.assignments[target.partition] !== consumerId || !consumer.assignedPartitions.includes(target.partition)
    || target.offset < (group.committedOffsets[target.partition] ?? 0)) return undefined;
  return { group, consumer, message };
}

/** An application failure keeps the record, read position, and pending offset. */
export function failRecordProcessing(
  state: KafkaSimulationState,
  groupId: string,
  consumerId: string,
  target: RecordProcessingTarget & { reason: string },
): SimulationResult<DeliveryAttempt | undefined> {
  const owned = ownedUncommittedRecord(state, groupId, consumerId, target);
  const previous = getLatestDeliveryAttempt(state, groupId, target.partition, target.offset);
  if (!owned || !owned.consumer.pendingOffsets[target.partition]?.includes(target.offset)
    || !previous || previous.consumerId !== consumerId || previous.messageId !== owned.message.id
    || previous.applicationStatus !== "pending") return result(state, undefined);

  const next = cloneSimulationState(state);
  const delivery = getLatestDeliveryAttempt(next, groupId, target.partition, target.offset)!;
  const reason = target.reason.replace(/[\u0000-\u001f\u007f]/g, " ").trim().slice(0, 160) || "Application processing failed";
  delivery.applicationStatus = "failed";
  delivery.status = "failed";
  delivery.failureReason = reason;
  const event = addEvent(next, "record-processing-failed", `${consumerId} failed to process offset ${target.offset}: ${reason}`,
    "application processing failure does not delete records or commit group progress", delivery.messageId,
    { groupId, consumerId, messageId: delivery.messageId, partition: target.partition, offset: target.offset, attempt: delivery.attempt, reason });
  return result(next, delivery, [event]);
}

/** Retry creates a new delivery attempt. Processing and commit remain separate. */
export function retryRecordProcessing(
  state: KafkaSimulationState,
  groupId: string,
  consumerId: string,
  target: RecordProcessingTarget,
): SimulationResult<DeliveryAttempt | undefined> {
  const owned = ownedUncommittedRecord(state, groupId, consumerId, target);
  const failed = getLatestDeliveryAttempt(state, groupId, target.partition, target.offset);
  if (!owned || !failed || failed.messageId !== owned.message.id || failed.applicationStatus !== "failed") return result(state, undefined);

  const next = cloneSimulationState(state);
  const consumer = getConsumer(next, groupId, consumerId)!;
  consumer.pendingOffsets[target.partition] ??= [];
  if (!consumer.pendingOffsets[target.partition].includes(target.offset)) consumer.pendingOffsets[target.partition].push(target.offset);
  consumer.currentPositions[target.partition] = Math.max(consumer.currentPositions[target.partition] ?? 0, target.offset + 1);
  const delivery: DeliveryAttempt = {
    id: nextId(next, "delivery", "delivery"), groupId, consumerId, messageId: owned.message.id,
    partition: target.partition, offset: target.offset, attempt: failed.attempt + 1,
    status: "read", applicationStatus: "pending", readAt: next.clock,
  };
  addDelivery(next, delivery);
  const event = addEvent(next, "record-processing-retried", `${consumerId} retried offset ${target.offset} (attempt ${delivery.attempt})`,
    "retry the same stored record with a separate application delivery attempt", delivery.messageId,
    { groupId, consumerId, messageId: delivery.messageId, partition: target.partition, offset: target.offset, attempt: delivery.attempt, previousAttempt: failed.attempt });
  return result(next, delivery, [event]);
}

/** Latest attempt across consumers for one record in one independent group. */
export function getLatestDeliveryAttempt(state: KafkaSimulationState, groupId: string, partition: number, offset: number) {
  for (let index = state.deliveries.length - 1; index >= 0; index -= 1) {
    const delivery = state.deliveries[index];
    if (delivery.groupId === groupId && delivery.partition === partition && delivery.offset === offset) return delivery;
  }
  return undefined;
}

export function pauseConsumer(state: KafkaSimulationState, groupId: string, consumerId: string): SimulationResult<Consumer> {
  return setConsumerStatus(state, groupId, consumerId, "paused", "consumer-paused", "paused");
}

export function resumeConsumer(state: KafkaSimulationState, groupId: string, consumerId: string): SimulationResult<Consumer> {
  return setConsumerStatus(state, groupId, consumerId, "running", "consumer-resumed", "resumed");
}

function setConsumerStatus(
  state: KafkaSimulationState,
  groupId: string,
  consumerId: string,
  status: Consumer["status"],
  eventType: "consumer-paused" | "consumer-resumed",
  label: string,
): SimulationResult<Consumer> {
  const next = cloneSimulationState(state);
  const consumer = getConsumer(next, groupId, consumerId);
  if (!consumer) throw new Error(`Consumer ${consumerId} does not exist in ${groupId}`);
  if (consumer.status === "dead") throw new Error(`Consumer ${consumerId} is dead; restart it first`);
  consumer.status = status;
  const event = addEvent(next, eventType, `${consumerId} ${label}`, "consumer lifecycle", consumerId, { groupId, consumerId });
  return result(next, consumer, [event]);
}

export function crashConsumer(state: KafkaSimulationState, groupId: string, consumerId: string): SimulationResult<Consumer> {
  const next = cloneSimulationState(state);
  const consumer = getConsumer(next, groupId, consumerId);
  if (!consumer) throw new Error(`Consumer ${consumerId} does not exist in ${groupId}`);
  consumer.status = "dead";
  const crashEvent = addEvent(next, "consumer-crashed", `${consumerId} crashed`, "failure before or after commit changes delivery semantics", consumerId, {
    groupId,
    consumerId,
  });
  const group = getGroup(next, groupId);
  if (group) {
    for (const partition of consumer.assignedPartitions) {
      const committed = group.committedOffsets[partition] ?? 0;
      const current = consumer.currentPositions[partition] ?? committed;
      if (current <= committed) continue;
      const processed = new Set(consumer.processedOffsets[partition] ?? []);
      for (const offset of Array.from({ length: current - committed }, (_, index) => committed + index)) {
        const delivery = latestDelivery(next, groupId, consumerId, partition, offset);
        if (delivery && processed.has(offset) && delivery.status !== "committed") {
          // This is a candidate for a duplicate when the consumer restarts.
          delivery.status = "processed";
        }
      }
    }
  }
  const rebalanced = rebalanceGroup(next, groupId, "consumer crashed");
  return result(rebalanced.state, rebalanced.state.groups.find((candidate) => candidate.id === groupId)?.consumers.find((candidate) => candidate.id === consumerId) ?? consumer, [crashEvent, ...rebalanced.events]);
}

export function restartConsumer(state: KafkaSimulationState, groupId: string, consumerId: string): SimulationResult<Consumer> {
  const next = cloneSimulationState(state);
  const consumer = getConsumer(next, groupId, consumerId);
  const group = getGroup(next, groupId);
  if (!consumer || !group) throw new Error(`Consumer ${consumerId} does not exist in ${groupId}`);
  consumer.status = "running";
  consumer.pendingOffsets = {};
  consumer.processedOffsets = {};
  // The crash rebalance revoked the old assignment. Reset every position to
  // the group's committed next offset before the new assignment is created;
  // otherwise an uncommitted record would be skipped on restart.
  consumer.currentPositions = Object.fromEntries(
    next.topic.partitions.map((partition) => [partition.id, group.committedOffsets[partition.id] ?? 0]),
  );
  const restartEvent = addEvent(next, "consumer-restarted", `${consumerId} restarted`, "uncommitted work may be delivered again", consumerId, { groupId, consumerId });
  const rebalanced = rebalanceGroup(next, groupId, "consumer restarted");
  const restarted = rebalanced.state.groups.find((candidate) => candidate.id === groupId)?.consumers.find((candidate) => candidate.id === consumerId) ?? consumer;
  return result(rebalanced.state, restarted, [restartEvent, ...rebalanced.events]);
}

export function consumeOne(
  state: KafkaSimulationState,
  groupId: string,
  consumerId: string,
  commit = true,
): SimulationResult<KafkaMessage | undefined> {
  const polled = pollConsumer(state, groupId, consumerId, { maxRecords: 1 });
  if (polled.value.length === 0) return result(polled.state, undefined, polled.events);
  const processed = processRecords(polled.state, groupId, consumerId);
  const committed = commit ? commitOffsets(processed.state, groupId, consumerId, "processed") : { state: processed.state, value: {}, events: [] };
  return result(committed.state, polled.value[0], [...polled.events, ...processed.events, ...committed.events]);
}

function latestDelivery(state: KafkaSimulationState, groupId: string, consumerId: string, partition: number, offset: number) {
  for (let index = state.deliveries.length - 1; index >= 0; index -= 1) {
    const delivery = state.deliveries[index];
    if (delivery.groupId === groupId && delivery.consumerId === consumerId && delivery.partition === partition && delivery.offset === offset) return delivery;
  }
  return undefined;
}
