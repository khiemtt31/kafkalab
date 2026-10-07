import { addEvent, cloneSimulationState, getConsumer, getGroup, result } from "./simulation";
import type {
  CommitMode,
  ConsumerGroup,
  KafkaSimulationState,
  SimulationEvent,
  SimulationResult,
} from "./types";

export function getCommittedOffset(state: KafkaSimulationState, groupId: string, partition: number): number {
  return getGroup(state, groupId)?.committedOffsets[partition] ?? 0;
}

export function getCurrentPosition(state: KafkaSimulationState, groupId: string, consumerId: string, partition: number): number {
  return getConsumer(state, groupId, consumerId)?.currentPositions[partition] ?? getCommittedOffset(state, groupId, partition);
}

// @CommitOffset
// Kafka stores the next offset a group should read. Committing 11 means that
// offsets 0..10 are considered handled by that group.
export function commitOffsets(
  state: KafkaSimulationState,
  groupId: string,
  consumerId: string,
  mode: CommitMode = "processed",
  partitionIds?: number[],
): SimulationResult<Record<number, number>> {
  const next = cloneSimulationState(state);
  const group = getGroup(next, groupId);
  const consumer = getConsumer(next, groupId, consumerId);
  if (!group) throw new Error(`Consumer group ${groupId} does not exist`);
  if (!consumer) throw new Error(`Consumer ${consumerId} does not exist in ${groupId}`);
  const selectedPartitions = partitionIds?.length ? [...partitionIds].sort((a, b) => a - b) : [...consumer.assignedPartitions].sort((a, b) => a - b);
  const committed: Record<number, number> = {};
  const events: SimulationEvent[] = [];

  for (const partition of selectedPartitions) {
    if (group.assignments[partition] !== consumer.id) continue;
    const before = group.committedOffsets[partition] ?? 0;
    const requested = mode === "current" ? consumer.currentPositions[partition] ?? before : contiguousProcessedOffset(consumer, partition, before);
    const latestAttempts = new Map(next.deliveries.filter((delivery) => delivery.groupId === groupId && delivery.partition === partition)
      .map((delivery) => [delivery.offset, delivery]));
    // @Simplification
    // This lab prevents both commit modes from skipping an explicitly failed
    // application attempt. Kafka commits do not enforce business success;
    // the separate commit-too-soon lesson still illustrates skipping pending work.
    const target = [...latestAttempts.values()].reduce((limit, delivery) => delivery.applicationStatus === "failed"
      && delivery.offset >= before && delivery.offset < limit ? delivery.offset : limit, requested);
    if (target < requested) {
      const failed = latestAttempts.get(target)!;
      events.push(addEvent(next, "offset-commit-blocked",
        `Lab safety policy blocked committing past failed offset ${target}. Retry the record first; Kafka itself permits explicit commits past failed work.`,
        "lab application safety policy; Kafka commits do not prove application success", failed.messageId,
        { groupId, consumerId, partition, previous: before, requestedOffset: requested, blockedOffset: target, messageId: failed.messageId, mode }));
    }
    if (target <= before) continue;

    // @Simplification
    // A UI action can explicitly choose "current" to demonstrate at-most-once
    // behavior. Real applications normally make this choice in their consumer
    // processing/commit code, not via a simulator button.
    if (mode === "current") {
      markUndeliveredAsLost(next, group, consumer.id, partition, before, target, events);
    }
    group.committedOffsets[partition] = target;
    committed[partition] = target;
    for (const delivery of next.deliveries) {
      if (delivery.groupId === groupId && delivery.partition === partition && delivery.offset < target
        && delivery.status !== "lost" && delivery.applicationStatus !== "failed") {
        delivery.status = "committed";
        delivery.committedAt = next.clock;
      }
    }
    consumer.processedOffsets[partition] = (consumer.processedOffsets[partition] ?? []).filter((offset) => offset >= target);
    consumer.pendingOffsets[partition] = (consumer.pendingOffsets[partition] ?? []).filter((offset) => offset >= target);
    events.push(
      addEvent(
        next,
        "offset-committed",
        `${groupId} committed ${next.topic.name}[${partition}] → next offset ${target}`,
        "consumer-group committed offset",
        consumer.id,
        { groupId, consumerId: consumer.id, partition, previous: before, committedOffset: target, mode },
      ),
    );
  }
  return result(next, committed, events);
}

export function commitProcessedOffsets(
  state: KafkaSimulationState,
  groupId: string,
  consumerId: string,
  partitionIds?: number[],
): SimulationResult<Record<number, number>> {
  return commitOffsets(state, groupId, consumerId, "processed", partitionIds);
}

export function commitCurrentOffsets(
  state: KafkaSimulationState,
  groupId: string,
  consumerId: string,
  partitionIds?: number[],
): SimulationResult<Record<number, number>> {
  return commitOffsets(state, groupId, consumerId, "current", partitionIds);
}

function contiguousProcessedOffset(consumer: NonNullable<ReturnType<typeof getConsumer>>, partition: number, from: number): number {
  const processed = new Set(consumer.processedOffsets[partition] ?? []);
  let next = from;
  while (processed.has(next)) next += 1;
  return next;
}

function markUndeliveredAsLost(
  state: KafkaSimulationState,
  group: ConsumerGroup,
  consumerId: string,
  partition: number,
  from: number,
  to: number,
  events: SimulationEvent[],
): void {
  const latestAttempts = new Map(state.deliveries.filter((delivery) => delivery.groupId === group.id && delivery.partition === partition)
    .map((delivery) => [delivery.offset, delivery]));
  for (const delivery of state.deliveries) {
    if (
      delivery.groupId === group.id &&
      delivery.consumerId === consumerId &&
      delivery.partition === partition &&
      delivery.offset >= from &&
      delivery.offset < to &&
      delivery.processedAt === undefined &&
      delivery.applicationStatus === "pending" &&
      latestAttempts.get(delivery.offset)?.id === delivery.id &&
      (delivery.status === "read" || delivery.status === "duplicate")
    ) {
      delivery.status = "lost";
      events.push(
        addEvent(
          state,
          "lost-delivery",
          `Offset ${delivery.offset} was committed before processing`,
          "at-most-once processing can lose work",
          delivery.messageId,
          { groupId: group.id, consumerId, partition, offset: delivery.offset },
        ),
      );
    }
  }
}

export function getGroupCommittedOffsets(state: KafkaSimulationState, groupId: string): Record<number, number> {
  return { ...(getGroup(state, groupId)?.committedOffsets ?? {}) };
}
