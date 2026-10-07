import { addEvent, cloneSimulationState, getConsumer, getGroup, nextId, result } from "./simulation";
import { produceMessage } from "./producer";
import { rebalanceGroup } from "./rebalance";
import type {
  BatchSettings,
  AdvancedPolicies,
  HeartbeatState,
  KafkaSimulationState,
  ProducedMessageInput,
  SchemaDefinition,
  SimulationEvent,
  SimulationResult,
  StreamTopology,
  TransactionState,
} from "./types";

// @KafkaConcept
// Transactions, heartbeats, batching, compression, schemas, and streams are
// represented as explorable educational state. They do not claim to implement
// Kafka's distributed protocols or a Schema Registry server.
export function beginTransaction(state: KafkaSimulationState, producerId: string, transactionId?: string): SimulationResult<TransactionState> {
  const next = cloneSimulationState(state);
  const transaction: TransactionState = {
    id: transactionId ?? nextId(next, "transaction", "transaction"),
    producerId,
    status: "open",
    messageIds: [],
    startedAt: next.clock,
  };
  next.advanced.transactions.push(transaction);
  const event = addEvent(next, "transaction-started", `${transaction.id} started`, "Kafka transaction boundary", transaction.id, { producerId });
  return result(next, transaction, [event]);
}

export function appendTransactionalMessage(
  state: KafkaSimulationState,
  transactionId: string,
  input: ProducedMessageInput,
): SimulationResult<{ transaction: TransactionState; message: ReturnType<typeof produceMessage>["value"] }> {
  const transaction = state.advanced.transactions.find((candidate) => candidate.id === transactionId);
  if (!transaction || transaction.status !== "open") throw new Error(`Transaction ${transactionId} is not open`);
  const produced = produceMessage(state, { ...input, producerId: transaction.producerId });
  const next = cloneSimulationState(produced.state);
  const owned = next.advanced.transactions.find((candidate) => candidate.id === transactionId);
  if (!owned) throw new Error(`Transaction ${transactionId} does not exist`);
  owned.messageIds.push(produced.value.id);
  return result(next, { transaction: owned, message: produced.value }, produced.events);
}

export function commitTransaction(state: KafkaSimulationState, transactionId: string): SimulationResult<TransactionState> {
  return finishTransaction(state, transactionId, "committed", "transaction-committed", "Kafka transaction commit");
}

export function abortTransaction(state: KafkaSimulationState, transactionId: string): SimulationResult<TransactionState> {
  return finishTransaction(state, transactionId, "aborted", "transaction-aborted", "Kafka transaction abort");
}

function finishTransaction(
  state: KafkaSimulationState,
  transactionId: string,
  status: "committed" | "aborted",
  eventType: "transaction-committed" | "transaction-aborted",
  concept: string,
): SimulationResult<TransactionState> {
  const next = cloneSimulationState(state);
  const transaction = next.advanced.transactions.find((candidate) => candidate.id === transactionId);
  if (!transaction || transaction.status !== "open") throw new Error(`Transaction ${transactionId} is not open`);
  transaction.status = status;
  transaction.completedAt = next.clock;
  const event = addEvent(next, eventType, `${transactionId} ${status}`, concept, transactionId, { producerId: transaction.producerId, messageCount: transaction.messageIds.length });
  return result(next, transaction, [event]);
}

export function recordHeartbeat(
  state: KafkaSimulationState,
  groupId: string,
  consumerId: string,
  options: Partial<Pick<HeartbeatState, "intervalMs" | "sessionTimeoutMs">> = {},
): SimulationResult<HeartbeatState> {
  const next = cloneSimulationState(state);
  const consumer = getConsumer(next, groupId, consumerId);
  const group = getGroup(next, groupId);
  if (!consumer || !group) throw new Error(`Consumer ${consumerId} does not exist in ${groupId}`);
  const existing = next.advanced.heartbeats.find((heartbeat) => heartbeat.consumerId === consumerId && heartbeat.groupId === groupId);
  const heartbeat: HeartbeatState = existing ?? {
    consumerId,
    groupId,
    lastHeartbeatAt: next.clock,
    intervalMs: options.intervalMs ?? 3000,
    sessionTimeoutMs: options.sessionTimeoutMs ?? next.advanced.policies.sessionTimeoutMs,
    missedHeartbeats: 0,
  };
  heartbeat.lastHeartbeatAt = next.clock;
  heartbeat.intervalMs = options.intervalMs ?? heartbeat.intervalMs;
  heartbeat.sessionTimeoutMs = options.sessionTimeoutMs ?? heartbeat.sessionTimeoutMs;
  heartbeat.missedHeartbeats = 0;
  if (!existing) next.advanced.heartbeats.push(heartbeat);
  const event = addEvent(next, "heartbeat-recorded", `${consumerId} heartbeat recorded`, "consumer group liveness", consumerId, { groupId, intervalMs: heartbeat.intervalMs });
  return result(next, heartbeat, [event]);
}

export function checkHeartbeats(state: KafkaSimulationState): SimulationResult<HeartbeatState[]> {
  let next = cloneSimulationState(state);
  const timedOutGroups = new Set<string>();
  for (const heartbeat of next.advanced.heartbeats) {
    const elapsed = next.clock - heartbeat.lastHeartbeatAt;
    heartbeat.missedHeartbeats = Math.max(0, Math.floor(elapsed / heartbeat.intervalMs));
    if (elapsed >= Math.min(heartbeat.sessionTimeoutMs, next.advanced.policies.sessionTimeoutMs)) {
      const consumer = getConsumer(next, heartbeat.groupId, heartbeat.consumerId);
      if (consumer?.status === "running") {
        consumer.status = "dead";
        addEvent(next, "consumer-timeout", `${heartbeat.consumerId} exceeded session timeout`, "heartbeat session timeout", heartbeat.consumerId, {
          groupId: heartbeat.groupId,
          sessionTimeoutMs: heartbeat.sessionTimeoutMs,
          elapsed,
        });
        timedOutGroups.add(heartbeat.groupId);
      }
    }
  }
  for (const group of next.groups) {
    for (const consumer of group.consumers) {
      if (consumer.status !== "running" || next.clock - consumer.lastPollAt < next.advanced.policies.maxPollIntervalMs) continue;
      consumer.status = "dead";
      addEvent(next, "consumer-timeout", `${consumer.id} exceeded max poll interval`, "consumer max.poll.interval.ms", consumer.id, {
        groupId: group.id,
        maxPollIntervalMs: next.advanced.policies.maxPollIntervalMs,
        elapsed: next.clock - consumer.lastPollAt,
      });
      timedOutGroups.add(group.id);
    }
  }
  for (const groupId of timedOutGroups) {
    next = rebalanceGroup(next, groupId, "consumer timeout").state;
  }
  return result(next, next.advanced.heartbeats);
}

export function configureBatching(state: KafkaSimulationState, settings: Partial<BatchSettings>): SimulationResult<BatchSettings> {
  const next = cloneSimulationState(state);
  next.advanced.batchSettings = {
    ...next.advanced.batchSettings,
    ...settings,
    batchSize: Math.max(1, Math.floor(settings.batchSize ?? next.advanced.batchSettings.batchSize)),
    lingerMs: Math.max(0, Math.floor(settings.lingerMs ?? next.advanced.batchSettings.lingerMs)),
  };
  const event = addEvent(next, "batch-settings-updated", `Batch settings updated (${next.advanced.batchSettings.compression})`, "producer batching and compression", "producer-1", {
    batchSize: next.advanced.batchSettings.batchSize,
    lingerMs: next.advanced.batchSettings.lingerMs,
    compression: next.advanced.batchSettings.compression,
  });
  return result(next, next.advanced.batchSettings, [event]);
}

export function configureAdvancedPolicies(state: KafkaSimulationState, settings: Partial<AdvancedPolicies>): SimulationResult<AdvancedPolicies> {
  const next = cloneSimulationState(state);
  next.advanced.policies = {
    ...next.advanced.policies,
    ...settings,
    retries: Math.max(0, Math.min(20, Math.floor(settings.retries ?? next.advanced.policies.retries))),
    retentionMs: Math.max(0, Math.floor(settings.retentionMs ?? next.advanced.policies.retentionMs)),
    sessionTimeoutMs: Math.max(1000, Math.floor(settings.sessionTimeoutMs ?? next.advanced.policies.sessionTimeoutMs)),
    maxPollIntervalMs: Math.max(1000, Math.floor(settings.maxPollIntervalMs ?? next.advanced.policies.maxPollIntervalMs)),
    deadLetterTopic: (settings.deadLetterTopic ?? next.advanced.policies.deadLetterTopic).trim() || "orders.DLT",
  };
  const event = addEvent(next, "advanced-policy-updated", "Advanced delivery and liveness policies updated", "retry, retention, compaction, DLT, and timeout concepts", "simulation", {
    retries: next.advanced.policies.retries,
    idempotence: next.advanced.policies.idempotence,
    compaction: next.advanced.policies.compaction,
    exactlyOnce: next.advanced.policies.exactlyOnce,
    cooperativeRebalance: next.advanced.policies.cooperativeRebalance,
  });
  return result(next, next.advanced.policies, [event]);
}

export function registerSchema(state: KafkaSimulationState, schema: Omit<SchemaDefinition, "version"> & { version?: number }): SimulationResult<SchemaDefinition> {
  const next = cloneSimulationState(state);
  const previous = next.advanced.schemas.filter((candidate) => candidate.subject === schema.subject).sort((left, right) => right.version - left.version)[0];
  const definition: SchemaDefinition = {
    subject: schema.subject,
    version: schema.version ?? (previous ? previous.version + 1 : 1),
    fields: [...schema.fields],
    compatibility: schema.compatibility,
  };
  next.advanced.schemas.push(definition);
  const event = addEvent(next, "schema-registered", `${definition.subject} schema v${definition.version} registered`, "schema evolution concept", definition.subject, { version: definition.version, compatibility: definition.compatibility });
  return result(next, definition, [event]);
}

export function createStreamTopology(
  state: KafkaSimulationState,
  input: Omit<StreamTopology, "id"> & { id?: string },
): SimulationResult<StreamTopology> {
  const next = cloneSimulationState(state);
  const topology: StreamTopology = {
    id: input.id ?? nextId(next, "topology", "topology"),
    sourceTopics: [...input.sourceTopics],
    sinkTopic: input.sinkTopic,
    operations: [...input.operations],
  };
  next.advanced.streamTopologies.push(topology);
  const event = addEvent(next, "stream-topology-created", `${topology.id} topology created`, "Kafka Streams processing topology", topology.id, { sinkTopic: topology.sinkTopic, operations: topology.operations });
  return result(next, topology, [event]);
}
