"use client";

import { create } from "zustand";
import {
  addConsumer,
  createGroup,
  removeConsumer,
} from "@/core/kafka/rebalance";
import {
  crashConsumer,
  consumeOne,
  pollConsumer,
  processRecords,
  retryRecordProcessing as retryProcessing,
  pauseConsumer,
  restartConsumer,
  resumeConsumer,
} from "@/core/kafka/consumer";
import { commitProcessedOffsets, commitCurrentOffsets } from "@/core/kafka/offsets";
import { getGroupLag } from "@/core/kafka/lag";
import { addProducer as addProducerInSimulation, produceMessage } from "@/core/kafka/producer";
import { canProduceWithAcks, createBrokerCluster, failBroker as failBrokerInCluster, recoverBroker, setRequiredAcks } from "@/core/kafka/cluster";
import { selectPartition } from "@/core/kafka/partitioner";
import { createReferenceFlow } from "@/core/kafka/reference-flow";
import { appendTransactionalMessage, beginTransaction, commitTransaction, abortTransaction, configureAdvancedPolicies, configureBatching, createStreamTopology, recordHeartbeat, registerSchema, checkHeartbeats } from "@/core/kafka/advanced";
import { getScenario, loadScenario, scenarioDefinitions } from "@/scenarios";
import {
  addEvent,
  advanceClock,
  appendMetric,
  cloneSimulationState,
  createSimulationState,
  getGroup as findGroup,
} from "@/core/kafka/simulation";
import type { DeliveryAttempt, KafkaSimulationState, ProducedMessageInput, SimulationEvent } from "@/core/kafka/types";
import type {
  InspectorTarget,
  KafkaLabActions,
  KafkaLabSnapshot,
  KafkaLabStore,
  LabConsumer,
  LabConsumerGroup,
  LabMessage,
  LabPartition,
  LabScenario,
  ProduceInput,
  FlowLayout,
  PracticeCategory,
  PracticeProgress,
} from "@/hooks/useKafkaLab";

const DEFAULT_GROUP = "analytics-group";
const MAX_PRODUCERS = 8;
const MAX_CONSUMERS_PER_GROUP = 12;
const TICK_MS = 100;

const scenarioCatalog: LabScenario[] = scenarioDefinitions.map((scenario) => ({
  id: scenario.id,
  name: scenario.name,
  description: scenario.description,
  learningGoals: scenario.learningGoals,
  goal: scenario.goal,
  action: scenario.action,
  observation: scenario.observation,
  explanation: scenario.explanation,
}));

const consumerRates = new Map<string, number>();
const consumerAutoCommit = new Map<string, boolean>();
const consumerRemainders = new Map<string, number>();
let producerRemainder = 0;
let timer: ReturnType<typeof setInterval> | undefined;

function stopTimer(): void {
  if (timer !== undefined) clearInterval(timer);
  timer = undefined;
}

function resetRuntimeState(): void {
  stopTimer();
  consumerRates.clear();
  consumerAutoCommit.clear();
  consumerRemainders.clear();
  producerRemainder = 0;
}

type StoreState = KafkaLabStore & { simulation: KafkaSimulationState; selectedGroupId: string; flowLayout: FlowLayout; practiceProgress: PracticeProgress; hydrated: boolean; storageNotice?: string };
const STORAGE_KEY = "kafkalab:workspace:v1";
const STORAGE_VERSION = 1;
const emptyPracticeProgress = (): PracticeProgress => ({ selectedCategory: null, currentQuestion: {}, answers: {} });
const emptyFlowLayout = (): FlowLayout => ({ positions: {}, viewport: null, views: {} });

type PersistedWorkspace = {
  version: number;
  simulation: KafkaSimulationState;
  selectedGroupId: string;
  controls: { producerRate: number; simulationSpeed: number; activeScenarioId?: string; inspector?: InspectorTarget };
  consumerRates: Array<[string, number]>;
  consumerAutoCommit: Array<[string, boolean]>;
  consumerRemainders: Array<[string, number]>;
  producerRemainder: number;
  flowLayout: FlowLayout;
  practiceProgress: PracticeProgress;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const isFiniteNumber = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);
const isString = (value: unknown): value is string => typeof value === "string";
const isArrayOfRecords = (value: unknown, validate: (item: Record<string, unknown>) => boolean) => Array.isArray(value) && value.every((item) => isRecord(item) && validate(item));
const isNumberMap = (value: unknown) => isRecord(value) && Object.values(value).every(isFiniteNumber);
const isNumberListMap = (value: unknown) => isRecord(value) && Object.values(value).every((list) => Array.isArray(list) && list.every(isFiniteNumber));

function isValidPersistedWorkspace(value: unknown): value is PersistedWorkspace {
  if (!isRecord(value) || value.version !== STORAGE_VERSION || !isRecord(value.simulation)) return false;
  const simulation = value.simulation;
  const topic = simulation.topic;
  if (!isRecord(topic) || !isString(topic.name) || !isFiniteNumber(topic.nextUnkeyedPartition)) return false;
  if (!isArrayOfRecords(topic.partitions, (partition) => isFiniteNumber(partition.id) && isFiniteNumber(partition.logEndOffset)
    && isArrayOfRecords(partition.messages, (message) => isString(message.id) && (message.producerId === undefined || isString(message.producerId))
      && (message.key === null || isString(message.key)) && isString(message.value) && isFiniteNumber(message.timestamp) && isFiniteNumber(message.partition) && isFiniteNumber(message.offset)))) return false;
  if ((topic.partitions as unknown[]).length < 1 || (topic.partitions as unknown[]).length > 8) return false;
  if (!isArrayOfRecords(simulation.groups, (group) => isString(group.id) && Array.isArray(group.consumers)
    && group.consumers.every((consumer) => isRecord(consumer) && isString(consumer.id) && isString(consumer.groupId)
      && ["running", "paused", "dead"].includes(String(consumer.status)) && Array.isArray(consumer.assignedPartitions)
      && consumer.assignedPartitions.every(isFiniteNumber) && isNumberMap(consumer.currentPositions)
      && isNumberListMap(consumer.pendingOffsets) && isNumberListMap(consumer.processedOffsets) && isFiniteNumber(consumer.lastPollAt))
    && isRecord(group.assignments) && Object.values(group.assignments).every(isString) && isNumberMap(group.committedOffsets)
    && ["stable", "in-progress"].includes(String(group.rebalanceState)))) return false;
  if (!isArrayOfRecords(simulation.events, (event) => isString(event.id) && isFiniteNumber(event.at) && isString(event.type) && isString(event.message) && isString(event.concept))) return false;
  if (!isArrayOfRecords(simulation.metrics, (metric) => isFiniteNumber(metric.at) && isFiniteNumber(metric.produced) && isFiniteNumber(metric.consumed) && isFiniteNumber(metric.committed) && isFiniteNumber(metric.totalLag))) return false;
  if (!isArrayOfRecords(simulation.deliveries, (delivery) => isString(delivery.id) && isString(delivery.groupId) && isString(delivery.consumerId) && isString(delivery.messageId)
    && isFiniteNumber(delivery.partition) && Number.isInteger(delivery.partition) && delivery.partition >= 0
    && isFiniteNumber(delivery.offset) && Number.isInteger(delivery.offset) && delivery.offset >= 0
    && isFiniteNumber(delivery.attempt) && Number.isInteger(delivery.attempt) && delivery.attempt >= 1 && isFiniteNumber(delivery.readAt)
    && ["read", "processed", "committed", "duplicate", "lost", "failed"].includes(String(delivery.status))
    && (delivery.processedAt === undefined || isFiniteNumber(delivery.processedAt))
    && (delivery.committedAt === undefined || isFiniteNumber(delivery.committedAt))
    && (delivery.failureReason === undefined || (isString(delivery.failureReason) && delivery.failureReason.length <= 160))
    && (delivery.applicationStatus === undefined ? delivery.status !== "failed"
      : delivery.applicationStatus === "succeeded" ? isFiniteNumber(delivery.processedAt)
      : delivery.applicationStatus === "pending" ? delivery.processedAt === undefined
      : delivery.applicationStatus === "failed" && delivery.processedAt === undefined && isString(delivery.failureReason) && delivery.failureReason.trim().length > 0))) return false;
  if (!isArrayOfRecords(simulation.producers, (producer) => isString(producer.id) && isFiniteNumber(producer.producedCount))) return false;
  if (!isArrayOfRecords(simulation.lagHistory, (lag) => isFiniteNumber(lag.at) && isString(lag.groupId) && isFiniteNumber(lag.total) && isNumberMap(lag.byPartition))) return false;
  if (!isRecord(simulation.config) || !Object.values(simulation.config).every(isFiniteNumber) || !isRecord(simulation.counters) || !Object.values(simulation.counters).every(isFiniteNumber)) return false;
  if (!isFiniteNumber(simulation.clock) || !isRecord(simulation.advanced) || !isArrayOfRecords(simulation.advanced.transactions, (item) => isString(item.id) && isString(item.producerId) && isString(item.status) && Array.isArray(item.messageIds) && item.messageIds.every(isString)) || !isArrayOfRecords(simulation.advanced.heartbeats, (item) => isString(item.consumerId) && isString(item.groupId))) return false;
  if (!isRecord(simulation.advanced.batchSettings) || !isFiniteNumber(simulation.advanced.batchSettings.batchSize) || !isFiniteNumber(simulation.advanced.batchSettings.lingerMs) || !["none", "gzip", "snappy", "lz4", "zstd"].includes(String(simulation.advanced.batchSettings.compression))) return false;
  const policies = simulation.advanced.policies;
  if (!isRecord(policies) || !["retries", "retentionMs", "sessionTimeoutMs", "maxPollIntervalMs"].every((key) => isFiniteNumber(policies[key])) || !["idempotence", "compaction", "exactlyOnce", "cooperativeRebalance"].every((key) => typeof policies[key] === "boolean") || !isString(policies.deadLetterTopic)) return false;
  if (!isArrayOfRecords(simulation.advanced.schemas, (schema) => isString(schema.subject) && isFiniteNumber(schema.version) && Array.isArray(schema.fields) && schema.fields.every(isString) && isString(schema.compatibility))) return false;
  if (!isArrayOfRecords(simulation.advanced.streamTopologies, (topology) => isString(topology.id) && Array.isArray(topology.sourceTopics) && topology.sourceTopics.every(isString) && isString(topology.sinkTopic) && Array.isArray(topology.operations) && topology.operations.every(isString))) return false;
  if (simulation.cluster !== undefined && (!isRecord(simulation.cluster) || !isArrayOfRecords(simulation.cluster.brokers, (broker) => isString(broker.id) && ["up", "down"].includes(String(broker.status)))
    || !isFiniteNumber(simulation.cluster.replicationFactor) || !isFiniteNumber(simulation.cluster.retryCount) || !["0", "1", "all"].includes(String(simulation.cluster.requiredAcks))
    || (simulation.cluster.minInSyncReplicas !== undefined && (!isFiniteNumber(simulation.cluster.minInSyncReplicas) || !Number.isInteger(simulation.cluster.minInSyncReplicas) || simulation.cluster.minInSyncReplicas < 1))
    || !isArrayOfRecords(simulation.cluster.assignments, (assignment) => isString(assignment.topic) && isFiniteNumber(assignment.partition) && Array.isArray(assignment.replicas) && assignment.replicas.every(isString) && isString(assignment.leader) && Array.isArray(assignment.inSyncReplicas) && assignment.inSyncReplicas.every(isString)))) return false;
  if (typeof value.selectedGroupId !== "string" || !isRecord(value.controls) || !isRecord(value.flowLayout) || !isRecord(value.practiceProgress)) return false;
  if (!isFiniteNumber(value.controls.producerRate) || !isFiniteNumber(value.controls.simulationSpeed) || value.controls.producerRate < 0 || value.controls.producerRate > 100 || value.controls.simulationSpeed < 0.25 || value.controls.simulationSpeed > 4) return false;
  const layout = value.flowLayout;
  if (!isRecord(layout.positions) || (layout.viewport !== null && layout.viewport !== undefined && !isRecord(layout.viewport))) return false;
  if (Object.values(layout.positions).some((position) => !isRecord(position) || typeof position.x !== "number" || !Number.isFinite(position.x) || typeof position.y !== "number" || !Number.isFinite(position.y))) return false;
  if (isRecord(layout.viewport) && (typeof layout.viewport.x !== "number" || !Number.isFinite(layout.viewport.x) || typeof layout.viewport.y !== "number" || !Number.isFinite(layout.viewport.y) || typeof layout.viewport.zoom !== "number" || !Number.isFinite(layout.viewport.zoom) || layout.viewport.zoom <= 0)) return false;
  if (layout.views !== undefined && (!isRecord(layout.views) || Object.values(layout.views).some((view) => !isRecord(view) || !isRecord(view.positions) || Object.values(view.positions).some((position) => !isRecord(position) || !isFiniteNumber(position.x) || !isFiniteNumber(position.y)) || (view.viewport !== null && view.viewport !== undefined && (!isRecord(view.viewport) || !isFiniteNumber(view.viewport.x) || !isFiniteNumber(view.viewport.y) || !isFiniteNumber(view.viewport.zoom) || view.viewport.zoom <= 0))))) return false;
  const practice = value.practiceProgress;
  const categories = ["easy", "middle", "hard", "extreme-hard", "most-common", "trick", "situation"];
  if (!((practice.selectedCategory === null || categories.includes(String(practice.selectedCategory))) && isRecord(practice.currentQuestion) && isRecord(practice.answers))) return false;
  if (Object.entries(practice.currentQuestion).some(([category, index]) => !categories.includes(category) || typeof index !== "number" || !Number.isInteger(index) || index < 0 || index > 49)) return false;
  if (Object.entries(practice.answers).some(([category, answers]) => !categories.includes(category) || !isRecord(answers) || Object.values(answers).some((answer) => typeof answer !== "string"))) return false;
  const entryArray = (entries: unknown, valueType: "number" | "boolean") => Array.isArray(entries) && entries.every((entry) => Array.isArray(entry) && entry.length === 2 && typeof entry[0] === "string" && (valueType === "number" ? typeof entry[1] === "number" && Number.isFinite(entry[1]) : typeof entry[1] === "boolean"));
  return entryArray(value.consumerRates, "number") && entryArray(value.consumerAutoCommit, "boolean") && entryArray(value.consumerRemainders, "number") && typeof value.producerRemainder === "number" && Number.isFinite(value.producerRemainder);
}

function migratePersistedWorkspace(workspace: PersistedWorkspace): void {
  if (workspace.simulation.cluster && workspace.simulation.cluster.minInSyncReplicas === undefined) {
    workspace.simulation.cluster.minInSyncReplicas = 1;
  }
  for (const delivery of workspace.simulation.deliveries) {
    if (delivery.applicationStatus === undefined) {
      // A saved offset (including a commit-current/lost lesson) never proves
      // application success. Legacy timestamps are the evidence for processing.
      delivery.applicationStatus = delivery.processedAt === undefined ? "pending" : "succeeded";
    }
  }
}

function persistentData(state: StoreState): PersistedWorkspace {
  return {
    version: STORAGE_VERSION,
    simulation: state.simulation,
    selectedGroupId: state.selectedGroupId,
    controls: { producerRate: state.snapshot.producerRate, simulationSpeed: state.snapshot.simulationSpeed, activeScenarioId: state.snapshot.activeScenario?.id, inspector: state.snapshot.inspector },
    consumerRates: [...consumerRates.entries()],
    consumerAutoCommit: [...consumerAutoCommit.entries()],
    consumerRemainders: [...consumerRemainders.entries()],
    producerRemainder,
    flowLayout: state.flowLayout,
    practiceProgress: state.practiceProgress,
  };
}

export function persistKafkaWorkspace(): string | undefined {
  try {
    const state = useKafkaStore.getState();
    if (!state.hydrated) return undefined;
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(persistentData(state)));
    if (state.storageNotice) useKafkaStore.setState({ storageNotice: undefined });
    return undefined;
  } catch (error) {
    const reason = error instanceof DOMException && error.name === "QuotaExceededError" ? "Browser storage is full. Your current session still works; reset old records or clear saved data to free space." : "Browser storage is unavailable. Your current session still works, but changes may not survive a reload.";
    if (useKafkaStore.getState().storageNotice !== reason) useKafkaStore.setState({ storageNotice: reason });
    return reason;
  }
}

function eventToLab(event: SimulationEvent) {
  return {
    id: event.id,
    entityId: event.entityId,
    timestamp: event.at,
    type: event.type,
    summary: event.message,
    concept: event.concept,
    metadata: event.details,
  };
}

function toSnapshot(state: KafkaSimulationState, selectedGroupId: string, running: boolean, producerRunning: boolean, producerRate: number, simulationSpeed: number, inspector?: InspectorTarget, activeScenario?: LabScenario, producedPerSecond = 0, consumedPerSecond = 0, resetRevision = 0, flowBaselineEventId?: string): KafkaLabSnapshot {
  const group = findGroup(state, selectedGroupId);
  const lag = group ? getGroupLag(state, group.id) : { total: 0, byPartition: {} };
  const deliveries = new Map<string, DeliveryAttempt>();
  for (const delivery of state.deliveries) {
    if (delivery.groupId === selectedGroupId) deliveries.set(delivery.messageId, delivery);
  }
  const partitions: LabPartition[] = state.topic.partitions.map((partition) => {
    const ownerId = group?.assignments[partition.id];
    const owner = ownerId ? group?.consumers.find((consumer) => consumer.id === ownerId) : undefined;
    const committed = group?.committedOffsets[partition.id] ?? 0;
    const current = owner?.currentPositions[partition.id] ?? committed;
    return {
      id: partition.id,
      messages: partition.messages.map((message): LabMessage => {
        const delivery = deliveries.get(message.id);
        return { ...message, delivery: delivery && delivery.partition === message.partition && delivery.offset === message.offset ? { ...delivery } : undefined };
      }),
      logEndOffset: partition.logEndOffset,
      assignedConsumerId: ownerId,
      assignedConsumerName: ownerId,
      committedOffset: committed,
      currentOffset: current,
      lag: lag.byPartition[partition.id] ?? 0,
    };
  });
  const consumers: LabConsumer[] = (group?.consumers ?? []).map((consumer) => ({
    id: consumer.id,
    name: consumer.id,
    groupId: consumer.groupId,
    status: consumer.status,
    assignedPartitions: [...consumer.assignedPartitions],
    currentOffsets: { ...consumer.currentPositions },
    committedOffsets: { ...group?.committedOffsets },
    rate: consumerRates.get(consumer.id) ?? 6,
    autoCommit: consumerAutoCommit.get(consumer.id) ?? true,
  }));
  const groups: LabConsumerGroup[] = state.groups.map((candidate) => {
    const candidateLag = getGroupLag(state, candidate.id);
    return {
      id: candidate.id,
      name: candidate.id,
      consumers: candidate.consumers.map((consumer) => ({
        id: consumer.id,
        name: consumer.id,
        groupId: consumer.groupId,
        status: consumer.status,
        assignedPartitions: [...consumer.assignedPartitions],
        currentOffsets: { ...consumer.currentPositions },
        committedOffsets: { ...candidate.committedOffsets },
        rate: consumerRates.get(consumer.id) ?? 6,
        autoCommit: consumerAutoCommit.get(consumer.id) ?? true,
      })),
      assignments: { ...candidate.assignments },
      committedOffsets: { ...candidate.committedOffsets },
      lagByPartition: candidateLag.byPartition,
    };
  });
  return {
    resetRevision,
    flowBaselineEventId,
    topicName: state.topic.name,
    producers: state.producers.map((producer) => ({ ...producer })),
    partitions,
    consumers,
    consumerGroups: groups,
    selectedGroupId,
    events: state.events.slice(-200).map(eventToLab),
    metrics: state.metrics.slice(-120).map((metric) => ({ timestamp: metric.at, lag: metric.totalLag, produced: metric.produced, consumed: metric.consumed })),
    running,
    producerRunning,
    producerRate,
    simulationSpeed,
    inspector,
    activeScenario,
    messageCount: state.topic.partitions.reduce((sum, partition) => sum + partition.messages.length, 0),
    producedPerSecond,
    consumedPerSecond,
    totalLag: lag.total,
    cluster: state.cluster,
    advanced: state.advanced,
  };
}

function ensureGroup(state: KafkaSimulationState, groupId: string): KafkaSimulationState {
  if (findGroup(state, groupId)) return state;
  return createGroup(state, groupId).state;
}

function addDefaultConsumer(state: KafkaSimulationState, groupId = DEFAULT_GROUP): KafkaSimulationState {
  const withGroup = ensureGroup(state, groupId);
  if ((findGroup(withGroup, groupId)?.consumers.length ?? MAX_CONSUMERS_PER_GROUP) >= MAX_CONSUMERS_PER_GROUP) return withGroup;
  const result = addConsumer(withGroup, groupId);
  const id = result.value.id;
  consumerRates.set(id, 6);
  consumerAutoCommit.set(id, true);
  return result.state;
}

function produceWithAcks(state: KafkaSimulationState, input: ProducedMessageInput): { state: KafkaSimulationState; produced: boolean } {
  const producerId = input.producerId ?? state.producers[0]?.id ?? "producer-1";
  const selectedPartition = input.partition ?? selectPartition(state.topic, input.key ?? null).partition;
  const acknowledgement = canProduceWithAcks(state, selectedPartition);
  if (!acknowledgement.accepted) {
    const rejected = cloneSimulationState(state);
    addEvent(rejected, "produce-rejected", `Produce rejected for ${rejected.topic.name}[${selectedPartition}]`, "required acknowledgements", `partition-${selectedPartition}`, {
      partition: selectedPartition,
      producerId,
      reason: acknowledgement.reason,
      inSyncReplicas: acknowledgement.inSyncReplicas,
    });
    return { state: rejected, produced: false };
  }
  return { state: produceMessage(state, { ...input, producerId }).state, produced: true };
}

function produceSyntheticWithAcks(
  state: KafkaSimulationState,
  count: number,
  options: Omit<ProducedMessageInput, "value"> & { valuePrefix?: string } = {},
  distributeProducers = false,
): { state: KafkaSimulationState; produced: number } {
  let current = state;
  let produced = 0;
  const producerIds = distributeProducers ? state.producers.map((producer) => producer.id) : [];
  // The configured rate is a total across all producers. Persisted cumulative
  // counts keep the rotation deterministic without another storage field.
  const firstProducer = state.producers.reduce((total, producer) => total + producer.producedCount, 0);
  for (let index = 0; index < Math.max(0, Math.floor(count)); index += 1) {
    const result = produceWithAcks(current, {
      producerId: options.producerId ?? (producerIds.length > 0 ? producerIds[(firstProducer + index) % producerIds.length] : undefined),
      key: options.key,
      partition: options.partition,
      timestamp: options.timestamp,
      value: `${options.valuePrefix ?? "message"}-${index + 1}`,
    });
    current = result.state;
    if (result.produced) produced += 1;
  }
  return { state: current, produced };
}

function snapshotState(state: StoreState): StoreState {
  return { ...state, snapshot: toSnapshot(state.simulation, state.selectedGroupId, state.snapshot.running, state.snapshot.producerRunning, state.snapshot.producerRate, state.snapshot.simulationSpeed, state.snapshot.inspector, state.snapshot.activeScenario, state.snapshot.producedPerSecond, state.snapshot.consumedPerSecond, state.snapshot.resetRevision, state.snapshot.flowBaselineEventId) };
}

function tick(store: StoreState): void {
  if (!store.snapshot.running) return;
  let current = advanceClock(store.simulation, TICK_MS * store.snapshot.simulationSpeed);
  current = checkHeartbeats(current).state;
  let produced = 0;
  let consumed = 0;
  producerRemainder += (store.snapshot.producerRunning ? store.snapshot.producerRate : 0) * (TICK_MS / 1000) * store.snapshot.simulationSpeed;
  const produceCount = Math.floor(producerRemainder + Number.EPSILON * 8);
  producerRemainder = Math.max(0, producerRemainder - produceCount);
  if (produceCount > 0) {
    const result = produceSyntheticWithAcks(current, produceCount, { valuePrefix: "event" }, true);
    current = result.state;
    produced = result.produced;
  }
  for (const group of current.groups) {
    for (const consumer of group.consumers) {
      if (consumer.status !== "running") continue;
      const rate = consumerRates.get(consumer.id) ?? 6;
      const accumulatorId = `${group.id}:${consumer.id}`;
      const accumulated = (consumerRemainders.get(accumulatorId) ?? 0) + rate * (TICK_MS / 1000) * store.snapshot.simulationSpeed;
      // Keep fractional work between ticks so a six-record rate actually reads
      // six per simulated second. Whole unused capacity expires when idle.
      const count = Math.floor(accumulated + Number.EPSILON * 8);
      consumerRemainders.set(accumulatorId, Math.max(0, accumulated - count));
      for (let index = 0; index < count; index += 1) {
        const polled = pollConsumer(current, group.id, consumer.id, { maxRecords: 1 });
        current = polled.state;
        if (polled.value.length === 0) break;
        const processed = processRecords(current, group.id, consumer.id);
        current = processed.state;
        const shouldCommit = consumerAutoCommit.get(consumer.id) ?? true;
        if (shouldCommit) current = commitProcessedOffsets(current, group.id, consumer.id).state;
        consumed += 1;
      }
    }
  }
  const selected = findGroup(current, store.selectedGroupId);
  const lag = selected ? getGroupLag(current, selected.id).total : 0;
  appendMetric(current, { at: current.clock, produced, consumed, committed: 0, totalLag: lag });
  store.simulation = current;
  store.snapshot = toSnapshot(current, store.selectedGroupId, true, store.snapshot.producerRunning, store.snapshot.producerRate, store.snapshot.simulationSpeed, store.snapshot.inspector, store.snapshot.activeScenario, produced / (TICK_MS / 1000), consumed / (TICK_MS / 1000), store.snapshot.resetRevision, store.snapshot.flowBaselineEventId);
}

function applyResult(store: StoreState, next: KafkaSimulationState): StoreState {
  store.simulation = next;
  return snapshotState(store);
}

function applyScenarioState(store: StoreState, scenarioId: string): StoreState {
  const scenario = getScenario(scenarioId);
  if (!scenario) return store;
  resetRuntimeState();
  let next = scenario.setup();
  if (next.groups.length === 0) next = addDefaultConsumer(next, DEFAULT_GROUP);
  const selectedGroupId = next.groups[0]?.id ?? DEFAULT_GROUP;
  const metadata = scenarioCatalog.find((candidate) => candidate.id === scenario.id) ?? scenarioCatalog[0];
  return {
    ...store,
    simulation: next,
    selectedGroupId,
    flowLayout: emptyFlowLayout(),
    snapshot: toSnapshot(next, selectedGroupId, false, false, store.snapshot.producerRate, store.snapshot.simulationSpeed, undefined, metadata, 0, 0, (store.snapshot.resetRevision ?? 0) + 1, next.events.at(-1)?.id),
  };
}

export const useKafkaStore = create<StoreState>((set, get) => {
  const seeded = createReferenceFlow();
  for (const consumer of seeded.groups[0].consumers) {
    consumerRates.set(consumer.id, 6);
    consumerAutoCommit.set(consumer.id, false);
  }
  const initialScenario = scenarioCatalog[0];
  const startTimer = () => {
    if (timer === undefined) timer = setInterval(() => set((store) => { tick(store); return { ...store }; }), TICK_MS);
  };
  const actions: KafkaLabActions = {
    hydrate: () => {
      stopTimer();
      let raw: string | null = null;
      try {
        raw = window.localStorage.getItem(STORAGE_KEY);
      } catch {
        set({ hydrated: true, storageNotice: "Browser storage is unavailable. Your current session still works, but changes may not survive a reload." });
        return;
      }
      if (!raw) { set({ hydrated: true }); return; }
      let value: unknown;
      try { value = JSON.parse(raw); } catch { set({ hydrated: true, storageNotice: "Saved data could not be read. The app started with a fresh workspace; your practice progress may need to be reset." }); return; }
      if (!isValidPersistedWorkspace(value)) { set({ hydrated: true, storageNotice: "Saved data is invalid or from an unsupported version. The app started with a fresh workspace." }); return; }
      try {
        migratePersistedWorkspace(value);
        resetRuntimeState();
        value.consumerRates.forEach(([id, rate]) => { if (typeof id === "string" && typeof rate === "number" && Number.isFinite(rate)) consumerRates.set(id, rate); });
        value.consumerAutoCommit.forEach(([id, enabled]) => { if (typeof id === "string" && typeof enabled === "boolean") consumerAutoCommit.set(id, enabled); });
        value.consumerRemainders.forEach(([id, remainder]) => { if (typeof id === "string" && typeof remainder === "number" && Number.isFinite(remainder)) consumerRemainders.set(id, remainder); });
        producerRemainder = Number.isFinite(value.producerRemainder) ? value.producerRemainder : 0;
        const selectedGroupId = findGroup(value.simulation, value.selectedGroupId) ? value.selectedGroupId : value.simulation.groups[0]?.id ?? DEFAULT_GROUP;
        const scenario = scenarioCatalog.find((candidate) => candidate.id === value.controls.activeScenarioId);
        const current = get();
        const restoredSnapshot = toSnapshot(value.simulation, selectedGroupId, false, false, value.controls.producerRate, value.controls.simulationSpeed, value.controls.inspector, scenario, 0, 0, current.snapshot.resetRevision, value.simulation.events.at(-1)?.id);
        const savedLayout = value.flowLayout as FlowLayout;
        const flowView = savedLayout.views?.flow ?? { positions: savedLayout.positions, viewport: savedLayout.viewport };
        set({ ...current, simulation: value.simulation, selectedGroupId, snapshot: restoredSnapshot, flowLayout: { ...savedLayout, views: { ...savedLayout.views, flow: flowView } }, practiceProgress: value.practiceProgress, hydrated: true, storageNotice: undefined });
      } catch {
        resetRuntimeState();
        const fresh = addDefaultConsumer(createSimulationState({ topicName: "orders", partitionCount: 3 }), DEFAULT_GROUP);
        const current = get();
        set({ ...current, simulation: fresh, selectedGroupId: DEFAULT_GROUP, snapshot: toSnapshot(fresh, DEFAULT_GROUP, false, false, 12, 1, undefined, initialScenario, 0, 0, 0, fresh.events.at(-1)?.id), flowLayout: emptyFlowLayout(), practiceProgress: emptyPracticeProgress(), hydrated: true, storageNotice: "Saved data could not be restored. A fresh workspace is ready; try the app again or clear its saved data." });
      }
    },
    setFlowPositions: (positions, viewId = "flow") => set((store) => {
      const previous = store.flowLayout.views?.[viewId] ?? { positions: {}, viewport: null };
      const nextView = { ...previous, positions: { ...positions } };
      return { ...store, flowLayout: { ...store.flowLayout, ...(viewId === "flow" ? { positions: { ...positions } } : {}), views: { ...store.flowLayout.views, [viewId]: nextView } } };
    }),
    setFlowViewport: (viewport, viewId = "flow") => set((store) => {
      const previous = store.flowLayout.views?.[viewId] ?? { positions: {}, viewport: null };
      const nextView = { ...previous, viewport };
      return { ...store, flowLayout: { ...store.flowLayout, ...(viewId === "flow" ? { viewport } : {}), views: { ...store.flowLayout.views, [viewId]: nextView } } };
    }),
    selectPracticeCategory: (category: PracticeCategory) => set((store) => ({ ...store, practiceProgress: { ...store.practiceProgress, selectedCategory: category } })),
    selectPracticeQuestion: (category: PracticeCategory, questionIndex: number) => set((store) => ({
      ...store,
      practiceProgress: {
        ...store.practiceProgress,
        selectedCategory: category,
        currentQuestion: { ...store.practiceProgress.currentQuestion, [category]: Math.max(0, Math.min(49, Math.floor(questionIndex))) },
      },
    })),
    answerPracticeQuestion: (category: PracticeCategory, questionId: string, optionId: string) => set((store) => ({ ...store, practiceProgress: { ...store.practiceProgress, selectedCategory: category, answers: { ...store.practiceProgress.answers, [category]: { ...store.practiceProgress.answers[category], [questionId]: optionId } } } })),
    resetPracticeCategory: (category: PracticeCategory) => set((store) => {
      const answers = { ...store.practiceProgress.answers };
      const currentQuestion = { ...store.practiceProgress.currentQuestion };
      delete answers[category];
      delete currentQuestion[category];
      return { ...store, practiceProgress: { ...store.practiceProgress, answers, currentQuestion } };
    }),
    produce: (input: ProduceInput) => set((store) => {
      const produced = produceWithAcks(store.simulation, { key: input.key, value: input.value, partition: input.partition, producerId: input.producerId });
      const next = produced.state;
      const selected = findGroup(next, store.selectedGroupId);
      appendMetric(next, { at: next.clock, produced: produced.produced ? 1 : 0, consumed: 0, committed: 0, totalLag: selected ? getGroupLag(next, selected.id).total : 0 });
      return applyResult(store, next);
    }),
    produceBatch: ({ keyPrefix, valuePrefix, count, producerId }) => set((store) => {
      const produced = produceSyntheticWithAcks(store.simulation, count, { key: keyPrefix ?? null, valuePrefix: valuePrefix ?? "event", producerId });
      const selected = findGroup(produced.state, store.selectedGroupId);
      appendMetric(produced.state, { at: produced.state.clock, produced: produced.produced, consumed: 0, committed: 0, totalLag: selected ? getGroupLag(produced.state, selected.id).total : 0 });
      return applyResult(store, produced.state);
    }),
    addProducer: (producerId) => set((store) => {
      if (store.simulation.producers.length >= MAX_PRODUCERS) return store;
      const requestedId = producerId?.replace(/[\u0000-\u001f\u007f]/g, " ").trim().slice(0, 80);
      if (requestedId) {
        if (store.simulation.producers.some((producer) => producer.id === requestedId)) return store;
        return applyResult(store, addProducerInSimulation(store.simulation, requestedId).state);
      }
      const next = cloneSimulationState(store.simulation);
      let generatedId: string;
      do {
        generatedId = `producer-${next.counters.producer++}`;
      } while (next.producers.some((producer) => producer.id === generatedId));
      return applyResult(store, addProducerInSimulation(next, generatedId).state);
    }),
    start: () => {
      startTimer();
      set((store) => ({ ...store, snapshot: { ...store.snapshot, running: true, producerRunning: true } }));
    },
    startReading: () => {
      startTimer();
      set((store) => ({ ...store, snapshot: { ...store.snapshot, running: true, producerRunning: false } }));
    },
    pause: () => {
      stopTimer();
      set((store) => ({ ...store, snapshot: { ...store.snapshot, running: false, producerRunning: false } }));
    },
    startProducer: () => {
      startTimer();
      set((store) => ({ ...store, snapshot: { ...store.snapshot, running: true, producerRunning: true } }));
    },
    pauseProducer: () => set((store) => ({ ...store, snapshot: { ...store.snapshot, producerRunning: false } })),
    reset: () => set((store) => {
      resetRuntimeState();
      const reset = addDefaultConsumer(createSimulationState({ topicName: "orders", partitionCount: 3 }), DEFAULT_GROUP);
      return { ...store, simulation: reset, selectedGroupId: DEFAULT_GROUP, flowLayout: emptyFlowLayout(), snapshot: toSnapshot(reset, DEFAULT_GROUP, false, false, 12, 1, undefined, initialScenario, 0, 0, (store.snapshot.resetRevision ?? 0) + 1, reset.events.at(-1)?.id) };
    }),
    loadReferenceFlow: () => set((store) => {
      resetRuntimeState();
      const next = createReferenceFlow();
      for (const consumer of next.groups[0].consumers) {
        consumerRates.set(consumer.id, 6);
        consumerAutoCommit.set(consumer.id, false);
      }
      return { ...store, simulation: next, selectedGroupId: DEFAULT_GROUP, flowLayout: emptyFlowLayout(),
        snapshot: toSnapshot(next, DEFAULT_GROUP, false, false, 12, 1, undefined, undefined, 0, 0, (store.snapshot.resetRevision ?? 0) + 1, next.events.at(-1)?.id) };
    }),
    retryRecordProcessing: ({ partition, offset }) => set((store) => {
      const group = findGroup(store.simulation, store.selectedGroupId);
      const owner = group?.assignments[partition];
      if (!group || !owner) return store;
      const retried = retryProcessing(store.simulation, group.id, owner, { partition, offset });
      if (!retried.value) return store;
      const processed = processRecords(retried.state, group.id, owner, { partition, offset });
      const next = (consumerAutoCommit.get(owner) ?? true) ? commitProcessedOffsets(processed.state, group.id, owner, [partition]).state : processed.state;
      appendMetric(next, { at: next.clock, produced: 0, consumed: processed.value.length, committed: 0, totalLag: getGroupLag(next, group.id).total });
      return applyResult(store, next);
    }),
    configureTopology: ({ topicName, partitionCount }) => set((store) => {
      resetRuntimeState();
      const safeTopicName = topicName.trim().slice(0, 80) || "orders";
      const safePartitionCount = Number.isFinite(partitionCount) ? Math.max(1, Math.min(8, Math.floor(partitionCount))) : 3;
      const next = addDefaultConsumer(createSimulationState({ topicName: safeTopicName, partitionCount: safePartitionCount }), DEFAULT_GROUP);
      return { ...store, simulation: next, selectedGroupId: DEFAULT_GROUP, flowLayout: emptyFlowLayout(), snapshot: toSnapshot(next, DEFAULT_GROUP, false, false, 12, 1, undefined, undefined, 0, 0, (store.snapshot.resetRevision ?? 0) + 1, next.events.at(-1)?.id) };
    }),
    setProducerRate: (rate) => set((store) => ({ ...store, snapshot: { ...store.snapshot, producerRate: Math.max(0, Math.min(100, rate)) } })),
    setSimulationSpeed: (speed) => set((store) => ({ ...store, snapshot: { ...store.snapshot, simulationSpeed: Math.max(0.25, Math.min(4, speed)) } })),
    addConsumer: (groupId = DEFAULT_GROUP) => set((store) => applyResult(store, addDefaultConsumer(store.simulation, groupId))),
    removeConsumer: (consumerId) => set((store) => {
      const consumer = store.simulation.groups.flatMap((group) => group.consumers.map((candidate) => ({ candidate, groupId: group.id }))).find(({ candidate }) => candidate.id === consumerId);
      if (consumer) consumerRemainders.delete(`${consumer.groupId}:${consumerId}`);
      return consumer ? applyResult(store, removeConsumer(store.simulation, consumer.groupId, consumerId).state) : store;
    }),
    pauseConsumer: (consumerId) => set((store) => {
      const found = store.simulation.groups.flatMap((group) => group.consumers.map((candidate) => ({ candidate, groupId: group.id }))).find(({ candidate }) => candidate.id === consumerId);
      return found ? applyResult(store, pauseConsumer(store.simulation, found.groupId, consumerId).state) : store;
    }),
    resumeConsumer: (consumerId) => set((store) => {
      const found = store.simulation.groups.flatMap((group) => group.consumers.map((candidate) => ({ candidate, groupId: group.id }))).find(({ candidate }) => candidate.id === consumerId);
      return found ? applyResult(store, resumeConsumer(store.simulation, found.groupId, consumerId).state) : store;
    }),
    crashConsumer: (consumerId) => set((store) => {
      const found = store.simulation.groups.flatMap((group) => group.consumers.map((candidate) => ({ candidate, groupId: group.id }))).find(({ candidate }) => candidate.id === consumerId);
      if (!found) return store;
      const crashed = crashConsumer(store.simulation, found.groupId, consumerId);
      return applyResult(store, crashed.state);
    }),
    restartConsumer: (consumerId) => set((store) => {
      const found = store.simulation.groups.flatMap((group) => group.consumers.map((candidate) => ({ candidate, groupId: group.id }))).find(({ candidate }) => candidate.id === consumerId);
      if (!found) return store;
      return applyResult(store, restartConsumer(store.simulation, found.groupId, consumerId).state);
    }),
    commitConsumer: (consumerId) => set((store) => {
      const found = store.simulation.groups.flatMap((group) => group.consumers.map((candidate) => ({ candidate, groupId: group.id }))).find(({ candidate }) => candidate.id === consumerId);
      return found ? applyResult(store, commitProcessedOffsets(store.simulation, found.groupId, consumerId).state) : store;
    }),
    commitCurrentConsumer: (consumerId) => set((store) => {
      const found = store.simulation.groups.flatMap((group) => group.consumers.map((candidate) => ({ candidate, groupId: group.id }))).find(({ candidate }) => candidate.id === consumerId);
      return found ? applyResult(store, commitCurrentOffsets(store.simulation, found.groupId, consumerId).state) : store;
    }),
    setConsumerRate: (consumerId, rate) => set((store) => { consumerRates.set(consumerId, Math.max(0, Math.min(100, Number.isFinite(rate) ? rate : 6))); return snapshotState(store); }),
    setConsumerAutoCommit: (consumerId, enabled) => set((store) => { consumerAutoCommit.set(consumerId, enabled); return snapshotState(store); }),
    selectGroup: (groupId) => set((store) => ({ ...store, selectedGroupId: groupId, snapshot: toSnapshot(store.simulation, groupId, store.snapshot.running, store.snapshot.producerRunning, store.snapshot.producerRate, store.snapshot.simulationSpeed, store.snapshot.inspector, store.snapshot.activeScenario, store.snapshot.producedPerSecond, store.snapshot.consumedPerSecond, store.snapshot.resetRevision, store.snapshot.flowBaselineEventId) })),
    inspect: (target) => set((store) => ({ ...store, snapshot: { ...store.snapshot, inspector: target } })),
    closeInspector: () => set((store) => ({ ...store, snapshot: { ...store.snapshot, inspector: undefined } })),
    loadScenario: (scenarioId) => set((store) => applyScenarioState(store, scenarioId)),
    runScenario: () => set((store) => {
      const scenarioId = store.snapshot.activeScenario?.id ?? initialScenario.id;
      let next = store.simulation;
      let scenarioInspector: InspectorTarget | undefined;
      if (scenarioId === "order-lifecycle") {
        for (const value of ["order-created", "payment-authorized", "inventory-reserved", "order-shipped"]) {
          next = produceWithAcks(next, { key: "order-7F3K9", value }).state;
        }
      } else if (scenarioId === "keyed-partitioning") next = produceSyntheticWithAcks(next, 3, { key: "user-42", valuePrefix: "keyed" }).state;
      else if (scenarioId === "slow-consumer") next = produceSyntheticWithAcks(next, 20, { valuePrefix: "slow-consumer" }).state;
      else if (scenarioId === "consumer-scaling") next = addDefaultConsumer(next, next.groups[0]?.id ?? DEFAULT_GROUP);
      else if (scenarioId === "rebalance") next = addDefaultConsumer(next, next.groups[0]?.id ?? DEFAULT_GROUP);
      else if (scenarioId === "duplicate-processing") {
        const group = next.groups[0];
        const consumer = group?.consumers[0];
        if (group && consumer) {
          next = crashConsumer(next, group.id, consumer.id).state;
          next = restartConsumer(next, group.id, consumer.id).state;
          next = pollConsumer(next, group.id, consumer.id, { maxRecords: 1 }).state;
          next = processRecords(next, group.id, consumer.id).state;
        }
      } else if (scenarioId === "loss-before-processing") {
        const group = next.groups[0];
        const consumer = group?.consumers[0];
        if (group && consumer) {
          next = commitCurrentOffsets(next, group.id, consumer.id).state;
          next = crashConsumer(next, group.id, consumer.id).state;
          next = restartConsumer(next, group.id, consumer.id).state;
          const restartedPoll = pollConsumer(next, group.id, consumer.id, { maxRecords: 1 });
          next = restartedPoll.state;
          const restartedGroup = findGroup(next, group.id);
          const savedOffset = restartedGroup?.committedOffsets[0] ?? 0;
          const processedCount = next.deliveries.filter((delivery) => delivery.groupId === group.id && delivery.processedAt !== undefined).length;
          const lostCount = next.deliveries.filter((delivery) => delivery.groupId === group.id && delivery.status === "lost").length;
          scenarioInspector = {
            kind: "offset",
            id: "loss-before-processing",
            title: lostCount > 0 && restartedPoll.value.length === 0 ? "Record skipped after restart" : "Commit-before-processing result",
            observations: { recordsReadAfterRestart: restartedPoll.value.length },
            state: `Stored records: ${next.topic.partitions[0]?.messages.length ?? 0} · saved next offset: ${savedOffset} · processed deliveries: ${processedCount} · records read after restart: ${restartedPoll.value.length}`,
            whyChanged: "The group saved its current read position before processing. Restart resumes at that saved next offset, so the unfinished record is skipped.",
            concept: "A committed offset saves where to read next; it is not proof of completed processing.",
            relatedCode: "src/core/kafka/offsets.ts",
            simplification: "The record remains in the topic log. This lesson models work lost by one consumer group, not removal from Kafka. Crash and restart run synchronously in the simulator.",
          };
        }
      } else if (scenarioId === "multiple-groups") {
        const group = next.groups[0];
        const consumer = group?.consumers[0];
        if (group && consumer) next = consumeOne(next, group.id, consumer.id, true).state;
      } else next = produceSyntheticWithAcks(next, 3, { valuePrefix: "scenario" }).state;
      const selected = findGroup(next, store.selectedGroupId);
      appendMetric(next, { at: next.clock, produced: 0, consumed: 0, committed: 0, totalLag: selected ? getGroupLag(next, selected.id).total : 0 });
      const updated = applyResult(store, next);
      return scenarioInspector ? { ...updated, snapshot: { ...updated.snapshot, inspector: scenarioInspector } } : updated;
    }),
    createCluster: () => set((store) => applyResult(store, createBrokerCluster(store.simulation, 3, 2, 2).state)),
    setAcks: (acks) => set((store) => applyResult(store, setRequiredAcks(store.simulation, acks).state)),
    failBroker: (brokerId) => set((store) => applyResult(store, failBrokerInCluster(store.simulation, brokerId).state)),
    recoverBroker: (brokerId) => set((store) => applyResult(store, recoverBroker(store.simulation, brokerId).state)),
    beginTransaction: () => set((store) => applyResult(store, beginTransaction(store.simulation, store.simulation.producers[0]?.id ?? "producer-1").state)),
    appendTransactionMessage: (transactionId) => set((store) => {
      const input = { value: `transaction-event-${store.simulation.clock}` };
      const partition = selectPartition(store.simulation.topic, null).partition;
      if (!canProduceWithAcks(store.simulation, partition).accepted) return applyResult(store, produceWithAcks(store.simulation, input).state);
      return applyResult(store, appendTransactionalMessage(store.simulation, transactionId, input).state);
    }),
    commitTransaction: (transactionId) => set((store) => applyResult(store, commitTransaction(store.simulation, transactionId).state)),
    abortTransaction: (transactionId) => set((store) => applyResult(store, abortTransaction(store.simulation, transactionId).state)),
    recordHeartbeat: (consumerId) => set((store) => {
      const found = store.simulation.groups.flatMap((group) => group.consumers.map((candidate) => ({ candidate, groupId: group.id }))).find(({ candidate }) => candidate.id === consumerId);
      return found ? applyResult(store, recordHeartbeat(store.simulation, found.groupId, consumerId).state) : store;
    }),
    configureBatching: (settings) => set((store) => applyResult(store, configureBatching(store.simulation, settings).state)),
    configurePolicies: (settings) => set((store) => applyResult(store, configureAdvancedPolicies(store.simulation, settings).state)),
    registerSchema: () => set((store) => applyResult(store, registerSchema(store.simulation, { subject: "orders-value", fields: ["id", "value"], compatibility: "backward" }).state)),
    createStreamTopology: () => set((store) => applyResult(store, createStreamTopology(store.simulation, { sourceTopics: ["orders"], sinkTopic: "orders-enriched", operations: ["filter", "map"] }).state)),
  };
  const initialSnapshot = toSnapshot(seeded, DEFAULT_GROUP, false, false, 12, 1, undefined, undefined, 0, 0, 0, seeded.events.at(-1)?.id);
  return { simulation: seeded, selectedGroupId: DEFAULT_GROUP, snapshot: initialSnapshot, actions, flowLayout: emptyFlowLayout(), practiceProgress: emptyPracticeProgress(), hydrated: false, storageNotice: undefined };
});

export function useKafkaLabStoreAdapter(): KafkaLabStore {
  const snapshot = useKafkaStore((store) => store.snapshot);
  const actions = useKafkaStore((store) => store.actions);
  const flowLayout = useKafkaStore((store) => store.flowLayout);
  const practiceProgress = useKafkaStore((store) => store.practiceProgress);
  const hydrated = useKafkaStore((store) => store.hydrated);
  const storageNotice = useKafkaStore((store) => store.storageNotice);
  return { snapshot, actions, flowLayout, practiceProgress, hydrated, storageNotice };
}
