import type {
  AdvancedState,
  BrokerCluster,
  Consumer,
  ConsumerGroup,
  Counters,
  DeliveryAttempt,
  KafkaSimulationState,
  Partition,
  Producer,
  SimulationEvent,
  SimulationOptions,
  SimulationResult,
  Topic,
} from "./types";

export const DEFAULT_SIMULATION_CONFIG = {
  maxEvents: 200,
  maxMetrics: 120,
  maxDeliveries: 200,
} as const;

export function createPartition(id: number): Partition {
  return { id, messages: [], logEndOffset: 0 };
}

export function createTopic(name = "orders", partitionCount = 3): Topic {
  const safeCount = Math.max(1, Math.floor(partitionCount));
  return {
    name,
    partitions: Array.from({ length: safeCount }, (_, id) => createPartition(id)),
    nextUnkeyedPartition: 0,
  };
}

export function createProducer(id: string): Producer {
  return { id, producedCount: 0 };
}

export function createConsumer(id: string, groupId: string, at = 0): Consumer {
  return {
    id,
    groupId,
    assignedPartitions: [],
    status: "running",
    currentPositions: {},
    pendingOffsets: {},
    processedOffsets: {},
    lastPollAt: at,
  };
}

export function createConsumerGroup(id: string): ConsumerGroup {
  return {
    id,
    consumers: [],
    assignments: {},
    committedOffsets: {},
    rebalanceState: "stable",
  };
}

function createAdvancedState(): AdvancedState {
  return {
    transactions: [],
    heartbeats: [],
    batchSettings: { batchSize: 1, lingerMs: 0, compression: "none" },
    schemas: [],
    streamTopologies: [],
    policies: {
      retries: 3,
      idempotence: false,
      retentionMs: 86400000,
      compaction: false,
      deadLetterTopic: "orders.DLT",
      exactlyOnce: false,
      sessionTimeoutMs: 10000,
      maxPollIntervalMs: 300000,
      cooperativeRebalance: false,
    },
  };
}

function createCounters(): Counters {
  return {
    event: 0,
    message: 0,
    consumer: 0,
    producer: 0,
    delivery: 0,
    transaction: 0,
    topology: 0,
  };
}

export function createSimulationState(options: SimulationOptions = {}): KafkaSimulationState {
  const producers = (options.producerIds ?? ["producer-1"]).map(createProducer);
  const state: KafkaSimulationState = {
    clock: options.startTime ?? 0,
    topic: createTopic(options.topicName ?? "orders", options.partitionCount ?? 3),
    producers,
    groups: [],
    events: [],
    deliveries: [],
    lagHistory: [],
    metrics: [],
    config: { ...DEFAULT_SIMULATION_CONFIG, ...options.config },
    counters: createCounters(),
    advanced: createAdvancedState(),
  };
  addEvent(state, "simulation-reset", "Simulation reset", "deterministic simulation state");
  return state;
}

function cloneConsumer(consumer: Consumer): Consumer {
  return {
    ...consumer,
    assignedPartitions: [...consumer.assignedPartitions],
    currentPositions: { ...consumer.currentPositions },
    pendingOffsets: cloneOffsetMap(consumer.pendingOffsets),
    processedOffsets: cloneOffsetMap(consumer.processedOffsets),
  };
}

function cloneOffsetMap(offsets: Record<number, number[]>): Record<number, number[]> {
  return Object.fromEntries(Object.entries(offsets).map(([partition, values]) => [partition, [...values]]));
}

export function cloneSimulationState(state: KafkaSimulationState): KafkaSimulationState {
  const cluster: BrokerCluster | undefined = state.cluster
    ? {
        ...state.cluster,
        brokers: state.cluster.brokers.map((broker) => ({ ...broker })),
        assignments: state.cluster.assignments.map((assignment) => ({
          ...assignment,
          replicas: [...assignment.replicas],
          inSyncReplicas: [...assignment.inSyncReplicas],
        })),
      }
    : undefined;
  return {
    ...state,
    topic: {
      ...state.topic,
      partitions: state.topic.partitions.map((partition) => ({
        ...partition,
        messages: partition.messages.map((message) => ({ ...message })),
      })),
    },
    producers: state.producers.map((producer) => ({ ...producer })),
    groups: state.groups.map((group) => ({
      ...group,
      consumers: group.consumers.map(cloneConsumer),
      assignments: { ...group.assignments },
      committedOffsets: { ...group.committedOffsets },
    })),
    events: state.events.map((event) => ({ ...event, details: event.details ? { ...event.details } : undefined })),
    deliveries: state.deliveries.map((delivery) => ({ ...delivery })),
    lagHistory: state.lagHistory.map((snapshot) => ({ ...snapshot, byPartition: { ...snapshot.byPartition } })),
    metrics: state.metrics.map((metric) => ({ ...metric })),
    config: { ...state.config },
    counters: { ...state.counters },
    cluster,
    advanced: {
      ...state.advanced,
      transactions: state.advanced.transactions.map((transaction) => ({ ...transaction, messageIds: [...transaction.messageIds] })),
      heartbeats: state.advanced.heartbeats.map((heartbeat) => ({ ...heartbeat })),
      batchSettings: { ...state.advanced.batchSettings },
      schemas: state.advanced.schemas.map((schema) => ({ ...schema, fields: [...schema.fields] })),
      streamTopologies: state.advanced.streamTopologies.map((topology) => ({ ...topology, sourceTopics: [...topology.sourceTopics], operations: [...topology.operations] })),
      policies: { ...state.advanced.policies },
    },
  };
}

export function addEvent(
  state: KafkaSimulationState,
  type: SimulationEvent["type"],
  message: string,
  concept: string,
  entityId?: string,
  details?: SimulationEvent["details"],
): SimulationEvent {
  const event: SimulationEvent = {
    id: `event-${state.counters.event++}`,
    at: state.clock,
    type,
    message,
    concept,
    ...(entityId ? { entityId } : {}),
    ...(details ? { details } : {}),
  };
  state.events.push(event);
  if (state.events.length > state.config.maxEvents) {
    state.events.splice(0, state.events.length - state.config.maxEvents);
  }
  return event;
}

export function addDelivery(state: KafkaSimulationState, delivery: DeliveryAttempt): void {
  state.deliveries.push(delivery);
  if (state.deliveries.length > state.config.maxDeliveries) {
    // Latest per-record/group outcomes are required application state: losing
    // one would turn a known failure into a queued record or erase a retry's
    // attempt number. The history limit applies to superseded attempts only;
    // latest outcomes, like the stored log itself, can grow in long sessions.
    const latest = new Map<string, string>();
    for (const attempt of state.deliveries) latest.set(JSON.stringify([attempt.groupId, attempt.messageId]), attempt.id);
    let historicalBudget = Math.max(0, state.config.maxDeliveries - latest.size);
    const retained: DeliveryAttempt[] = [];
    for (let index = state.deliveries.length - 1; index >= 0; index -= 1) {
      const attempt = state.deliveries[index];
      if (latest.get(JSON.stringify([attempt.groupId, attempt.messageId])) === attempt.id) retained.push(attempt);
      else if (historicalBudget > 0) { retained.push(attempt); historicalBudget -= 1; }
    }
    state.deliveries = retained.reverse();
  }
}

export function getGroup(state: KafkaSimulationState, groupId: string): ConsumerGroup | undefined {
  return state.groups.find((group) => group.id === groupId);
}

export function getConsumer(state: KafkaSimulationState, groupId: string, consumerId: string): Consumer | undefined {
  return getGroup(state, groupId)?.consumers.find((consumer) => consumer.id === consumerId);
}

export function getPartition(state: KafkaSimulationState, partitionId: number): Partition | undefined {
  return state.topic.partitions.find((partition) => partition.id === partitionId);
}

export function getMessage(state: KafkaSimulationState, partitionId: number, offset: number) {
  return getPartition(state, partitionId)?.messages.find((message) => message.offset === offset);
}

export function advanceClock(state: KafkaSimulationState, elapsedMs: number): KafkaSimulationState {
  const next = cloneSimulationState(state);
  next.clock += Math.max(0, Math.floor(elapsedMs));
  return next;
}

export function result<T>(state: KafkaSimulationState, value: T, events: SimulationEvent[] = []): SimulationResult<T> {
  return { state, value, events };
}

export function nextId(state: KafkaSimulationState, kind: keyof Counters, prefix: string): string {
  return `${prefix}-${state.counters[kind]++}`;
}

export function appendMetric(state: KafkaSimulationState, point: KafkaSimulationState["metrics"][number]): void {
  state.metrics.push(point);
  if (state.metrics.length > state.config.maxMetrics) {
    state.metrics.splice(0, state.metrics.length - state.config.maxMetrics);
  }
}

export function appendLagSnapshot(state: KafkaSimulationState, snapshot: KafkaSimulationState["lagHistory"][number]): void {
  state.lagHistory.push(snapshot);
  if (state.lagHistory.length > state.config.maxMetrics) {
    state.lagHistory.splice(0, state.lagHistory.length - state.config.maxMetrics);
  }
}
