/**
 * The domain types intentionally model the learner-facing Kafka concepts rather
 * than the Kafka wire protocol.  The engine is pure TypeScript and has no UI or
 * browser dependencies.
 */

export type ConsumerStatus = "running" | "paused" | "dead";

export type EventType =
  | "simulation-reset"
  | "message-produced"
  | "produce-rejected"
  | "acks-updated"
  | "consumer-joined"
  | "consumer-left"
  | "consumer-paused"
  | "consumer-resumed"
  | "consumer-polled"
  | "record-processed"
  | "record-processing-failed"
  | "record-processing-retried"
  | "offset-committed"
  | "offset-commit-blocked"
  | "consumer-crashed"
  | "consumer-restarted"
  | "consumer-timeout"
  | "rebalance-started"
  | "assignments-revoked"
  | "assignment-created"
  | "rebalance-completed"
  | "duplicate-delivery"
  | "lost-delivery"
  | "broker-failed"
  | "broker-recovered"
  | "leader-elected"
  | "transaction-started"
  | "transaction-committed"
  | "transaction-aborted"
  | "heartbeat-recorded"
  | "schema-registered"
  | "stream-topology-created"
  | "batch-settings-updated"
  | "advanced-policy-updated";

export type Scalar = string | number | boolean | null;
export type EventDetails = Record<string, Scalar | string[]>;

// @KafkaConcept
// A record's offset is local to its partition. There is deliberately no
// topic-wide offset in this model.
export type KafkaMessage = {
  id: string;
  /** Local teaching provenance; Kafka records do not intrinsically name an application service. */
  producerId?: string;
  key: string | null;
  value: string;
  timestamp: number;
  partition: number;
  offset: number;
};

// @Partition
// logEndOffset is the offset where the next record will be appended.
export type Partition = {
  id: number;
  messages: KafkaMessage[];
  logEndOffset: number;
};

export type Topic = {
  name: string;
  partitions: Partition[];
  /** Next partition used by the deterministic no-key educational strategy. */
  nextUnkeyedPartition: number;
};

export type Producer = {
  id: string;
  producedCount: number;
};

export type DeliveryStatus = "read" | "processed" | "committed" | "duplicate" | "lost" | "failed";

export type DeliveryAttempt = {
  id: string;
  groupId: string;
  consumerId: string;
  messageId: string;
  partition: number;
  offset: number;
  attempt: number;
  status: DeliveryStatus;
  /** Application work is separate from reading and group offset commits. */
  applicationStatus: "pending" | "succeeded" | "failed";
  failureReason?: string;
  readAt: number;
  processedAt?: number;
  committedAt?: number;
};

export type Consumer = {
  id: string;
  groupId: string;
  assignedPartitions: number[];
  status: ConsumerStatus;
  /** Current processing position, represented as the next offset to read. */
  currentPositions: Record<number, number>;
  /** Records read but not yet processed. */
  pendingOffsets: Record<number, number[]>;
  /** Records processed but whose group offset has not necessarily been committed. */
  processedOffsets: Record<number, number[]>;
  lastPollAt: number;
};

// @ConsumerGroup
// A committed offset is the NEXT offset this group should read.
export type ConsumerGroup = {
  id: string;
  consumers: Consumer[];
  assignments: Record<number, string>;
  committedOffsets: Record<number, number>;
  rebalanceState: "stable" | "in-progress";
};

export type SimulationEvent = {
  id: string;
  at: number;
  type: EventType;
  message: string;
  concept: string;
  entityId?: string;
  details?: EventDetails;
};

export type LagSnapshot = {
  at: number;
  groupId: string;
  total: number;
  byPartition: Record<number, number>;
};

export type MetricsPoint = {
  at: number;
  produced: number;
  consumed: number;
  committed: number;
  totalLag: number;
};

export type SimulationConfig = {
  maxEvents: number;
  maxMetrics: number;
  maxDeliveries: number;
};

export type BrokerStatus = "up" | "down";

export type Broker = {
  id: string;
  status: BrokerStatus;
  rack?: string;
};

export type ReplicaAssignment = {
  topic: string;
  partition: number;
  replicas: string[];
  leader: string;
  inSyncReplicas: string[];
};

export type BrokerCluster = {
  brokers: Broker[];
  replicationFactor: number;
  minInSyncReplicas: number;
  assignments: ReplicaAssignment[];
  requiredAcks: "0" | "1" | "all";
  retryCount: number;
};

export type TransactionStatus = "idle" | "open" | "committed" | "aborted";

export type TransactionState = {
  id: string;
  producerId: string;
  status: TransactionStatus;
  messageIds: string[];
  startedAt?: number;
  completedAt?: number;
};

export type HeartbeatState = {
  consumerId: string;
  groupId: string;
  lastHeartbeatAt: number;
  intervalMs: number;
  sessionTimeoutMs: number;
  missedHeartbeats: number;
};

export type BatchSettings = {
  batchSize: number;
  lingerMs: number;
  compression: "none" | "gzip" | "snappy" | "lz4" | "zstd";
};

export type SchemaDefinition = {
  subject: string;
  version: number;
  fields: string[];
  compatibility: "backward" | "forward" | "full" | "none";
};

export type StreamTopology = {
  id: string;
  sourceTopics: string[];
  sinkTopic: string;
  operations: string[];
};

export type AdvancedPolicies = {
  retries: number;
  idempotence: boolean;
  retentionMs: number;
  compaction: boolean;
  deadLetterTopic: string;
  exactlyOnce: boolean;
  sessionTimeoutMs: number;
  maxPollIntervalMs: number;
  cooperativeRebalance: boolean;
};

export type AdvancedState = {
  transactions: TransactionState[];
  heartbeats: HeartbeatState[];
  batchSettings: BatchSettings;
  schemas: SchemaDefinition[];
  streamTopologies: StreamTopology[];
  policies: AdvancedPolicies;
};

export type Counters = {
  event: number;
  message: number;
  consumer: number;
  producer: number;
  delivery: number;
  transaction: number;
  topology: number;
};

export type KafkaSimulationState = {
  clock: number;
  topic: Topic;
  producers: Producer[];
  groups: ConsumerGroup[];
  events: SimulationEvent[];
  deliveries: DeliveryAttempt[];
  lagHistory: LagSnapshot[];
  metrics: MetricsPoint[];
  config: SimulationConfig;
  counters: Counters;
  cluster?: BrokerCluster;
  advanced: AdvancedState;
};

export type SimulationOptions = {
  topicName?: string;
  partitionCount?: number;
  producerIds?: string[];
  startTime?: number;
  config?: Partial<SimulationConfig>;
};

export type ProducedMessageInput = {
  producerId?: string;
  key?: string | null;
  value: string;
  timestamp?: number;
  partition?: number;
};

export type PollOptions = {
  maxRecords?: number;
  partition?: number;
};

export type CommitMode = "processed" | "current";

export type SimulationResult<T> = {
  state: KafkaSimulationState;
  value: T;
  events: SimulationEvent[];
};
