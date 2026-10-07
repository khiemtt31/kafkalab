"use client";

import { createContext, useContext } from "react";
import type { AdvancedPolicies, AdvancedState, BatchSettings, BrokerCluster, DeliveryAttempt, TransactionState } from "@/core/kafka/types";
import type { ReactNode } from "react";

export type KafkaStatus = "running" | "paused" | "dead";

export type LabProducer = {
  id: string;
  producedCount: number;
};

export type LabMessage = {
  id: string;
  /** Local teaching provenance, absent on older saved records. */
  producerId?: string;
  key: string | null;
  value: string;
  timestamp: number;
  partition: number;
  offset: number;
  /** Latest application attempt for the selected consumer group, if it read this record. */
  delivery?: DeliveryAttempt;
};

export type LabPartition = {
  id: number;
  messages: LabMessage[];
  logEndOffset: number;
  /** The active owner for the currently selected group, when one exists. */
  assignedConsumerId?: string;
  assignedConsumerName?: string;
  committedOffset?: number;
  currentOffset?: number;
  lag?: number;
};

export type LabConsumer = {
  id: string;
  name?: string;
  groupId: string;
  status: KafkaStatus;
  assignedPartitions: number[];
  currentOffsets: Record<number, number>;
  committedOffsets: Record<number, number>;
  rate?: number;
  autoCommit?: boolean;
};

export type LabConsumerGroup = {
  id: string;
  name?: string;
  consumers: LabConsumer[];
  assignments: Record<number, string>;
  committedOffsets: Record<number, number>;
  lagByPartition: Record<number, number>;
};

export type LabTimelineEvent = {
  id: string;
  entityId?: string;
  timestamp: number;
  type: string;
  summary: string;
  concept: string;
  metadata?: Record<string, unknown>;
};

export type LabMetricPoint = {
  timestamp: number | string;
  lag: number;
  produced: number;
  consumed: number;
};

export type LabScenario = {
  id: string;
  name: string;
  description: string;
  learningGoals: string[];
  goal: string;
  action: string;
  observation: string;
  explanation: string;
};

export type InspectorTarget = {
  /** Numeric observations returned by an executed scenario, never expected values. */
  observations?: Record<string, number>;
  kind: "producer" | "topic" | "partition" | "message" | "consumer" | "group" | "offset" | "edge";
  id: string;
  title: string;
  state: string;
  whyChanged: string;
  concept: string;
  relatedCode: string;
  simplification: string;
};

export type KafkaLabSnapshot = {
  /** Ephemeral history watermark: seeded/restored events never replay as packets. */
  flowBaselineEventId?: string;
  /** Increments when a fresh simulation should restore the canonical graph layout. */
  resetRevision?: number;
  topicName: string;
  producers?: LabProducer[];
  partitions: LabPartition[];
  consumers: LabConsumer[];
  consumerGroups: LabConsumerGroup[];
  selectedGroupId: string;
  events: LabTimelineEvent[];
  metrics: LabMetricPoint[];
  running: boolean;
  producerRunning: boolean;
  producerRate: number;
  simulationSpeed: number;
  inspector?: InspectorTarget;
  activeScenario?: LabScenario;
  messageCount: number;
  producedPerSecond: number;
  consumedPerSecond: number;
  totalLag: number;
  cluster?: BrokerCluster;
  advanced: AdvancedState;
};

export type ProduceInput = { key: string | null; value: string; partition?: number; producerId?: string };

export type PracticeCategory = "easy" | "middle" | "hard" | "extreme-hard" | "most-common" | "trick" | "situation";
export type PracticeProgress = {
  selectedCategory: PracticeCategory | null;
  currentQuestion: Partial<Record<PracticeCategory, number>>;
  answers: Partial<Record<PracticeCategory, Record<string, string>>>;
};
export type FlowLayout = {
  positions: Record<string, { x: number; y: number }>;
  viewport: { x: number; y: number; zoom: number } | null;
  views?: Partial<Record<string, { positions: Record<string, { x: number; y: number }>; viewport: { x: number; y: number; zoom: number } | null }>>;
};

export type KafkaLabActions = {
  produce: (input: ProduceInput) => void;
  produceBatch?: (input: { keyPrefix?: string; valuePrefix?: string; count: number; producerId?: string }) => void;
  addProducer?: (producerId?: string) => void;
  start?: () => void;
  /** Run consumers without generating additional records. */
  startReading?: () => void;
  pause?: () => void;
  reset: () => void;
  loadReferenceFlow?: () => void;
  retryRecordProcessing?: (input: { partition: number; offset: number }) => void;
  configureTopology?: (input: { topicName: string; partitionCount: number }) => void;
  setProducerRate?: (rate: number) => void;
  setSimulationSpeed?: (speed: number) => void;
  addConsumer: (groupId?: string) => void;
  removeConsumer: (consumerId: string) => void;
  pauseConsumer: (consumerId: string) => void;
  resumeConsumer: (consumerId: string) => void;
  crashConsumer: (consumerId: string) => void;
  restartConsumer: (consumerId: string) => void;
  commitConsumer: (consumerId: string) => void;
  commitCurrentConsumer: (consumerId: string) => void;
  startProducer?: () => void;
  pauseProducer?: () => void;
  setConsumerRate?: (consumerId: string, rate: number) => void;
  setConsumerAutoCommit?: (consumerId: string, enabled: boolean) => void;
  selectGroup?: (groupId: string) => void;
  inspect?: (target: InspectorTarget) => void;
  closeInspector?: () => void;
  loadScenario?: (scenarioId: string) => void;
  runScenario?: () => void;
  createCluster?: () => void;
  setAcks?: (acks: "0" | "1" | "all") => void;
  failBroker?: (brokerId: string) => void;
  recoverBroker?: (brokerId: string) => void;
  beginTransaction?: () => void;
  appendTransactionMessage?: (transactionId: string) => void;
  commitTransaction?: (transactionId: string) => void;
  abortTransaction?: (transactionId: string) => void;
  recordHeartbeat?: (consumerId: string) => void;
  configureBatching?: (settings: Partial<BatchSettings>) => void;
  configurePolicies?: (settings: Partial<AdvancedPolicies>) => void;
  registerSchema?: () => void;
  createStreamTopology?: () => void;
  hydrate?: () => void;
  setFlowPositions?: (positions: Record<string, { x: number; y: number }>, viewId?: string) => void;
  setFlowViewport?: (viewport: { x: number; y: number; zoom: number }, viewId?: string) => void;
  selectPracticeCategory?: (category: PracticeCategory) => void;
  selectPracticeQuestion?: (category: PracticeCategory, questionIndex: number) => void;
  answerPracticeQuestion?: (category: PracticeCategory, questionId: string, optionId: string) => void;
  resetPracticeCategory?: (category: PracticeCategory) => void;
};

export type KafkaLabStore = {
  snapshot: KafkaLabSnapshot;
  actions: KafkaLabActions;
  flowLayout?: FlowLayout;
  practiceProgress?: PracticeProgress;
  hydrated?: boolean;
  storageNotice?: string;
};

const KafkaLabContext = createContext<KafkaLabStore | null>(null);

export function KafkaLabProvider({
  store,
  children,
}: {
  store: KafkaLabStore;
  children: ReactNode;
}) {
  return <KafkaLabContext.Provider value={store}>{children}</KafkaLabContext.Provider>;
}

/**
 * The UI depends on this adapter rather than directly importing a Zustand store.
 * A store selector hook can construct the object and pass it to KafkaLabProvider.
 */
export function useKafkaLab(providedStore?: KafkaLabStore): KafkaLabStore {
  const contextStore = useContext(KafkaLabContext);
  const store = providedStore ?? contextStore;
  if (!store) {
    throw new Error("KafkaLabProvider or a KafkaLabStore prop is required.");
  }
  return store;
}

export function useKafkaLabStore(): KafkaLabStore {
  return useKafkaLab();
}
