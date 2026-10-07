import { addConsumer } from "../core/kafka/rebalance";
import { pollConsumer, processRecords } from "../core/kafka/consumer";
import { produceBatch, produceMessage } from "../core/kafka/producer";
import { createSimulationState } from "../core/kafka/simulation";
import type { KafkaSimulationState, SimulationResult } from "../core/kafka/types";

export type ScenarioId =
  | "order-lifecycle"
  | "basic-produce-consume"
  | "keyed-partitioning"
  | "slow-consumer"
  | "consumer-scaling"
  | "rebalance"
  | "duplicate-processing"
  | "loss-before-processing"
  | "multiple-groups";

export type ScenarioDefinition = {
  id: ScenarioId;
  name: string;
  description: string;
  learningGoals: string[];
  goal: string;
  action: string;
  observation: string;
  explanation: string;
  setup: () => KafkaSimulationState;
};

function withConsumer(state: KafkaSimulationState, groupId: string, consumerId: string): KafkaSimulationState {
  return addConsumer(state, groupId, consumerId).state;
}

function withMessages(state: KafkaSimulationState, values: Array<{ key?: string | null; value: string }>): KafkaSimulationState {
  return produceBatch(state, values).state;
}

export const basicProduceConsume: ScenarioDefinition = {
  id: "basic-produce-consume",
  name: "Producer → Partition",
  description: "Follow a record from a producer into an append-only partition log.",
  learningGoals: ["topic", "partition", "offset", "append-only log"],
  goal: "Understand how a record gets a partition-local offset.",
  action: "Produce a record, then inspect its partition and offset.",
  observation: "The record appears at the next offset and the partition LEO increases.",
  explanation: "Kafka ordering is guaranteed inside a partition. This lab allocates offsets locally per partition.",
  setup: () => createSimulationState({ partitionCount: 3 }),
};

export const orderLifecycle: ScenarioDefinition = {
  id: "order-lifecycle",
  name: "Order lifecycle",
  description: "Follow several events for one order key through a partition log.",
  learningGoals: ["event-driven design", "record key", "partition-local order", "consumer lag"],
  goal: "Connect a real business entity to Kafka's keys, partition routing, offsets, and consumers.",
  action: "Run the example to append order-created, payment-authorized, inventory-reserved, and order-shipped for order-7F3K9.",
  observation: "All four events use the same key, land in one partition, and receive increasing offsets. The consumer group can read that log independently.",
  explanation: "With a stable partition count and partitioner, a key commonly keeps related events together so one partition provides their order. Other order keys may land in other partitions and progress concurrently; Kafka does not create a global order across the topic.",
  setup: () => createSimulationState({ topicName: "orders", partitionCount: 3 }),
};

export const keyedPartitioning: ScenarioDefinition = {
  id: "keyed-partitioning",
  name: "Message Keys",
  description: "Compare deterministic routing of records with the same key.",
  learningGoals: ["key-based partitioning", "partition ordering", "deterministic hashing"],
  goal: "See that the same key repeatedly selects the same partition while the topology is unchanged.",
  action: "Produce two records with the same key and one with a different key.",
  observation: "Records with the same key route together under the lab's documented hash.",
  explanation: "With a fixed partition count, the real client partitioner normally keeps a key on one partition, but partition-count changes or a different partitioner can change that mapping. This simulator uses a stable FNV-1a teaching hash.",
  setup: () => createSimulationState({ partitionCount: 3 }),
};

export const slowConsumer: ScenarioDefinition = {
  id: "slow-consumer",
  name: "Slow Consumer",
  description: "Create more records than a consumer commits and watch lag grow.",
  learningGoals: ["log-end-offset", "committed offset", "consumer lag"],
  goal: "Understand consumer lag.",
  action: "Produce records faster than the consumer commits them.",
  observation: "LEO advances while committed offsets stay behind, so lag grows.",
  explanation: "Lag is the log end offset minus the next committed offset. Records can remain available while a group is behind, but retention can still move the earliest available offset forward before the group catches up.",
  setup: () => {
    let state = withConsumer(createSimulationState({ partitionCount: 3 }), "analytics", "consumer-1");
    state = withMessages(state, Array.from({ length: 12 }, (_, index) => ({ value: `slow-${index + 1}` })));
    return state;
  },
};

export const consumerScaling: ScenarioDefinition = {
  id: "consumer-scaling",
  name: "Scaling Consumers",
  description: "Observe one group distribute partitions across consumers and leave extras idle.",
  learningGoals: ["consumer groups", "partition ownership", "idle consumers"],
  goal: "Understand that each partition has at most one active owner inside a group.",
  action: "Add consumers until there are more consumers than partitions.",
  observation: "Partitions move between consumers and extra consumers have no assignment.",
  explanation: "Different groups can independently read the same partition, but one group shares ownership.",
  setup: () => {
    let state = createSimulationState({ partitionCount: 3 });
    state = withConsumer(state, "analytics", "consumer-1");
    state = withConsumer(state, "analytics", "consumer-2");
    state = withConsumer(state, "analytics", "consumer-3");
    state = withConsumer(state, "analytics", "consumer-4");
    return state;
  },
};

export const rebalance: ScenarioDefinition = {
  id: "rebalance",
  name: "Rebalance",
  description: "Watch deterministic assignments change when a group member joins or leaves.",
  learningGoals: ["join", "leave", "assignment revoke", "rebalance"],
  goal: "Understand why consumer membership changes partition ownership.",
  action: "Add or remove a consumer and inspect the event timeline.",
  observation: "The timeline shows join, revoke, assignment, and completion events.",
  explanation: "The transition is synchronous and round-robin here; Kafka uses a distributed coordinator and negotiated assignment protocol, which may use eager or incremental cooperative movement depending on the protocol and assignor.",
  setup: () => {
    let state = createSimulationState({ partitionCount: 3 });
    state = withConsumer(state, "analytics", "consumer-1");
    return state;
  },
};

export const duplicateProcessing: ScenarioDefinition = {
  id: "duplicate-processing",
  name: "Duplicate Processing",
  description: "Process a record, crash before commit, restart, and observe a second delivery.",
  learningGoals: ["read", "process", "commit", "at-least-once duplicate"],
  goal: "See why processing before committing can repeat work after a crash.",
  action: "Poll and process a record, crash before committing, restart, then poll again.",
  observation: "The same message is read again with a duplicate-delivery event.",
  explanation: "At-least-once is a possible application strategy; Kafka itself does not force one delivery guarantee.",
  setup: () => {
    let state = createSimulationState({ partitionCount: 1 });
    state = withConsumer(state, "analytics", "consumer-1");
    state = produceMessage(state, { value: "process-before-commit" }).state;
    const polled = pollConsumer(state, "analytics", "consumer-1");
    state = polled.state;
    return processRecords(state, "analytics", "consumer-1").state;
  },
};

export const multipleGroups: ScenarioDefinition = {
  id: "multiple-groups",
  name: "Multiple Consumer Groups",
  description: "Read one topic from independent groups and compare their commits.",
  learningGoals: ["independent offsets", "fan-out", "group identity"],
  goal: "Understand that each consumer group owns its own committed offsets.",
  action: "Consume from analytics, then inspect the payments group.",
  observation: "One group's committed offsets move without moving the other group's offsets.",
  explanation: "Group identity is part of the offset key; the same partition can be read independently by many groups.",
  setup: () => {
    let state = createSimulationState({ partitionCount: 2 });
    state = withMessages(state, ["a", "b", "c"].map((value) => ({ value })));
    state = withConsumer(state, "analytics", "analytics-1");
    state = withConsumer(state, "payments", "payments-1");
    return state;
  },
};

export const lossBeforeProcessing: ScenarioDefinition = {
  id: "loss-before-processing",
  name: "Commit too soon",
  description: "Save the read position before processing, then restart and see the skipped work.",
  learningGoals: ["read position", "committed offset", "at-most-once loss"],
  goal: "See why a saved offset does not prove that a record was processed.",
  action: "Read one record, commit its current position before processing, then crash and restart.",
  observation: "The group restarts at offset 1 and reads nothing, although offset 0 was never processed.",
  explanation: "The record is still in this in-memory topic immediately after the action. This group skips its work because it saved the next read offset too early; real topic retention can eventually remove any record.",
  setup: () => {
    let state = createSimulationState({ partitionCount: 1 });
    state = withConsumer(state, "analytics", "consumer-1");
    state = produceMessage(state, { value: "commit-before-processing" }).state;
    return pollConsumer(state, "analytics", "consumer-1", { maxRecords: 1 }).state;
  },
};

export const scenarioDefinitions: ScenarioDefinition[] = [
  orderLifecycle,
  basicProduceConsume,
  keyedPartitioning,
  slowConsumer,
  consumerScaling,
  rebalance,
  duplicateProcessing,
  lossBeforeProcessing,
  multipleGroups,
];

export const scenarios = scenarioDefinitions;

export function getScenario(id: ScenarioId | string): ScenarioDefinition | undefined {
  return scenarioDefinitions.find((scenario) => scenario.id === id);
}

export function loadScenario(id: ScenarioId): KafkaSimulationState {
  const scenario = getScenario(id);
  if (!scenario) throw new Error(`Scenario ${id} does not exist`);
  return scenario.setup();
}

export function resetScenario(id: ScenarioId): KafkaSimulationState {
  return loadScenario(id);
}
