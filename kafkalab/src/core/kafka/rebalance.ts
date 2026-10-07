import {
  addEvent,
  cloneSimulationState,
  createConsumer,
  createConsumerGroup,
  getGroup,
  nextId,
  result,
} from "./simulation";
import type { Consumer, ConsumerGroup, KafkaSimulationState, SimulationEvent, SimulationResult } from "./types";

function activeConsumers(group: ConsumerGroup): Consumer[] {
  return group.consumers
    .filter((consumer) => consumer.status !== "dead")
    .sort((left, right) => left.id.localeCompare(right.id));
}

function rebalanceMutable(state: KafkaSimulationState, group: ConsumerGroup, reason: string): SimulationEvent[] {
  const events: SimulationEvent[] = [];
  group.rebalanceState = "in-progress";
  events.push(addEvent(state, "rebalance-started", `Rebalance started (${reason})`, "consumer group coordination", group.id, { reason }));

  for (const consumer of group.consumers) {
    consumer.assignedPartitions = [];
  }
  group.assignments = {};
  events.push(addEvent(state, "assignments-revoked", `Assignments revoked for ${group.id}`, "partition ownership changes", group.id));

  const consumers = activeConsumers(group);
  state.topic.partitions.forEach((partition, index) => {
    const consumer = consumers[index % consumers.length];
    if (!consumer) return;
    group.assignments[partition.id] = consumer.id;
    consumer.assignedPartitions.push(partition.id);
    group.committedOffsets[partition.id] ??= 0;
    consumer.currentPositions[partition.id] ??= group.committedOffsets[partition.id];
    events.push(
      addEvent(
        state,
        "assignment-created",
        `${state.topic.name}[${partition.id}] → ${consumer.id}`,
        "one active owner per partition in a group",
        consumer.id,
        { groupId: group.id, partition: partition.id, consumerId: consumer.id },
      ),
    );
  });

  group.rebalanceState = "stable";
  events.push(addEvent(state, "rebalance-completed", `Rebalance completed for ${group.id}`, "deterministic educational assignment", group.id, {
    consumers: consumers.map((consumer) => consumer.id),
  }));
  return events;
}

// @Rebalance
// @Simplification
// Real Kafka uses a group coordinator, heartbeats, protocol versions, and
// assignor negotiations. The lab performs a synchronous sorted round-robin so
// learners can see the ownership result and its timeline.
export function rebalanceGroup(state: KafkaSimulationState, groupId: string, reason = "manual"): SimulationResult<ConsumerGroup> {
  const next = cloneSimulationState(state);
  const group = getGroup(next, groupId);
  if (!group) throw new Error(`Consumer group ${groupId} does not exist`);
  const events = rebalanceMutable(next, group, reason);
  return result(next, group, events);
}

export function createGroup(state: KafkaSimulationState, groupId: string): SimulationResult<ConsumerGroup> {
  const next = cloneSimulationState(state);
  if (getGroup(next, groupId)) throw new Error(`Consumer group ${groupId} already exists`);
  const group = createConsumerGroup(groupId);
  next.groups.push(group);
  return result(next, group);
}

export function addConsumer(
  state: KafkaSimulationState,
  groupId: string,
  consumerId?: string,
): SimulationResult<Consumer> {
  let next = cloneSimulationState(state);
  let group = getGroup(next, groupId);
  if (!group) {
    group = createConsumerGroup(groupId);
    next.groups.push(group);
  }
  let id = consumerId;
  if (id === undefined) {
    // Explicitly named scenario members do not advance the generated counter.
    // Skip names used anywhere so store controls can identify each member.
    const existingIds = new Set(next.groups.flatMap((candidate) => candidate.consumers.map((consumer) => consumer.id)));
    do {
      id = nextId(next, "consumer", "consumer");
    } while (existingIds.has(id));
  }
  if (group.consumers.some((consumer) => consumer.id === id)) throw new Error(`Consumer ${id} already exists in ${groupId}`);
  const consumer = createConsumer(id, groupId, next.clock);
  group.consumers.push(consumer);
  const events: SimulationEvent[] = [
    addEvent(next, "consumer-joined", `${id} joined ${groupId}`, "consumer groups", id, { groupId, consumerId: id }),
  ];
  const rebalance = rebalanceMutable(next, group, "consumer joined");
  events.push(...rebalance);
  return result(next, consumer, events);
}

export function removeConsumer(
  state: KafkaSimulationState,
  groupId: string,
  consumerId: string,
): SimulationResult<ConsumerGroup> {
  const next = cloneSimulationState(state);
  const group = getGroup(next, groupId);
  if (!group) throw new Error(`Consumer group ${groupId} does not exist`);
  const index = group.consumers.findIndex((consumer) => consumer.id === consumerId);
  if (index < 0) throw new Error(`Consumer ${consumerId} does not exist in ${groupId}`);
  group.consumers.splice(index, 1);
  const events: SimulationEvent[] = [
    addEvent(next, "consumer-left", `${consumerId} left ${groupId}`, "consumer groups", consumerId, { groupId, consumerId }),
  ];
  events.push(...rebalanceMutable(next, group, "consumer left"));
  return result(next, group, events);
}

export function getAssignment(state: KafkaSimulationState, groupId: string): Record<number, string> {
  return { ...(getGroup(state, groupId)?.assignments ?? {}) };
}

export function getIdleConsumers(state: KafkaSimulationState, groupId: string): Consumer[] {
  return (getGroup(state, groupId)?.consumers ?? [])
    .filter((consumer) => consumer.status !== "dead" && consumer.assignedPartitions.length === 0)
    .map((consumer) => ({ ...consumer, assignedPartitions: [...consumer.assignedPartitions] }));
}

// Kept available from the group module because the UI adapter commonly needs
// to inspect a group before dispatching a membership action.
export { getGroup } from "./simulation";
