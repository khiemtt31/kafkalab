import { addEvent, cloneSimulationState, result } from "./simulation";
import type { Broker, BrokerCluster, KafkaSimulationState, ReplicaAssignment, SimulationEvent, SimulationResult } from "./types";

export function createBrokerCluster(state: KafkaSimulationState, brokerCount = 3, replicationFactor = 2, minInSyncReplicas = 1): SimulationResult<BrokerCluster> {
  const next = cloneSimulationState(state);
  const count = Math.max(1, Math.floor(brokerCount));
  const brokers: Broker[] = Array.from({ length: count }, (_, index) => ({ id: `broker-${index + 1}`, status: "up" }));
  const factor = Math.max(1, Math.min(count, Math.floor(replicationFactor)));
  const assignments: ReplicaAssignment[] = next.topic.partitions.map((partition) => {
    const replicas = Array.from({ length: factor }, (_, index) => brokers[(partition.id + index) % brokers.length].id);
    return {
      topic: next.topic.name,
      partition: partition.id,
      replicas,
      leader: replicas[0],
      inSyncReplicas: [...replicas],
    };
  });
  const minimum = Number.isFinite(minInSyncReplicas) ? Math.max(1, Math.min(factor, Math.floor(minInSyncReplicas))) : 1;
  next.cluster = { brokers, replicationFactor: factor, minInSyncReplicas: minimum, assignments, requiredAcks: "1", retryCount: 0 };
  return result(next, next.cluster);
}

export function setRequiredAcks(state: KafkaSimulationState, acks: BrokerCluster["requiredAcks"]): SimulationResult<BrokerCluster | undefined> {
  const next = cloneSimulationState(state);
  if (!next.cluster) return result(next, undefined);
  next.cluster.requiredAcks = acks;
  const event = addEvent(next, "acks-updated", `Required acknowledgements set to acks=${acks}`, "producer acknowledgement policy", "cluster", { acks });
  return result(next, next.cluster, [event]);
}

// @Broker
// @Replication
// This state illustrates leader/follower/ISR concepts. It does not persist data
// or implement broker-to-broker replication protocols.
export function failBroker(state: KafkaSimulationState, brokerId: string): SimulationResult<BrokerCluster | undefined> {
  const next = cloneSimulationState(state);
  if (!next.cluster) return result(next, undefined);
  const broker = next.cluster.brokers.find((candidate) => candidate.id === brokerId);
  if (!broker) throw new Error(`Broker ${brokerId} does not exist`);
  broker.status = "down";
  const events: SimulationEvent[] = [addEvent(next, "broker-failed", `${brokerId} failed`, "broker failure and replication", brokerId)];
  for (const assignment of next.cluster.assignments) {
    assignment.inSyncReplicas = assignment.inSyncReplicas.filter((replica) => replica !== brokerId);
    if (assignment.leader === brokerId) {
      const replacement = assignment.inSyncReplicas.find((replica) => next.cluster?.brokers.find((candidate) => candidate.id === replica)?.status === "up");
      if (replacement) {
        assignment.leader = replacement;
        events.push(addEvent(next, "leader-elected", `${replacement} became leader for ${assignment.topic}[${assignment.partition}]`, "leader election", replacement, {
          partition: assignment.partition,
          previousLeader: brokerId,
        }));
      }
    }
  }
  return result(next, next.cluster, events);
}

export function recoverBroker(state: KafkaSimulationState, brokerId: string): SimulationResult<BrokerCluster | undefined> {
  const next = cloneSimulationState(state);
  if (!next.cluster) return result(next, undefined);
  const broker = next.cluster.brokers.find((candidate) => candidate.id === brokerId);
  if (!broker) throw new Error(`Broker ${brokerId} does not exist`);
  broker.status = "up";
  for (const assignment of next.cluster.assignments) {
    if (assignment.replicas.includes(brokerId) && !assignment.inSyncReplicas.includes(brokerId)) assignment.inSyncReplicas.push(brokerId);
  }
  const event = addEvent(next, "broker-recovered", `${brokerId} recovered and rejoined ISR where assigned`, "replica recovery", brokerId);
  return result(next, next.cluster, [event]);
}

export function getReplicaAssignment(state: KafkaSimulationState, partition: number): ReplicaAssignment | undefined {
  const assignment = state.cluster?.assignments.find((candidate) => candidate.partition === partition);
  return assignment ? { ...assignment, replicas: [...assignment.replicas], inSyncReplicas: [...assignment.inSyncReplicas] } : undefined;
}

export function canProduceWithAcks(state: KafkaSimulationState, partition: number): { accepted: boolean; reason: string; inSyncReplicas: string[] } {
  const assignment = getReplicaAssignment(state, partition);
  if (!state.cluster || !assignment) return { accepted: true, reason: "No broker cluster configured; direct educational append", inSyncReplicas: [] };
  const liveISR = [...new Set(assignment.inSyncReplicas)].filter((replica) => assignment.replicas.includes(replica)
    && state.cluster?.brokers.some((broker) => broker.id === replica && broker.status === "up"));
  const leaderUp = state.cluster.brokers.some((broker) => broker.id === assignment.leader && broker.status === "up");
  if (!leaderUp) return { accepted: false, reason: "The partition has no available leader", inSyncReplicas: liveISR };
  // @Simplification
  // Acceptance models all CURRENT live ISR members acknowledging and the
  // minimum ISR requirement. It does not wait for every configured replica or
  // simulate network acknowledgements, broker storage, or consumer success.
  const minimum = state.cluster.minInSyncReplicas ?? 1;
  if (state.cluster.requiredAcks === "all" && (!liveISR.includes(assignment.leader) || liveISR.length < minimum)) {
    return { accepted: false, reason: `acks=all requires at least ${minimum} live in-sync replicas (min.insync.replicas)`, inSyncReplicas: liveISR };
  }
  return { accepted: true, reason: `acks=${state.cluster.requiredAcks} accepted by the educational cluster`, inSyncReplicas: liveISR };
}
