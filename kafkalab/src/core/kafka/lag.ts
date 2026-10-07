import { appendLagSnapshot, cloneSimulationState, getGroup, result } from "./simulation";
import type { KafkaSimulationState, LagSnapshot, SimulationResult } from "./types";

// @Lag
// Because committedOffset is the next record to read, lag is the number of
// records from that position through the current log end.
export function calculateLag(logEndOffset: number, committedOffset: number): number {
  return Math.max(0, logEndOffset - committedOffset);
}

export function getPartitionLag(state: KafkaSimulationState, groupId: string, partitionId: number): number {
  const partition = state.topic.partitions.find((candidate) => candidate.id === partitionId);
  if (!partition) return 0;
  return calculateLag(partition.logEndOffset, getGroup(state, groupId)?.committedOffsets[partitionId] ?? 0);
}

export function getGroupLag(state: KafkaSimulationState, groupId: string): { total: number; byPartition: Record<number, number> } {
  const byPartition: Record<number, number> = {};
  let total = 0;
  for (const partition of state.topic.partitions) {
    const lag = getPartitionLag(state, groupId, partition.id);
    byPartition[partition.id] = lag;
    total += lag;
  }
  return { total, byPartition };
}

export function snapshotLag(state: KafkaSimulationState, groupId: string): SimulationResult<LagSnapshot> {
  const next = cloneSimulationState(state);
  const values = getGroupLag(next, groupId);
  const snapshot: LagSnapshot = { at: next.clock, groupId, total: values.total, byPartition: values.byPartition };
  appendLagSnapshot(next, snapshot);
  return result(next, snapshot);
}

export function getTotalLag(state: KafkaSimulationState): number {
  return state.groups.reduce((total, group) => total + getGroupLag(state, group.id).total, 0);
}
