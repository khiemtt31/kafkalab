import { calculateLag } from "./lag";
import type { KafkaSimulationState } from "./types";

export type InvariantViolation = {
  code: string;
  message: string;
  entity?: string;
};

export function findInvariantViolations(state: KafkaSimulationState): InvariantViolation[] {
  const violations: InvariantViolation[] = [];
  for (const partition of state.topic.partitions) {
    const offsets = partition.messages.map((message) => message.offset);
    offsets.forEach((offset, index) => {
      if (offset !== index) violations.push({ code: "partition-offset-order", message: `Partition ${partition.id} has offset ${offset} at index ${index}`, entity: `partition-${partition.id}` });
    });
    if (partition.logEndOffset !== partition.messages.length) {
      violations.push({ code: "partition-leo", message: `Partition ${partition.id} LEO must equal the next append offset`, entity: `partition-${partition.id}` });
    }
  }
  for (const group of state.groups) {
    const owners = new Map<number, string>();
    for (const [partitionKey, consumerId] of Object.entries(group.assignments)) {
      const partition = Number(partitionKey);
      if (owners.has(partition)) violations.push({ code: "duplicate-owner", message: `Partition ${partition} has two owners in ${group.id}`, entity: group.id });
      owners.set(partition, consumerId);
      const consumer = group.consumers.find((candidate) => candidate.id === consumerId);
      if (!consumer || consumer.status === "dead" || !consumer.assignedPartitions.includes(partition)) {
        violations.push({ code: "invalid-owner", message: `Assignment ${partition} points to an inactive or inconsistent consumer`, entity: group.id });
      }
    }
    for (const partition of state.topic.partitions) {
      const committed = group.committedOffsets[partition.id] ?? 0;
      if (committed < 0) violations.push({ code: "negative-commit", message: `Committed offset cannot be negative`, entity: group.id });
      const lag = calculateLag(partition.logEndOffset, committed);
      if (lag < 0) violations.push({ code: "negative-lag", message: `Lag cannot be negative`, entity: group.id });
    }
  }
  return violations;
}

export function assertInvariants(state: KafkaSimulationState): void {
  const violations = findInvariantViolations(state);
  if (violations.length > 0) throw new Error(violations.map((violation) => `[${violation.code}] ${violation.message}`).join("; "));
}
