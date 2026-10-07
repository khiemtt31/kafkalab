import type { Topic } from "./types";

/**
 * @Simplification
 * Real Kafka clients commonly use a murmur2-compatible partitioner and may
 * choose a sticky partition for records without keys. The lab uses a tiny,
 * documented FNV-1a hash and round-robin for unkeyed records so the lesson is
 * deterministic and easy to inspect.
 */
export function stableHash(value: string): number {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

export function partitionForKey(key: string, partitionCount: number): number {
  if (partitionCount < 1) {
    throw new Error("A topic must have at least one partition");
  }
  return stableHash(key) % partitionCount;
}

export function selectPartition(topic: Topic, key: string | null): { partition: number; nextUnkeyedPartition: number } {
  const count = topic.partitions.length;
  if (count === 0) {
    throw new Error("Cannot select a partition on an empty topic");
  }
  if (key !== null && key.length > 0) {
    return { partition: partitionForKey(key, count), nextUnkeyedPartition: topic.nextUnkeyedPartition };
  }
  const partition = topic.nextUnkeyedPartition % count;
  return { partition, nextUnkeyedPartition: (partition + 1) % count };
}
