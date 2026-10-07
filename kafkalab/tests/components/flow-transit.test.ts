import { describe, expect, it } from "vitest";
import type { KafkaLabSnapshot, LabConsumer, LabConsumerGroup, LabMessage, LabTimelineEvent } from "@/hooks/useKafkaLab";
import { buildTransitTokens, transitMotionEnabled } from "@/components/flow-transit";

const GROUP_ID = "analytics-group";
const CONSUMER_ID = "consumer-0";

function message(partition: number, offset: number): LabMessage {
  return { id: `message-${partition}-${offset}`, key: `key-${offset}`, value: `order-${offset}`, timestamp: offset, partition, offset };
}

function event(id: string, type: string, metadata: Record<string, unknown>): LabTimelineEvent {
  return { id, timestamp: Number(id.replace(/\D/g, "")) || 1, type, summary: id, concept: "test", metadata };
}

function makeSnapshot(messages: LabMessage[], assignments: Record<number, string> = { 0: CONSUMER_ID, 1: CONSUMER_ID }): { snapshot: KafkaLabSnapshot; group: LabConsumerGroup } {
  const consumer: LabConsumer = { id: CONSUMER_ID, name: CONSUMER_ID, groupId: GROUP_ID, status: "running", assignedPartitions: Object.keys(assignments).map(Number), currentOffsets: {}, committedOffsets: {}, rate: 6, autoCommit: false };
  const group: LabConsumerGroup = { id: GROUP_ID, name: GROUP_ID, consumers: [consumer], assignments, committedOffsets: {}, lagByPartition: {} };
  const maxOffsetByPartition = new Map<number, number>();
  messages.forEach((candidate) => maxOffsetByPartition.set(candidate.partition, Math.max(maxOffsetByPartition.get(candidate.partition) ?? 0, candidate.offset + 1)));
  const snapshot: KafkaLabSnapshot = {
    resetRevision: 0,
    topicName: "orders",
    partitions: [0, 1].map((id) => ({ id, messages: messages.filter((candidate) => candidate.partition === id), logEndOffset: maxOffsetByPartition.get(id) ?? 0, assignedConsumerId: assignments[id], assignedConsumerName: assignments[id], committedOffset: 0, currentOffset: 0, lag: maxOffsetByPartition.get(id) ?? 0 })),
    consumers: [consumer],
    consumerGroups: [group],
    selectedGroupId: GROUP_ID,
    events: [],
    metrics: [],
    running: false,
    producerRunning: false,
    producerRate: 0,
    simulationSpeed: 1,
    messageCount: messages.length,
    producedPerSecond: 0,
    consumedPerSecond: 0,
    totalLag: messages.length,
    advanced: {} as KafkaLabSnapshot["advanced"],
  };
  return { snapshot, group };
}

describe("flow transit projection", () => {
  it("projects a produced record onto the producer and exact partition phases", () => {
    const { snapshot, group } = makeSnapshot([message(1, 0)]);
    const tokens = buildTransitTokens([event("produce-1", "message-produced", { messageId: "message-1-0", partition: 1, offset: 0 })], snapshot, group);

    expect(tokens.map((token) => token.edgeId)).toEqual(["producer-topic", "topic-partition-1"]);
    expect(tokens.map((token) => token.phase)).toEqual(["produced", "routed"]);
    expect(tokens.every((token) => token.messageId === "message-1-0" && token.partition === 1 && token.offset === 0 && token.sourceEventId === "produce-1")).toBe(true);
    expect(tokens[0].delayMs).toBe(0);
    expect(tokens[1].delayMs).toBeGreaterThan(tokens[0].durationMs);
  });

  it("routes every real producer over its own source edge", () => {
    const messages = [message(0, 0), message(0, 1), message(1, 0)];
    const { snapshot, group } = makeSnapshot(messages);
    const producerIds = ["checkout-api", "payment-service", "shipping-service"];
    snapshot.producers = producerIds.map((id) => ({ id, producedCount: 1 }));
    const produced = messages.map((record, index) => event(`produce-${index}`, "message-produced", { producerId: producerIds[index], messageId: record.id, partition: record.partition, offset: record.offset }));
    const tokens = buildTransitTokens(produced, snapshot, group);

    expect(tokens.filter((token) => token.phase === "produced").map((token) => token.edgeId)).toEqual(producerIds.map((id) => `producer-${id}-topic`));
    expect(tokens.filter((token) => token.phase === "produced").map((token) => token.producerId)).toEqual(producerIds);
    expect(tokens.some((token) => token.edgeId === "producer-topic")).toBe(false);
  });

  it("never assigns a missing, unknown, or conflicting real source to another producer", () => {
    const { snapshot, group } = makeSnapshot([{ ...message(0, 0), producerId: "checkout-api" }]);
    snapshot.producers = [{ id: "checkout-api", producedCount: 1 }, { id: "payment-service", producedCount: 0 }];
    for (const producerId of [undefined, "missing-service", "payment-service"]) {
      const produced = event("produce-unknown", "message-produced", { producerId, messageId: "message-0-0", partition: 0, offset: 0 });
      expect(buildTransitTokens([produced], snapshot, group).map((token) => token.edgeId)).toEqual(["topic-partition-0"]);
    }
  });

  it("uses short order labels while retaining exact record and attempt identity", () => {
    const record = { ...message(0, 0), key: "order-7F3K9", value: "payment-authorized-with-detailed-payload" };
    const { snapshot, group } = makeSnapshot([record]);
    const poll = event("poll-label", "consumer-polled", { messageId: record.id, groupId: GROUP_ID, consumerId: CONSUMER_ID, partition: 0, offset: 0, attempt: 2 });
    expect(buildTransitTokens([poll], snapshot, group)[0]).toMatchObject({ label: "7F3K9 · a2", messageId: record.id, partition: 0, offset: 0, attempt: 2 });
  });

  it("routes a poll only to the event consumer and assigned partition", () => {
    const { snapshot, group } = makeSnapshot([message(0, 0), message(1, 0)]);
    const poll = event("poll-1", "consumer-polled", { messageId: "message-1-0", groupId: GROUP_ID, consumerId: CONSUMER_ID, partition: 1, offset: 0, attempt: 2 });
    const tokens = buildTransitTokens([poll], snapshot, group);

    expect(tokens).toHaveLength(1);
    expect(tokens[0]).toMatchObject({ edgeId: "partition-consumer-1-consumer-0", messageId: "message-1-0", partition: 1, offset: 0, phase: "polled", attempt: 2, consumerId: CONSUMER_ID, groupId: GROUP_ID });
  });

  it("filters commit tokens to the exact newly committed offset range", () => {
    const messages = [message(0, 0), message(0, 1), message(0, 2)];
    const { snapshot, group } = makeSnapshot(messages);
    const commit = event("commit-1", "offset-committed", { groupId: GROUP_ID, consumerId: CONSUMER_ID, partition: 0, previous: 1, committedOffset: 3, mode: "processed" });
    const tokens = buildTransitTokens([commit], snapshot, group);

    expect(tokens.map((token) => token.messageId)).toEqual(["message-0-1", "message-0-2"]);
    expect(tokens.every((token) => token.edgeId === "consumer-commit-consumer-0" && token.phase === "committed" && token.consumerId === CONSUMER_ID && token.groupId === GROUP_ID)).toBe(true);
    expect(tokens.every((token) => token.label === "P0 C→3")).toBe(true);
  });

  it("preserves duplicate message identity while exposing a new attempt", () => {
    const { snapshot, group } = makeSnapshot([message(0, 0)]);
    const events = [1, 2].map((attempt) => event(`poll-${attempt}`, "consumer-polled", { messageId: "message-0-0", groupId: GROUP_ID, consumerId: CONSUMER_ID, partition: 0, offset: 0, attempt }));
    const tokens = buildTransitTokens(events, snapshot, group);

    expect(tokens.map((token) => token.messageId)).toEqual(["message-0-0", "message-0-0"]);
    expect(tokens.map((token) => token.attempt)).toEqual([1, 2]);
    expect(new Set(tokens.map((token) => token.id)).size).toBe(2);
  });

  it("keeps the delivery attempt on a checkpoint after its poll pulse expires", () => {
    const record = message(0, 0);
    record.delivery = { id: "delivery-3", messageId: record.id, groupId: GROUP_ID, consumerId: CONSUMER_ID, partition: 0, offset: 0, attempt: 3, status: "committed", applicationStatus: "succeeded", readAt: 0, processedAt: 1, committedAt: 2 };
    const { snapshot, group } = makeSnapshot([record]);
    const commit = event("commit-3", "offset-committed", { groupId: GROUP_ID, consumerId: CONSUMER_ID, partition: 0, previous: 0, committedOffset: 1 });
    expect(buildTransitTokens([commit], snapshot, group)[0]).toMatchObject({ attempt: 3, label: "P0 C→1" });
  });

  it("accepts a produced event entity ID when messageId metadata is unavailable", () => {
    const { snapshot, group } = makeSnapshot([message(0, 0)]);
    const produced = { ...event("produce-entity", "message-produced", { partition: 0, offset: 0 }), entityId: "message-0-0" } as LabTimelineEvent & { entityId: string };

    expect(buildTransitTokens([produced], snapshot, group).map((token) => token.messageId)).toEqual(["message-0-0", "message-0-0"]);
  });

  it("drops produced and poll events that have no message identity", () => {
    const { snapshot, group } = makeSnapshot([message(0, 0)]);
    const produced = event("produce-missing-id", "message-produced", { partition: 0, offset: 0 });
    const polled = event("poll-missing-id", "consumer-polled", { groupId: GROUP_ID, consumerId: CONSUMER_ID, partition: 0, offset: 0, attempt: 1 });

    expect(buildTransitTokens([produced, polled], snapshot, group)).toEqual([]);
  });

  it("keeps the visual projection bounded at 24 tokens", () => {
    const messages = Array.from({ length: 20 }, (_, offset) => message(0, offset));
    const { snapshot, group } = makeSnapshot(messages, { 0: CONSUMER_ID });
    const events = messages.map((candidate) => event(`produce-${candidate.offset}`, "message-produced", { messageId: candidate.id, partition: 0, offset: candidate.offset }));
    const tokens = buildTransitTokens(events, snapshot, group);

    expect(tokens).toHaveLength(24);
    expect(tokens.every((token) => token.edgeId === "producer-topic" || token.edgeId === "topic-partition-0")).toBe(true);
    expect(tokens.at(-1)?.sourceEventId).toBe("produce-19");
  });

  it("drops stale metadata instead of routing a token over an unrelated edge", () => {
    const { snapshot, group } = makeSnapshot([message(0, 0)]);
    const stalePartition = event("stale-partition", "message-produced", { messageId: "message-0-0", partition: 1, offset: 0 });
    const staleMessage = event("stale-message", "consumer-polled", { messageId: "wrong-message", groupId: GROUP_ID, consumerId: CONSUMER_ID, partition: 0, offset: 0, attempt: 1 });
    const staleConsumer = event("stale-consumer", "consumer-polled", { messageId: "message-0-0", groupId: GROUP_ID, consumerId: "consumer-elsewhere", partition: 0, offset: 0, attempt: 1 });

    expect(buildTransitTokens([stalePartition, staleMessage, staleConsumer], snapshot, group)).toEqual([]);
  });

  it("does not replay tokens without fresh events and suppresses motion when reduced or hidden", () => {
    const { snapshot, group } = makeSnapshot([message(0, 0)]);
    const produce = event("produce-1", "message-produced", { messageId: "message-0-0", partition: 0, offset: 0 });

    expect(buildTransitTokens([], snapshot, group)).toEqual([]);
    expect(transitMotionEnabled(false, true)).toBe(true);
    expect(transitMotionEnabled(true, true)).toBe(false);
    expect(transitMotionEnabled(false, false)).toBe(false);
    expect(produce.id).toBe("produce-1");
  });
});
