import { beforeEach, describe, expect, it, vi } from "vitest";
import { buildChartTransitTokens, captureOffsetChanges } from "@/components/chart-activity";
import { CHART_VARIANTS, projectChart } from "@/components/chart-views";
import type { VisualEvent } from "@/components/flow-transit";
import { useKafkaStore } from "@/store/kafka-store";

describe("live chart activity", () => {
  beforeEach(() => useKafkaStore.getState().actions.loadReferenceFlow?.());

  function recordEvent(type: string, metadata: Record<string, unknown> = {}): VisualEvent {
    const snapshot = useKafkaStore.getState().snapshot;
    const message = snapshot.partitions[0].messages[0];
    const group = snapshot.consumerGroups[0];
    return { id: `fresh-${type}`, type, summary: type, concept: type, timestamp: 1, visualStartedAt: 100,
      metadata: { messageId: message.id, partition: message.partition, offset: message.offset,
        groupId: group.id, consumerId: group.assignments[message.partition], attempt: 2, ...metadata } };
  }

  it("routes a real append onto partition, log-end, replication and activity connections", () => {
    const baseline = new Set(useKafkaStore.getState().snapshot.events.map((event) => event.id));
    useKafkaStore.getState().actions.produce({ key: "order-live", value: "live", partition: 1, producerId: "payment-service" });
    const snapshot = useKafkaStore.getState().snapshot;
    const events = snapshot.events.filter((event) => !baseline.has(event.id));
    const expected = { flow: "producer-payment-service-topic", partitions: "topic-partition-1", offsets: "offset-1-end", replication: "replication-replica-1-broker-2", activity: "activity-root-produced" } as const;
    for (const [variant, edge] of Object.entries(expected)) {
      const projection = projectChart(snapshot, variant as keyof typeof expected);
      const tokens = buildChartTransitTokens(events, snapshot, projection);
      expect(tokens.some((token) => token.edgeId === edge)).toBe(true);
      expect(tokens.every((token) => projection.edges.some((edge) => edge.id === token.edgeId))).toBe(true);
    }
  });

  it("moves reads toward the actual consumer on the reversed ownership link", () => {
    const snapshot = useKafkaStore.getState().snapshot;
    const event = recordEvent("consumer-polled");
    const tokens = buildChartTransitTokens([event], snapshot, projectChart(snapshot, "consumers"));
    expect(tokens.find((token) => token.messageId)).toMatchObject({ edgeId: "consumer-consumer-0-partition-0", reverse: true, attempt: 2 });
    expect(buildChartTransitTokens([event], snapshot, projectChart(snapshot, "offsets"))[0].edgeId).toBe("offset-0-current");
    expect(buildChartTransitTokens([event], snapshot, projectChart(snapshot, "delivery"))[0].edgeId).toBe("delivery-queued-in-flight");
    const stale = recordEvent("consumer-polled", { consumerId: "wrong-owner" });
    expect(buildChartTransitTokens([stale], snapshot, projectChart(snapshot, "consumers"))).toEqual([]);
  });

  it("animates a checkpoint on the exact edge rendered by Flow and only within the committed range", () => {
    const snapshot = useKafkaStore.getState().snapshot;
    const event = recordEvent("offset-committed", { previous: 0, committedOffset: 1 });
    const projection = projectChart(snapshot, "flow");
    const tokens = buildChartTransitTokens([event], snapshot, projection);
    expect(tokens).toHaveLength(1);
    expect(tokens[0].edgeId).toBe("consumer-commit-consumer-0");
    expect(projection.edges.some((edge) => edge.id === tokens[0].edgeId)).toBe(true);
    expect(buildChartTransitTokens([event], snapshot, projectChart(snapshot, "offsets"))[0].edgeId).toBe("offset-0-committed");
  });

  it("keeps failure, retry and skipped work separate from successful delivery", () => {
    const snapshot = useKafkaStore.getState().snapshot;
    const projection = projectChart(snapshot, "delivery");
    const expected = { "record-processing-failed": ["delivery-in-flight-failed"], "record-processing-retried": ["delivery-failed-in-flight"],
      "record-processed": ["delivery-in-flight-succeeded"], "lost-delivery": ["delivery-in-flight-skipped"] };
    for (const [type, edges] of Object.entries(expected)) expect(buildChartTransitTokens([recordEvent(type)], snapshot, projection).map((token) => token.edgeId)).toEqual(edges);
    const lost = recordEvent("lost-delivery");
    const commit = recordEvent("offset-committed", { previous: 0, committedOffset: 1 });
    expect(buildChartTransitTokens([lost, commit], snapshot, projection).map((token) => token.edgeId)).toEqual(["delivery-in-flight-skipped"]);
    expect(buildChartTransitTokens([recordEvent("record-processed", { groupId: "other-group" })], snapshot, projection)).toEqual([]);
  });

  it("shows broker and rebalance signals without inventing a message identity", () => {
    const snapshot = useKafkaStore.getState().snapshot;
    const broker: VisualEvent = { id: "broker-down", type: "broker-failed", entityId: "broker-1", summary: "failed", concept: "broker", timestamp: 1 };
    const tokens = buildChartTransitTokens([broker], snapshot, projectChart(snapshot, "replication"));
    expect(tokens.length).toBeGreaterThan(0);
    expect(tokens.every((token) => token.edgeId.endsWith("-broker-1") && token.messageId === undefined)).toBe(true);
    const assignment = recordEvent("assignment-created");
    expect(buildChartTransitTokens([assignment], snapshot, projectChart(snapshot, "consumers")).map((token) => token.edgeId)).toEqual(["group-consumer-consumer-0", "consumer-consumer-0-partition-0"]);
  });

  it("caps every chart at 24 visual tokens and leaves the simulation unchanged", () => {
    const snapshot = useKafkaStore.getState().snapshot;
    const before = JSON.stringify(snapshot);
    const events = Array.from({ length: 60 }, (_, index) => ({ ...recordEvent("consumer-polled"), id: `poll-${index}` }));
    for (const variant of CHART_VARIANTS) expect(buildChartTransitTokens(events, snapshot, projectChart(snapshot, variant.id)).length).toBeLessThanOrEqual(24);
    expect(JSON.stringify(snapshot)).toBe(before);
  });
  it("counts each stored record in exactly one delivery state", () => {
    const snapshot = useKafkaStore.getState().snapshot;
    const projection = projectChart(snapshot, "delivery");
    expect(projection.nodes.reduce((sum, node) => sum + Number.parseInt(node.data.metric), 0)).toBe(snapshot.messageCount);
    expect(projection.nodes.some((node) => node.id === "delivery-processed")).toBe(false);
    expect(buildChartTransitTokens([recordEvent("record-processed")], snapshot, projection)).toHaveLength(1);
  });
  it("emits one Consumers checkpoint signal for a multi-record range", () => {
    const snapshot = useKafkaStore.getState().snapshot;
    const commit = recordEvent("offset-committed", { previous: 0, committedOffset: 3 });
    const tokens = buildChartTransitTokens([commit], snapshot, projectChart(snapshot, "consumers"));
    expect(tokens).toHaveLength(1);
    expect(new Set(tokens.map((token) => token.id)).size).toBe(tokens.length);
  });
  it("does not let producer activity evict an unrelated read from Consumers", () => {
    const snapshot = useKafkaStore.getState().snapshot;
    const poll = recordEvent("consumer-polled");
    const produces = Array.from({ length: 12 }, (_, index) => ({ ...recordEvent("message-produced", { producerId: "checkout-api" }), id: `append-${index}` }));
    const tokens = buildChartTransitTokens([poll, ...produces], snapshot, projectChart(snapshot, "consumers"));
    expect(tokens.some((token) => token.sourceEventId === poll.id && token.reverse)).toBe(true);
  });
  it("preserves record and attempt identity on membership and activity signals", () => {
    const snapshot = useKafkaStore.getState().snapshot;
    for (const type of ["record-processing-failed", "record-processing-retried"]) {
      const event = recordEvent(type);
      for (const variant of ["consumers", "activity"] as const) {
        expect(buildChartTransitTokens([event], snapshot, projectChart(snapshot, variant))[0]).toMatchObject({
          messageId: snapshot.partitions[0].messages[0].id, partition: 0, offset: 0, attempt: 2,
        });
      }
    }
  });
  it("captures actual restart/assignment position changes without fabricating record traffic", () => {
    const snapshot = useKafkaStore.getState().snapshot;
    const previous = { ...snapshot, partitions: snapshot.partitions.map((partition) => partition.id === 0 ? { ...partition, currentOffset: 4 } : partition) };
    const restart = recordEvent("consumer-restarted");
    const captured = captureOffsetChanges([restart], snapshot, previous);
    const tokens = buildChartTransitTokens(captured, snapshot, projectChart(snapshot, "offsets"));
    expect(tokens).toHaveLength(1);
    expect(tokens[0]).toMatchObject({ edgeId: "offset-0-current", previousOffset: 4, currentOffset: snapshot.partitions[0].currentOffset, sourceEventId: restart.id });
    expect(tokens[0].messageId).toBeUndefined();
    expect(captureOffsetChanges([restart], { ...snapshot, selectedGroupId: "different" }, previous)[0].offsetChanges).toBeUndefined();
  });
  it("captures the actual single-consumer read → crash → restart rewind at the crash boundary", () => {
    vi.useFakeTimers();
    const actions = useKafkaStore.getState().actions;
    try {
      actions.reset();
      const consumerId = useKafkaStore.getState().snapshot.consumers[0].id;
      actions.setConsumerAutoCommit?.(consumerId, false);
      actions.produce({ key: "rewind-order", value: "read without commit", partition: 0 });
      actions.startReading?.(); vi.advanceTimersByTime(300); actions.pause?.();
      const read = useKafkaStore.getState().snapshot;
      expect(read.partitions[0].currentOffset).toBe(1);
      expect(read.partitions[0].committedOffset).toBe(0);
      const oldIds = new Set(read.events.map((event) => event.id));
      actions.crashConsumer(consumerId);
      const crashed = useKafkaStore.getState().snapshot;
      expect(crashed.partitions[0].assignedConsumerId).toBeUndefined();
      expect(crashed.partitions[0].currentOffset).toBe(0);
      const events = captureOffsetChanges(crashed.events.filter((event) => !oldIds.has(event.id)), crashed, read);
      const tokens = buildChartTransitTokens(events, crashed, projectChart(crashed, "offsets"));
      expect(tokens).toHaveLength(1);
      expect(tokens[0]).toMatchObject({ edgeId: "offset-0-current", phase: "consumer-crashed", previousOffset: 1, currentOffset: 0 });
      const crashedIds = new Set(crashed.events.map((event) => event.id));
      actions.restartConsumer(consumerId);
      const restarted = useKafkaStore.getState().snapshot;
      const restartEvents = captureOffsetChanges(restarted.events.filter((event) => !crashedIds.has(event.id)), restarted, crashed);
      expect(buildChartTransitTokens(restartEvents, restarted, projectChart(restarted, "offsets"))).toEqual([]);
    } finally { actions.pause?.(); vi.useRealTimers(); }
  });
  it("does not attribute a position change to a different consumer despite matching partition metadata", () => {
    const snapshot = useKafkaStore.getState().snapshot;
    const previous = { ...snapshot, partitions: snapshot.partitions.map((partition) => partition.id === 0 ? { ...partition, currentOffset: 4 } : partition) };
    for (const type of ["consumer-crashed", "consumer-restarted"]) {
      const event = recordEvent(type, { consumerId: "wrong-owner", partition: 0 });
      const captured = captureOffsetChanges([event], snapshot, previous);
      expect(captured[0].offsetChanges).toBeUndefined();
      expect(buildChartTransitTokens(captured, snapshot, projectChart(snapshot, "offsets"))).toEqual([]);
    }
  });
});
