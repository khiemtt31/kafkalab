import { beforeEach, describe, expect, it } from "vitest";
import { CHART_VARIANTS, chartLayout, projectChart } from "@/components/chart-views";
import { useKafkaStore } from "@/store/kafka-store";

describe("beginner chart view projections", () => {
  beforeEach(() => {
    useKafkaStore.getState().actions.reset();
  });

  it("projects every tab from real snapshot data with stable bounded graph identities", () => {
    const snapshot = useKafkaStore.getState().snapshot;

    for (const variant of CHART_VARIANTS) {
      const first = projectChart(snapshot, variant.id);
      const second = projectChart(snapshot, variant.id);
      expect(first.nodes.map((node) => node.id)).toEqual(second.nodes.map((node) => node.id));
      expect(first.edges.map((edge) => edge.id)).toEqual(second.edges.map((edge) => edge.id));
      expect(first.nodes.length).toBeLessThanOrEqual(40);
      expect(first.nodes.every((node) => !/(message|record)-\d+/i.test(node.id))).toBe(true);
      expect(first.edges.every((edge) => first.nodes.some((node) => node.id === edge.source) && first.nodes.some((node) => node.id === edge.target))).toBe(true);
      expect(first.nodes.every((node) => node.data.tooltip.rows.every((row) => typeof row.value === "string"))).toBe(true);
    }
  });

  it("shows explicit unavailable replication data without inventing brokers", () => {
    const projection = projectChart(useKafkaStore.getState().snapshot, "replication");
    expect(projection.nodes.map((node) => node.id)).toEqual(["topic", "replication-unavailable"]);
    expect(projection.nodes[1].data.tooltip.rows).toEqual(expect.arrayContaining([
      { label: "Replication factor", value: "unavailable" },
      { label: "In-sync replicas", value: "unavailable" },
    ]));
  });

  it("projects broker, leader, replica, and ISR information when the reference cluster exists", () => {
    useKafkaStore.getState().actions.loadReferenceFlow?.();
    const snapshot = useKafkaStore.getState().snapshot;
    const projection = projectChart(snapshot, "replication");
    expect(snapshot.cluster?.brokers.length).toBeGreaterThan(0);
    expect(projection.nodes.some((node) => node.id.startsWith("broker-"))).toBe(true);
    expect(projection.edges.some((edge) => edge.data?.label === "leader" || edge.data?.label === "ISR replica")).toBe(true);
    expect(projection.nodes.find((node) => node.id.startsWith("partition-"))?.data.tooltip.rows).toEqual(expect.arrayContaining([
      expect.objectContaining({ label: "Leader" }),
      expect.objectContaining({ label: "In-sync replicas" }),
    ]));
  });

  it("normalizes legacy positions into Flow while keeping other variants independent", () => {
    const legacy = { positions: { topic: { x: 12, y: 18 } }, viewport: { x: 1, y: 2, zoom: 0.8 } };
    expect(chartLayout(legacy, "flow")).toEqual(legacy);
    expect(chartLayout(legacy, "partitions")).toEqual({ positions: {}, viewport: null });
    expect(chartLayout({ ...legacy, views: { partitions: { positions: { topic: { x: 80, y: 90 } }, viewport: null } } }, "partitions")).toEqual({ positions: { topic: { x: 80, y: 90 } }, viewport: null });
  });

  it("pinpoints failed application work on one path and keeps success separate from its checkpoint", () => {
    const actions = useKafkaStore.getState().actions;
    actions.loadReferenceFlow?.();
    const before = useKafkaStore.getState().snapshot;
    const serialized = JSON.stringify(before);
    const focus = { focused: true, focusPartitionId: 1 };
    const projection = projectChart(before, "flow", focus);
    expect(projection.nodes.map((node) => node.id)).toEqual(expect.arrayContaining(["partition-1", "consumer-consumer-1", "handler-1", "outcome-success-1", "outcome-failure-1", "commit"]));
    expect(projection.nodes).toHaveLength(8);
    expect(projection.nodes.some((node) => node.id === "partition-0" || node.id === "consumer-consumer-0")).toBe(false);
    const failure = projection.nodes.find((node) => node.id === "outcome-failure-1")!;
    expect(failure.data.problem).toBe(before.partitions[1].messages[0].delivery?.failureReason);
    expect(failure.data.metric).toMatch(/offset 0.*attempt 1/);
    expect(projection.nodes.find((node) => node.id === "topic")?.data.inline).toEqual(expect.arrayContaining([{ label: "key", value: before.partitions[1].messages[0].key! }]));
    expect(projection.edges.find((edge) => edge.id === "consumer-commit-consumer-1")?.data?.relationship).toBe("checkpoint");
    expect(projection.edges.find((edge) => edge.id === "handler-failure-1")?.data?.relationship).toBe("learning");
    expect(JSON.stringify(before)).toBe(serialized);

    actions.retryRecordProcessing?.({ partition: 1, offset: 0 });
    const after = projectChart(useKafkaStore.getState().snapshot, "flow", focus);
    expect(after.nodes.find((node) => node.id === "outcome-success-1")?.data.status).toBe("retry attempt 2 succeeded");
    expect(after.nodes.find((node) => node.id === "outcome-failure-1")?.data.problem).toBeUndefined();
    expect(after.nodes.find((node) => node.id === "commit")?.data.metric).toBe("P1 next offset 0");
    expect(after.nodes.map((node) => node.id)).toEqual(projection.nodes.map((node) => node.id));
  });

  it("filters unrelated lanes and owners while keeping Delivery and Activity group-wide", () => {
    useKafkaStore.getState().actions.loadReferenceFlow?.();
    const snapshot = useKafkaStore.getState().snapshot;
    for (const variant of ["partitions", "consumers", "offsets", "replication"] as const) {
      const projection = projectChart(snapshot, variant, { focused: true, focusPartitionId: 2 });
      expect(projection.nodes.filter((node) => node.data.kind === "partition").map((node) => node.id)).toEqual(["partition-2"]);
      expect(projection.nodes.filter((node) => node.data.kind === "consumer").every((node) => node.id === "consumer-consumer-2")).toBe(true);
      expect(projection.edges.every((edge) => projection.nodes.some((node) => node.id === edge.source) && projection.nodes.some((node) => node.id === edge.target))).toBe(true);
    }
    for (const variant of ["delivery", "activity"] as const) {
      expect(projectChart(snapshot, variant, { focused: true, focusPartitionId: 2 })).toEqual(projectChart(snapshot, variant));
    }
  });

  it("shows an early checkpoint as skipped work rather than a successful or retryable outcome", () => {
    useKafkaStore.getState().actions.loadReferenceFlow?.();
    const actions = useKafkaStore.getState().actions;
    actions.commitCurrentConsumer("consumer-3");
    const snapshot = useKafkaStore.getState().snapshot;
    expect(snapshot.partitions[3].messages[0].delivery?.status).toBe("lost");
    const projection = projectChart(snapshot, "flow", { focused: true, focusPartitionId: 3 });
    expect(projection.nodes.find((node) => node.id === "outcome-failure-3")?.data).toMatchObject({ title: "Work was skipped", problem: "Saved offset 1 has passed offset 0 without successful work." });
    expect(projection.nodes.find((node) => node.id === "outcome-success-3")?.data.metric).toBe("0 records");
    expect(projection.edges.some((edge) => edge.id === "failure-handler-3")).toBe(false);
  });

  it("keeps old records with unknown provenance from being attributed to an arbitrary producer", () => {
    useKafkaStore.getState().actions.loadReferenceFlow?.();
    const snapshot = useKafkaStore.getState().snapshot;
    const legacy = { ...snapshot, partitions: snapshot.partitions.map((partition) => partition.id === 1 ? { ...partition, messages: partition.messages.map((message) => ({ ...message, producerId: undefined })) } : partition) };
    const projection = projectChart(legacy, "flow", { focused: true, focusPartitionId: 1 });
    expect(projection.nodes.find((node) => node.id === "origin-unavailable")?.data.title).toBe("Unknown producer");
    expect(projection.nodes.some((node) => node.id === "producer-checkout-api")).toBe(false);
    expect(projection.edges.some((edge) => edge.id === "producer-checkout-api-topic")).toBe(false);
    expect(projection.edges.find((edge) => edge.id === "origin-unavailable-topic")?.data?.relationship).toBe("learning");
  });
});
