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
});
