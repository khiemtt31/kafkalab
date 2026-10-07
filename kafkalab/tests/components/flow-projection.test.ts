import { describe, expect, it } from "vitest";
import { compactFlowPositions, projectPartitionStats, projectPartitionSummary, recordState, recordStateLabel, visiblePartitionMessages } from "@/components/flow-projection";

const message = (offset: number, delivery?: { status?: string; applicationStatus?: "pending" | "succeeded" | "failed" }) => ({ offset, delivery });

describe("technical flow projection", () => {
  it("keeps a skipped record distinct from a successfully processed record", () => {
    expect(recordState(message(0), 1)).toBe("skipped");
    expect(recordState(message(0, { status: "committed", applicationStatus: "pending" }), 1)).toBe("skipped");
    expect(recordState(message(0, { status: "lost", applicationStatus: "pending" }), 1)).toBe("skipped");
    expect(recordState(message(0, { status: "read", applicationStatus: "pending" }), 1)).toBe("skipped");
    expect(recordState(message(1, { applicationStatus: "succeeded" }), 1)).toBe("succeeded");
    expect(recordStateLabel("in-flight")).toBe("in flight");
  });

  it("projects read, processed, and failed counts from delivery state", () => {
    expect(projectPartitionStats([
      message(0, { status: "read", applicationStatus: "pending" }),
      message(1, { status: "processed", applicationStatus: "succeeded" }),
      message(2, { status: "read", applicationStatus: "failed" }),
      message(3),
    ], 0)).toEqual({ read: 3, processed: 1, failed: 1, committed: 0 });
  });

  it("keeps only the bounded newest records in offset order", () => {
    const messages = Array.from({ length: 8 }, (_, offset) => ({ offset }));
    expect(visiblePartitionMessages(messages, 6).map((item) => item.offset)).toEqual([2, 3, 4, 5, 6, 7]);
    expect(visiblePartitionMessages(messages, 0).map((item) => item.offset)).toEqual([7]);
    expect(visiblePartitionMessages([...messages].reverse(), 6).map((item) => item.offset)).toEqual([2, 3, 4, 5, 6, 7]);
  });

  it("keeps the oldest unresolved failure reachable as later records arrive", () => {
    const messages = Array.from({ length: 12 }, (_, offset) => message(offset, offset === 0 ? { applicationStatus: "failed" } : undefined));
    expect(visiblePartitionMessages(messages, 6).map((item) => item.offset)).toEqual([0, 7, 8, 9, 10, 11]);
    expect(visiblePartitionMessages(messages, 1).map((item) => item.offset)).toEqual([0]);
    messages[0].delivery = { applicationStatus: "succeeded" };
    expect(visiblePartitionMessages(messages, 6).map((item) => item.offset)).toEqual([6, 7, 8, 9, 10, 11]);
  });

  it("summarizes distinct business orders and each selected-group outcome without counting skipped work as processed", () => {
    const messages = [
      { ...message(0, { status: "committed", applicationStatus: "succeeded" }), key: "order-A" },
      { ...message(1, { status: "read", applicationStatus: "pending" }), key: "order-A" },
      { ...message(2, { status: "read", applicationStatus: "failed" }), key: "order-B" },
      { ...message(3, { applicationStatus: "succeeded" }), key: "order-B" },
      { ...message(4), key: "customer-X" },
      { ...message(5, { status: "lost", applicationStatus: "pending" }), key: null },
    ];
    expect(projectPartitionSummary(messages, 1)).toEqual({ orders: 2, records: 6, unread: 1, inFlight: 1, skipped: 1, read: 5, processed: 2, failed: 1, committed: 1 });
    expect(projectPartitionSummary(messages, 1, { current: 0, logEndOffset: 6 }).unread).toBe(6);
    expect(projectPartitionSummary(messages, 1, { current: 4, logEndOffset: 6 }).unread).toBe(2);
  });

  it("keeps the reference topology compact with separate real producer positions", () => {
    const positions = compactFlowPositions(["checkout-api", "payment-service", "shipping-service"], [0, 1, 2, 3], ["worker-0", "worker-1", "worker-2", "worker-3"]);
    expect(Object.keys(positions)).toHaveLength(13);
    expect(positions["producer-checkout-api"].x).toBe(0);
    expect(positions["producer-payment-service"].y - positions["producer-checkout-api"].y).toBe(144);
    expect(positions["partition-3"]).toEqual({ x: 500, y: 498 });
    expect(positions["consumer-worker-3"]).toEqual({ x: 850, y: 503 });
    expect(positions.commit.x + 180).toBeLessThanOrEqual(1330);
    expect(positions.producer).toBeUndefined();
    expect(compactFlowPositions(undefined, [0], ["worker-0"]).producer).toBeDefined();
  });
});
