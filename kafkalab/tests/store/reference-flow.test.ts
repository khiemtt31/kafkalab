import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { persistKafkaWorkspace, useKafkaStore } from "@/store/kafka-store";

describe("technical chart reference workload and retry adapter", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    localStorage.clear();
    useKafkaStore.getState().actions.reset();
  });

  afterEach(() => {
    useKafkaStore.getState().actions.reset();
    localStorage.clear();
    vi.useRealTimers();
  });

  it("opens a fresh store with the paused reference flow", async () => {
    vi.resetModules();
    const { useKafkaStore: freshStore } = await import("@/store/kafka-store");
    const snapshot = freshStore.getState().snapshot;
    expect(snapshot.messageCount).toBe(24);
    expect(snapshot.running).toBe(false);
    expect(snapshot.producerRunning).toBe(false);
    expect(snapshot.consumers).toHaveLength(4);
    expect(snapshot.producers).toEqual([
      { id: "checkout-api", producedCount: 8 },
      { id: "payment-service", producedCount: 8 },
      { id: "shipping-service", producedCount: 8 },
    ]);
    expect(snapshot.consumers.every((consumer) => consumer.autoCommit === false)).toBe(true);
    expect(snapshot.cluster).toMatchObject({ replicationFactor: 3, minInSyncReplicas: 2, requiredAcks: "all" });
    expect(vi.getTimerCount()).toBe(0);
    freshStore.getState().actions.reset();
  });

  it("loads the exact ordered rows and selected-group delivery states while keeping reset empty", () => {
    const actions = useKafkaStore.getState().actions;
    const revision = useKafkaStore.getState().snapshot.resetRevision ?? 0;
    actions.loadReferenceFlow?.();
    const snapshot = useKafkaStore.getState().snapshot;
    expect(snapshot.topicName).toBe("orders");
    expect(snapshot.partitions.map((partition) => partition.messages.map((message) => [message.offset, message.key, message.value]))).toEqual([
      [
        [0, "order-7F3K9", "order-created"], [1, "order-N8R4C", "order-created"],
        [2, "order-7F3K9", "order-paid"], [3, "order-N8R4C", "order-paid"],
        [4, "order-7F3K9", "order-shipped"], [5, "order-N8R4C", "order-shipped"],
      ],
      [
        [0, "order-Q2M81", "order-created"], [1, "order-A4D92", "order-created"],
        [2, "order-Q2M81", "order-paid"], [3, "order-A4D92", "order-paid"],
        [4, "order-Q2M81", "order-shipped"], [5, "order-A4D92", "order-shipped"],
      ],
      [
        [0, "order-R8H27", "order-created"], [1, "order-B6T90", "order-created"],
        [2, "order-R8H27", "order-paid"], [3, "order-B6T90", "order-paid"],
        [4, "order-R8H27", "order-shipped"], [5, "order-B6T90", "order-shipped"],
      ],
      [
        [0, "order-V5X14", "order-created"], [1, "order-C9L36", "order-created"],
        [2, "order-V5X14", "order-paid"], [3, "order-C9L36", "order-paid"],
        [4, "order-V5X14", "order-shipped"], [5, "order-C9L36", "order-shipped"],
      ],
    ]);
    expect(snapshot.partitions.map((partition) => [partition.logEndOffset, partition.currentOffset, partition.committedOffset, partition.lag, partition.assignedConsumerId])).toEqual([
      [6, 2, 1, 5, "consumer-0"], [6, 1, 0, 6, "consumer-1"],
      [6, 1, 1, 5, "consumer-2"], [6, 1, 0, 6, "consumer-3"],
    ]);
    expect(snapshot.partitions[0].messages[0].delivery).toMatchObject({ status: "committed", applicationStatus: "succeeded", attempt: 1 });
    expect(snapshot.partitions[0].messages[1].delivery).toMatchObject({ status: "processed", applicationStatus: "succeeded", attempt: 1 });
    expect(snapshot.partitions[1].messages[0].delivery).toMatchObject({ status: "failed", applicationStatus: "failed", attempt: 1, failureReason: expect.stringMatching(/timed out/) });
    expect(snapshot.partitions[1].messages[1].delivery).toBeUndefined();
    expect(snapshot.partitions[2].messages[0].delivery).toMatchObject({ status: "committed", applicationStatus: "succeeded", attempt: 1 });
    expect(snapshot.partitions[3].messages[0].delivery).toMatchObject({ status: "read", applicationStatus: "pending", attempt: 1 });
    expect(snapshot.partitions.flatMap((partition) => partition.messages).filter((message) => message.delivery !== undefined)).toHaveLength(5);
    expect(snapshot.consumers.map((consumer) => consumer.assignedPartitions)).toEqual([[0], [1], [2], [3]]);
    expect(snapshot.partitions.every((partition) => partition.messages.map((message) => message.producerId).join(",") === "checkout-api,checkout-api,payment-service,payment-service,shipping-service,shipping-service")).toBe(true);
    expect(snapshot.cluster?.brokers).toHaveLength(3);
    expect(snapshot.cluster?.assignments.every((assignment) => assignment.replicas.length === 3 && assignment.inSyncReplicas.length === 3)).toBe(true);
    expect(snapshot.totalLag).toBe(22);
    const producedEvents = snapshot.events.filter((event) => event.type === "message-produced");
    expect(producedEvents).toHaveLength(24);
    expect(producedEvents.map((event) => event.metadata?.producerId)).toEqual([
      ...Array(8).fill("checkout-api"), ...Array(8).fill("payment-service"), ...Array(8).fill("shipping-service"),
    ]);
    expect(snapshot.resetRevision).toBe(revision + 1);
    expect(snapshot.running).toBe(false);
    expect(snapshot.producerRunning).toBe(false);

    actions.reset();
    expect(useKafkaStore.getState().snapshot.messageCount).toBe(0);
    expect(useKafkaStore.getState().snapshot.cluster).toBeUndefined();
    expect(useKafkaStore.getState().snapshot.consumers).toHaveLength(1);
    expect(useKafkaStore.getState().snapshot.producers).toEqual([{ id: "producer-1", producedCount: 0 }]);
  });

  it.each([false, true])("processes retry attempt 2 and commits only when auto-commit is %s", (autoCommit) => {
    const actions = useKafkaStore.getState().actions;
    actions.loadReferenceFlow?.();
    const before = useKafkaStore.getState().snapshot.partitions[1].messages[0];
    actions.setConsumerAutoCommit?.("consumer-1", autoCommit);
    actions.retryRecordProcessing?.({ partition: 1, offset: 0 });
    const snapshot = useKafkaStore.getState().snapshot;
    const retried = snapshot.partitions[1].messages[0];
    expect(retried.id).toBe(before.id);
    expect(retried.delivery).toMatchObject({ messageId: before.id, attempt: 2, applicationStatus: "succeeded", status: autoCommit ? "committed" : "processed" });
    expect(retried.delivery?.failureReason).toBeUndefined();
    expect(snapshot.messageCount).toBe(24);
    expect(snapshot.partitions[1].committedOffset).toBe(autoCommit ? 1 : 0);
    expect(snapshot.partitions[0].committedOffset).toBe(1);
    expect(snapshot.events.some((event) => event.type === "record-processing-retried" && event.entityId === before.id)).toBe(true);
    expect(useKafkaStore.getState().simulation.deliveries.find((delivery) => delivery.id === before.delivery?.id)?.applicationStatus).toBe("failed");
    const eventCount = snapshot.events.length;
    actions.retryRecordProcessing?.({ partition: 1, offset: 0 });
    expect(useKafkaStore.getState().snapshot.events).toHaveLength(eventCount);
  });

  it("projects outcomes for the selected group rather than a global record status", () => {
    const actions = useKafkaStore.getState().actions;
    actions.loadReferenceFlow?.();
    actions.addConsumer("payments");
    actions.selectGroup?.("payments");
    expect(useKafkaStore.getState().snapshot.partitions.flatMap((partition) => partition.messages).every((message) => message.delivery === undefined)).toBe(true);
    actions.selectGroup?.("analytics-group");
    expect(useKafkaStore.getState().snapshot.partitions[1].messages[0].delivery?.applicationStatus).toBe("failed");
  });

  it("ignores invalid, committed and ownerless retry requests", () => {
    const actions = useKafkaStore.getState().actions;
    actions.loadReferenceFlow?.();
    for (const target of [{ partition: 0, offset: 0 }, { partition: 1, offset: 1 }, { partition: 9, offset: 0 }, { partition: 1, offset: -1 }]) {
      const before = useKafkaStore.getState().snapshot;
      actions.retryRecordProcessing?.(target);
      expect(useKafkaStore.getState().snapshot).toBe(before);
    }
    for (const consumer of useKafkaStore.getState().snapshot.consumers) actions.removeConsumer(consumer.id);
    const before = useKafkaStore.getState().snapshot;
    actions.retryRecordProcessing?.({ partition: 1, offset: 0 });
    expect(useKafkaStore.getState().snapshot).toBe(before);
  });

  it("hydrates saved user work instead of replacing it with the fresh reference demo", async () => {
    const actions = useKafkaStore.getState().actions;
    actions.configureTopology?.({ topicName: "saved-payments", partitionCount: 2 });
    actions.produce({ partition: 1, key: "my-order", value: "keep-this" });
    useKafkaStore.setState({ hydrated: true });
    expect(persistKafkaWorkspace()).toBeUndefined();

    vi.resetModules();
    const { useKafkaStore: freshStore } = await import("@/store/kafka-store");
    expect(freshStore.getState().snapshot.messageCount).toBe(24);
    freshStore.getState().actions.hydrate?.();
    const snapshot = freshStore.getState().snapshot;
    expect(snapshot.topicName).toBe("saved-payments");
    expect(snapshot.partitions).toHaveLength(2);
    expect(snapshot.messageCount).toBe(1);
    expect(snapshot.partitions[1].messages[0]).toMatchObject({ key: "my-order", value: "keep-this" });
    expect(snapshot.cluster).toBeUndefined();
    expect(snapshot.running).toBe(false);
    freshStore.getState().actions.reset();
  });

  it("retains reference outcomes and a working retry after over 200 newer deliveries and reload", () => {
    const actions = useKafkaStore.getState().actions;
    actions.loadReferenceFlow?.();
    for (let index = 0; index < 211; index += 1) actions.produce({ partition: 2, key: null, value: `later-${index}` });
    for (const consumer of useKafkaStore.getState().snapshot.consumers) {
      actions.setConsumerRate?.(consumer.id, consumer.id === "consumer-2" ? 100 : 0);
    }
    actions.startReading?.();
    vi.advanceTimersByTime(3000);
    actions.pause?.();
    expect(useKafkaStore.getState().simulation.deliveries.length).toBeGreaterThan(200);
    useKafkaStore.setState({ hydrated: true });
    expect(persistKafkaWorkspace()).toBeUndefined();
    actions.reset();
    actions.hydrate?.();

    const restored = useKafkaStore.getState().snapshot;
    expect(restored.partitions[0].messages[0].delivery).toMatchObject({ status: "committed", applicationStatus: "succeeded" });
    expect(restored.partitions[0].messages[1].delivery).toMatchObject({ status: "processed", applicationStatus: "succeeded" });
    expect(restored.partitions[1].messages[0].delivery).toMatchObject({ status: "failed", applicationStatus: "failed", attempt: 1 });
    expect(restored.events.some((event) => event.type === "message-produced" && event.entityId === restored.partitions[0].messages[0].id)).toBe(false);
    expect(restored.partitions[0].messages[0].producerId).toBe("checkout-api");
    expect(restored.partitions[0].messages[2].producerId).toBe("payment-service");
    expect(restored.partitions[0].messages[4].producerId).toBe("shipping-service");
    actions.retryRecordProcessing?.({ partition: 1, offset: 0 });
    expect(useKafkaStore.getState().snapshot.partitions[1].messages[0].delivery).toMatchObject({ applicationStatus: "succeeded", attempt: 2 });
    expect(useKafkaStore.getState().snapshot.partitions[1].committedOffset).toBe(0);
  });
});
