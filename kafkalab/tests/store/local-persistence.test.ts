import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { persistKafkaWorkspace, useKafkaStore } from "@/store/kafka-store";

const key = "kafkalab:workspace:v1";

describe("versioned local workspace persistence", () => {
  beforeEach(() => {
    localStorage.clear();
    useKafkaStore.setState({ hydrated: true, storageNotice: undefined });
    useKafkaStore.getState().actions.reset();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    useKafkaStore.getState().actions.pause?.();
    useKafkaStore.getState().actions.reset();
    localStorage.clear();
  });

  it("restores simulation, flow layout, and quiz progress paused after reload", () => {
    const actions = useKafkaStore.getState().actions;
    actions.produce({ key: "order-7F3K9", value: "order-created" });
    actions.setFlowPositions?.({ producer: { x: 42, y: 70 } });
    actions.setFlowViewport?.({ x: 8, y: -4, zoom: 0.9 });
    actions.selectPracticeQuestion?.("hard", 17);
    actions.answerPracticeQuestion?.("hard", "kafka-hard-18", "c");
    actions.start?.();
    expect(persistKafkaWorkspace()).toBeUndefined();
    expect(JSON.parse(localStorage.getItem(key) ?? "{}").version).toBe(1);

    actions.reset();
    actions.resetPracticeCategory?.("hard");
    actions.hydrate?.();

    const restored = useKafkaStore.getState();
    expect(restored.snapshot.messageCount).toBe(1);
    expect(restored.snapshot.partitions.flatMap((partition) => partition.messages)[0]).toMatchObject({ key: "order-7F3K9", value: "order-created" });
    expect(restored.snapshot.running).toBe(false);
    expect(restored.snapshot.producerRunning).toBe(false);
    expect(restored.flowLayout.positions.producer).toEqual({ x: 42, y: 70 });
    expect(restored.flowLayout.viewport).toEqual({ x: 8, y: -4, zoom: 0.9 });
    expect(restored.practiceProgress.currentQuestion.hard).toBe(17);
    expect(restored.practiceProgress.answers.hard?.["kafka-hard-18"]).toBe("c");
  });

  it("persists chart positions and viewport independently for each variant", () => {
    const actions = useKafkaStore.getState().actions;
    actions.setFlowPositions?.({ topic: { x: 20, y: 30 } });
    actions.setFlowViewport?.({ x: 1, y: 2, zoom: 0.9 });
    actions.setFlowPositions?.({ topic: { x: 220, y: 330 } }, "partitions");
    actions.setFlowViewport?.({ x: -4, y: 5, zoom: 0.7 }, "partitions");
    expect(persistKafkaWorkspace()).toBeUndefined();

    useKafkaStore.getState().actions.reset();
    useKafkaStore.getState().actions.hydrate?.();
    const restored = useKafkaStore.getState();
    expect(restored.flowLayout.positions.topic).toEqual({ x: 20, y: 30 });
    expect(restored.flowLayout.views?.partitions?.positions.topic).toEqual({ x: 220, y: 330 });
    expect(restored.flowLayout.views?.partitions?.viewport).toEqual({ x: -4, y: 5, zoom: 0.7 });
    expect(restored.flowLayout.views?.flow?.viewport).toEqual({ x: 1, y: 2, zoom: 0.9 });
  });

  it("reports malformed browser state and leaves the deterministic current app usable", () => {
    localStorage.setItem(key, "{not json");
    useKafkaStore.getState().actions.hydrate?.();
    expect(useKafkaStore.getState().hydrated).toBe(true);
    expect(useKafkaStore.getState().storageNotice).toMatch(/could not be read/i);
    expect(useKafkaStore.getState().snapshot.topicName).toBe("orders");
  });

  it("rejects parseable snapshots whose nested partition shape is damaged", () => {
    localStorage.setItem(key, JSON.stringify({ version: 1, simulation: { topic: { name: "orders", nextUnkeyedPartition: 0, partitions: [{ id: 0, logEndOffset: 1 }] }, groups: [], events: [], metrics: [] } }));
    useKafkaStore.getState().actions.hydrate?.();
    expect(useKafkaStore.getState().hydrated).toBe(true);
    expect(useKafkaStore.getState().storageNotice).toMatch(/invalid|unsupported/i);
    expect(useKafkaStore.getState().snapshot.topicName).toBe("orders");
  });

  it("rejects a malformed saved broker cluster without crashing the workspace", () => {
    const actions = useKafkaStore.getState().actions;
    actions.createCluster?.();
    expect(persistKafkaWorkspace()).toBeUndefined();
    const saved = JSON.parse(localStorage.getItem(key) ?? "{}");
    saved.simulation.cluster = {};
    localStorage.setItem(key, JSON.stringify(saved));
    actions.reset();
    actions.hydrate?.();
    expect(useKafkaStore.getState().hydrated).toBe(true);
    expect(useKafkaStore.getState().storageNotice).toMatch(/invalid|unsupported/i);
    expect(useKafkaStore.getState().snapshot.cluster).toBeUndefined();
  });

  it("survives browser storage quota errors with an actionable notice", () => {
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => { throw new DOMException("Quota exceeded", "QuotaExceededError"); });
    expect(persistKafkaWorkspace()).toMatch(/storage is full/i);
    expect(useKafkaStore.getState().storageNotice).toMatch(/reset old records|clear saved data/i);
    expect(useKafkaStore.getState().snapshot.topicName).toBe("orders");
  });

  it("persists failed application outcomes, reasons, attempts and min ISR", () => {
    const actions = useKafkaStore.getState().actions;
    actions.loadReferenceFlow?.();
    const failed = useKafkaStore.getState().snapshot.partitions[1].messages[0];
    expect(persistKafkaWorkspace()).toBeUndefined();
    actions.reset();
    actions.hydrate?.();
    const snapshot = useKafkaStore.getState().snapshot;
    expect(snapshot.cluster?.minInSyncReplicas).toBe(2);
    expect(snapshot.partitions[1].messages[0].delivery).toEqual(failed.delivery);
    expect(snapshot.running).toBe(false);
    actions.retryRecordProcessing?.({ partition: 1, offset: 0 });
    expect(useKafkaStore.getState().snapshot.partitions[1].messages[0].delivery).toMatchObject({ messageId: failed.id, attempt: 2, applicationStatus: "succeeded", status: "processed" });
  });

  it("restores real producer identities and cumulative counts in the existing storage version", () => {
    const actions = useKafkaStore.getState().actions;
    actions.loadReferenceFlow?.();
    actions.addProducer?.("returns-service");
    actions.produceBatch?.({ producerId: "returns-service", count: 2, valuePrefix: "return" });
    const producers = useKafkaStore.getState().snapshot.producers;
    expect(persistKafkaWorkspace()).toBeUndefined();
    expect(JSON.parse(localStorage.getItem(key) ?? "{}").version).toBe(1);
    actions.reset();
    actions.hydrate?.();
    expect(useKafkaStore.getState().snapshot.producers).toEqual(producers);
    expect(useKafkaStore.getState().snapshot.partitions.flatMap((partition) => partition.messages).filter((message) => message.value.startsWith("return-")).map((message) => message.producerId)).toEqual(["returns-service", "returns-service"]);
    expect(useKafkaStore.getState().snapshot.messageCount).toBe(26);
    expect(useKafkaStore.getState().snapshot.running).toBe(false);
  });

  it("restores older saved records without inventing their missing producer provenance", () => {
    const actions = useKafkaStore.getState().actions;
    actions.produce({ key: "legacy-order", value: "keep-legacy-record" });
    expect(persistKafkaWorkspace()).toBeUndefined();
    const saved = JSON.parse(localStorage.getItem(key) ?? "{}");
    for (const partition of saved.simulation.topic.partitions) {
      for (const message of partition.messages) delete message.producerId;
    }
    localStorage.setItem(key, JSON.stringify(saved));
    actions.reset();
    actions.hydrate?.();
    const snapshot = useKafkaStore.getState().snapshot;
    expect(useKafkaStore.getState().storageNotice).toBeUndefined();
    const restored = snapshot.partitions.flatMap((partition) => partition.messages);
    expect(restored).toHaveLength(1);
    expect(restored[0]).toMatchObject({ key: "legacy-order", value: "keep-legacy-record" });
    expect(restored[0].producerId).toBeUndefined();
    expect(snapshot.producers).toEqual([{ id: "producer-1", producedCount: 1 }]);
  });

  it("migrates legacy min ISR and infers application success from processing time, never from a commit", () => {
    const actions = useKafkaStore.getState().actions;
    actions.loadReferenceFlow?.();
    expect(persistKafkaWorkspace()).toBeUndefined();
    const saved = JSON.parse(localStorage.getItem(key) ?? "{}");
    delete saved.simulation.cluster.minInSyncReplicas;
    for (const delivery of saved.simulation.deliveries) delete delivery.applicationStatus;
    const unprocessedCommitted = saved.simulation.deliveries[1];
    unprocessedCommitted.status = "committed";
    unprocessedCommitted.committedAt = saved.simulation.clock;
    delete unprocessedCommitted.processedAt;
    const lost = saved.simulation.deliveries[2];
    lost.status = "lost";
    delete lost.failureReason;
    saved.simulation.groups[0].committedOffsets[0] = 2;
    saved.simulation.groups[0].committedOffsets[1] = 1;
    localStorage.setItem(key, JSON.stringify(saved));
    actions.reset();
    actions.hydrate?.();

    const restored = useKafkaStore.getState();
    expect(restored.storageNotice).toBeUndefined();
    expect(restored.snapshot.cluster?.minInSyncReplicas).toBe(1);
    expect(restored.snapshot.partitions[0].messages[0].delivery?.applicationStatus).toBe("succeeded");
    expect(restored.snapshot.partitions[0].messages[1].delivery).toMatchObject({ status: "committed", applicationStatus: "pending" });
    expect(restored.snapshot.partitions[1].messages[0].delivery).toMatchObject({ status: "lost", applicationStatus: "pending" });
  });

  it.each(["bad-application-status", "missing-failure-reason", "bad-min-isr", "bad-producer-id"])("rejects malformed new persisted fields: %s", (damage) => {
    const actions = useKafkaStore.getState().actions;
    actions.loadReferenceFlow?.();
    expect(persistKafkaWorkspace()).toBeUndefined();
    const saved = JSON.parse(localStorage.getItem(key) ?? "{}");
    if (damage === "bad-application-status") saved.simulation.deliveries[0].applicationStatus = "success-maybe";
    else if (damage === "missing-failure-reason") delete saved.simulation.deliveries[2].failureReason;
    else if (damage === "bad-min-isr") saved.simulation.cluster.minInSyncReplicas = 0;
    else saved.simulation.topic.partitions[0].messages[0].producerId = 42;
    localStorage.setItem(key, JSON.stringify(saved));
    actions.reset();
    actions.hydrate?.();
    expect(useKafkaStore.getState().storageNotice).toMatch(/invalid|unsupported/i);
    expect(useKafkaStore.getState().snapshot.messageCount).toBe(0);
  });

  it("keeps Practice progress when simulator reset clears the chart and graph layout", () => {
    const actions = useKafkaStore.getState().actions;
    actions.produce({ key: null, value: "temporary" });
    actions.setFlowPositions?.({ topic: { x: 500, y: 100 } });
    actions.answerPracticeQuestion?.("easy", "kafka-easy-01", "a");

    actions.reset();

    expect(useKafkaStore.getState().snapshot.messageCount).toBe(0);
    expect(useKafkaStore.getState().snapshot.metrics).toHaveLength(0);
    expect(useKafkaStore.getState().flowLayout.positions).toEqual({});
    expect(useKafkaStore.getState().practiceProgress.answers.easy?.["kafka-easy-01"]).toBe("a");
  });
});
