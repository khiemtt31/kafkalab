import { describe, expect, it } from "vitest";
import {
  addConsumer,
  assertInvariants,
  beginTransaction,
  commitCurrentOffsets,
  commitProcessedOffsets,
  commitTransaction,
  configureBatching,
  configureAdvancedPolicies,
  checkHeartbeats,
  consumeOne,
  createBrokerCluster,
  createSimulationState,
  failBroker,
  findInvariantViolations,
  getGroupLag,
  getIdleConsumers,
  getPartitionLag,
  loadScenario,
  pollConsumer,
  processRecords,
  produceBatch,
  produceMessage,
  recordHeartbeat,
  rebalanceGroup,
  restartConsumer,
  crashConsumer,
  registerSchema,
  runTicks,
  advanceClock,
  selectPartition,
} from "@/core/kafka";

describe("Kafka simulation domain", () => {
  it("allocates partition-local offsets starting at zero", () => {
    let state = createSimulationState({ partitionCount: 2 });
    const first = produceMessage(state, { partition: 0, value: "a" });
    state = first.state;
    const second = produceMessage(state, { partition: 0, value: "b" });
    state = second.state;
    const otherPartition = produceMessage(state, { partition: 1, value: "c" });

    expect(first.value.offset).toBe(0);
    expect(first.value.producerId).toBe("producer-1");
    expect(second.value.offset).toBe(1);
    expect(otherPartition.value.offset).toBe(0);
    expect(otherPartition.state.topic.partitions[0].logEndOffset).toBe(2);
    expect(otherPartition.state.topic.partitions[1].logEndOffset).toBe(1);
  });

  it("retains local producer provenance after the bounded event history expires", () => {
    const state = createSimulationState({ producerIds: ["checkout-api"], config: { maxEvents: 2 } });
    const produced = produceBatch(state, [
      { producerId: "checkout-api", partition: 0, key: "order-7F3K9", value: "order-created" },
      { producerId: "checkout-api", partition: 0, key: "order-N8R4C", value: "order-created" },
      { producerId: "checkout-api", partition: 0, key: "order-Q2M81", value: "order-created" },
    ]);
    expect(produced.state.events.some((event) => event.entityId === produced.value[0].id)).toBe(false);
    expect(produced.state.topic.partitions[0].messages[0]).toMatchObject({ producerId: "checkout-api", key: "order-7F3K9", offset: 0 });
    expect(produced.state.producers).toEqual([{ id: "checkout-api", producedCount: 3 }]);
    assertInvariants(produced.state);
  });

  it("routes the same key deterministically and rotates unkeyed records", () => {
    const state = createSimulationState({ partitionCount: 3 });
    expect(selectPartition(state.topic, "same-key").partition).toBe(selectPartition(state.topic, "same-key").partition);
    const first = selectPartition(state.topic, null);
    const nextTopic = { ...state.topic, nextUnkeyedPartition: first.nextUnkeyedPartition };
    expect(selectPartition(nextTopic, null).partition).not.toBe(first.partition);
  });

  it("assigns one owner per partition and shows idle consumers", () => {
    let state = createSimulationState({ partitionCount: 3 });
    state = addConsumer(state, "analytics", "consumer-a").state;
    state = addConsumer(state, "analytics", "consumer-b").state;
    state = addConsumer(state, "analytics", "consumer-c").state;
    state = addConsumer(state, "analytics", "consumer-d").state;
    const group = state.groups.find((candidate) => candidate.id === "analytics");
    expect(Object.keys(group?.assignments ?? {})).toHaveLength(3);
    expect(getIdleConsumers(state, "analytics")).toHaveLength(1);
    assertInvariants(state);
  });

  it("keeps consuming, processing, and committing separate", () => {
    let state = createSimulationState({ partitionCount: 1 });
    state = produceBatch(state, [{ value: "a" }, { value: "b" }]).state;
    state = addConsumer(state, "analytics", "consumer-a").state;
    const polled = pollConsumer(state, "analytics", "consumer-a");
    expect(polled.value[0].offset).toBe(0);
    expect(polled.state.groups[0].committedOffsets[0] ?? 0).toBe(0);
    const processed = processRecords(polled.state, "analytics", "consumer-a");
    expect(processed.state.groups[0].committedOffsets[0] ?? 0).toBe(0);
    const committed = commitProcessedOffsets(processed.state, "analytics", "consumer-a");
    expect(committed.state.groups[0].committedOffsets[0]).toBe(1);
    expect(getPartitionLag(committed.state, "analytics", 0)).toBe(1);
  });

  it("identifies the exact record in poll metadata for each partition and group", () => {
    let state = createSimulationState({ partitionCount: 2 });
    state = produceMessage(state, { partition: 0, value: "partition-zero" }).state;
    state = produceMessage(state, { partition: 1, value: "partition-one" }).state;
    state = addConsumer(state, "analytics", "analytics-1").state;
    state = addConsumer(state, "payments", "payments-1").state;

    for (const [groupId, consumerId] of [["analytics", "analytics-1"], ["payments", "payments-1"]]) {
      const polled = pollConsumer(state, groupId, consumerId, { maxRecords: 2 });
      const pollEvents = polled.events.filter((event) => event.type === "consumer-polled");
      expect(polled.value).toHaveLength(2);
      expect(pollEvents).toHaveLength(2);
      for (const [index, message] of polled.value.entries()) {
        expect(pollEvents[index].entityId).toBe(consumerId);
        expect(pollEvents[index].details).toEqual({
          groupId,
          consumerId,
          messageId: message.id,
          partition: message.partition,
          offset: message.offset,
          attempt: 1,
        });
      }
      expect(pollEvents[0].details?.messageId).not.toBe(pollEvents[1].details?.messageId);
      expect(polled.value.map((message) => message.offset)).toEqual([0, 0]);
      expect(polled.state.groups.find((group) => group.id === groupId)?.committedOffsets).toEqual({ 0: 0, 1: 0 });
      assertInvariants(polled.state);
      state = polled.state;
    }
  });

  it.each(["processed", "current"] as const)("describes every advanced commit range in %s mode", (mode) => {
    let state = createSimulationState({ partitionCount: 2 });
    state = addConsumer(state, "analytics", "consumer-a").state;
    for (const [partition, count] of [[0, 3], [1, 2]]) {
      for (let offset = 0; offset < count; offset += 1) {
        state = produceMessage(state, { partition, value: `p${partition}-${offset}` }).state;
      }
    }
    const commit = mode === "processed" ? commitProcessedOffsets : commitCurrentOffsets;
    state = pollConsumer(state, "analytics", "consumer-a", { partition: 0 }).state;
    state = processRecords(state, "analytics", "consumer-a").state;
    state = commit(state, "analytics", "consumer-a", [0]).state;

    for (const partition of [0, 0, 1, 1]) {
      state = pollConsumer(state, "analytics", "consumer-a", { partition }).state;
      state = processRecords(state, "analytics", "consumer-a").state;
    }
    const committed = commit(state, "analytics", "consumer-a");
    const commitEvents = committed.events.filter((event) => event.type === "offset-committed");
    expect(commitEvents.map((event) => event.details)).toEqual([
      { groupId: "analytics", consumerId: "consumer-a", partition: 0, previous: 1, committedOffset: 3, mode },
      { groupId: "analytics", consumerId: "consumer-a", partition: 1, previous: 0, committedOffset: 2, mode },
    ]);
    expect(committed.value).toEqual({ 0: 3, 1: 2 });
    expect(commitEvents.map((event) => event.entityId)).toEqual(["consumer-a", "consumer-a"]);
    expect(getGroupLag(committed.state, "analytics").total).toBe(0);
    expect(commit(committed.state, "analytics", "consumer-a").events).toEqual([]);
    assertInvariants(committed.state);
  });

  it("keeps committed offsets independent between groups", () => {
    let state = createSimulationState({ partitionCount: 1 });
    state = produceMessage(state, { value: "a" }).state;
    state = addConsumer(state, "analytics", "analytics-1").state;
    state = addConsumer(state, "payments", "payments-1").state;
    state = consumeOne(state, "analytics", "analytics-1").state;
    expect(state.groups.find((group) => group.id === "analytics")?.committedOffsets[0]).toBe(1);
    expect(state.groups.find((group) => group.id === "payments")?.committedOffsets[0] ?? 0).toBe(0);
  });

  it("replays a processed record when the consumer crashes before commit", () => {
    let state = createSimulationState({ partitionCount: 1 });
    state = produceMessage(state, { value: "retry-me" }).state;
    state = addConsumer(state, "analytics", "consumer-a").state;
    state = processRecords(pollConsumer(state, "analytics", "consumer-a").state, "analytics", "consumer-a").state;
    state = crashConsumer(state, "analytics", "consumer-a").state;
    state = restartConsumer(state, "analytics", "consumer-a").state;
    const replay = pollConsumer(state, "analytics", "consumer-a");
    expect(replay.value[0].offset).toBe(0);
    expect(replay.events.some((event) => event.type === "duplicate-delivery")).toBe(true);
    expect(replay.events.find((event) => event.type === "consumer-polled")?.details).toEqual({
      groupId: "analytics",
      consumerId: "consumer-a",
      messageId: replay.value[0].id,
      partition: 0,
      offset: 0,
      attempt: 2,
    });
  });

  it("can demonstrate loss when current position is committed before processing", () => {
    let state = createSimulationState({ partitionCount: 1 });
    state = produceMessage(state, { value: "may-be-lost" }).state;
    state = addConsumer(state, "analytics", "consumer-a").state;
    state = pollConsumer(state, "analytics", "consumer-a").state;
    const committed = commitCurrentOffsets(state, "analytics", "consumer-a");
    expect(committed.state.groups[0].committedOffsets[0]).toBe(1);
    expect(committed.state.deliveries.some((delivery) => delivery.status === "lost")).toBe(true);
    expect(committed.events.some((event) => event.type === "lost-delivery")).toBe(true);
    const repeated = commitCurrentOffsets(committed.state, "analytics", "consumer-a");
    expect(repeated.events.some((event) => event.type === "lost-delivery")).toBe(false);
    expect(repeated.state.events.filter((event) => event.type === "lost-delivery")).toHaveLength(1);
  });

  it("does not label processed work as lost when its current position is committed", () => {
    let state = createSimulationState({ partitionCount: 1 });
    state = produceMessage(state, { value: "already-processed" }).state;
    state = addConsumer(state, "analytics", "consumer-a").state;
    state = pollConsumer(state, "analytics", "consumer-a").state;
    state = processRecords(state, "analytics", "consumer-a").state;
    const committed = commitCurrentOffsets(state, "analytics", "consumer-a");

    expect(committed.state.groups[0].committedOffsets[0]).toBe(1);
    expect(committed.state.deliveries[0].status).toBe("committed");
    expect(committed.state.deliveries[0].processedAt).toBe(0);
    expect(committed.state.deliveries.some((delivery) => delivery.status === "lost")).toBe(false);
    expect(committed.events.some((event) => event.type === "lost-delivery")).toBe(false);
    assertInvariants(committed.state);
  });

  it("marks only unprocessed deliveries as lost in a mixed current-position commit", () => {
    let state = createSimulationState({ partitionCount: 1 });
    state = addConsumer(state, "analytics", "consumer-a").state;
    state = produceBatch(state, [{ value: "processed" }, { value: "unprocessed" }]).state;
    state = pollConsumer(state, "analytics", "consumer-a").state;
    state = processRecords(state, "analytics", "consumer-a").state;
    state = pollConsumer(state, "analytics", "consumer-a").state;
    const committed = commitCurrentOffsets(state, "analytics", "consumer-a");

    expect(committed.state.groups[0].committedOffsets[0]).toBe(2);
    expect(committed.state.deliveries.map((delivery) => delivery.status)).toEqual(["committed", "lost"]);
    expect(committed.events.filter((event) => event.type === "lost-delivery").map((event) => event.details?.offset)).toEqual([1]);
  });

  it("generates consumer IDs that skip explicit names in every group", () => {
    let state = createSimulationState({ partitionCount: 2 });
    state = addConsumer(state, "analytics", "consumer-0").state;
    state = addConsumer(state, "payments", "consumer-1").state;
    const generated = addConsumer(state, "analytics");

    expect(generated.value.id).toBe("consumer-2");
    const consumerIds = generated.state.groups.flatMap((group) => group.consumers.map((consumer) => consumer.id));
    expect(new Set(consumerIds).size).toBe(consumerIds.length);
    assertInvariants(generated.state);
  });

  it("records lag and deterministic tick metrics", () => {
    let state = createSimulationState({ partitionCount: 1 });
    state = addConsumer(state, "analytics", "consumer-a").state;
    const ticked = runTicks(state, 3, { elapsedMs: 1000, producerRate: 4, consumerRate: 1 });
    expect(ticked.value).toHaveLength(3);
    expect(ticked.state.metrics).toHaveLength(3);
    expect(getGroupLag(ticked.state, "analytics").total).toBeGreaterThan(0);
  });

  it("supports broker failure and educational leader election", () => {
    let state = createSimulationState({ partitionCount: 1 });
    state = createBrokerCluster(state, 3, 2).state;
    const assignment = state.cluster?.assignments[0];
    expect(assignment).toBeDefined();
    const failed = failBroker(state, assignment?.leader ?? "broker-1");
    expect(failed.state.events.some((event) => event.type === "leader-elected")).toBe(true);
  });

  it("exposes phase-three educational state without a backend", () => {
    let state = createSimulationState({ partitionCount: 1 });
    state = beginTransaction(state, "producer-1", "tx-1").state;
    state = commitTransaction(state, "tx-1").state;
    state = addConsumer(state, "analytics", "consumer-a").state;
    state = recordHeartbeat(state, "analytics", "consumer-a").state;
    state = configureBatching(state, { batchSize: 10, compression: "lz4" }).state;
    state = registerSchema(state, { subject: "orders-value", fields: ["id", "total"], compatibility: "backward" }).state;
    expect(state.advanced.transactions[0].status).toBe("committed");
    expect(state.advanced.heartbeats).toHaveLength(1);
    expect(state.advanced.batchSettings.compression).toBe("lz4");
    expect(state.advanced.schemas[0].version).toBe(1);
  });

  it("times out consumers through session and poll policies", () => {
    let state = createSimulationState({ partitionCount: 1 });
    state = addConsumer(state, "analytics", "consumer-a").state;
    state = configureAdvancedPolicies(state, { sessionTimeoutMs: 1000, maxPollIntervalMs: 1000 }).state;
    state = recordHeartbeat(state, "analytics", "consumer-a").state;
    state = advanceClock(state, 1500);
    const checked = checkHeartbeats(state);

    expect(checked.state.groups[0]?.consumers[0]?.status).toBe("dead");
    expect(checked.state.events.some((event) => event.type === "consumer-timeout")).toBe(true);
  });

  it("loads every learning scenario with a valid deterministic state", () => {
    for (const id of ["basic-produce-consume", "keyed-partitioning", "slow-consumer", "consumer-scaling", "rebalance", "duplicate-processing", "loss-before-processing", "multiple-groups"] as const) {
      const state = loadScenario(id);
      expect(state.topic.partitions.length).toBeGreaterThan(0);
      expect(findInvariantViolations(state)).toEqual([]);
    }
  });
});
