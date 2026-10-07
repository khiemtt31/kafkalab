import { describe, expect, it } from "vitest";
import {
  addConsumer, assertInvariants, canProduceWithAcks, commitCurrentOffsets, commitProcessedOffsets,
  createBrokerCluster, createReferenceFlow, createSimulationState, failBroker, failRecordProcessing,
  getLatestDeliveryAttempt, pollConsumer, processRecords, produceMessage, removeConsumer,
  retryRecordProcessing, setRequiredAcks,
} from "@/core/kafka";

function pendingRecord() {
  let state = addConsumer(createSimulationState({ partitionCount: 1 }), "analytics", "worker").state;
  state = produceMessage(state, { value: "pending-order" }).state;
  return pollConsumer(state, "analytics", "worker").state;
}

describe("separate application processing and group commits", () => {
  it("retains a failed record and pending offset without letting playback or either commit mode skip it", () => {
    const original = pendingRecord();
    const failed = failRecordProcessing(original, "analytics", "worker", { partition: 0, offset: 0, reason: "  Payment\nservice timed out  " });
    expect(original.deliveries[0].applicationStatus).toBe("pending");
    expect(failed.value).toMatchObject({ applicationStatus: "failed", status: "failed", failureReason: "Payment service timed out", attempt: 1 });
    expect(failed.state.topic.partitions[0].messages).toEqual(original.topic.partitions[0].messages);
    expect(failed.state.groups[0].consumers[0].pendingOffsets[0]).toEqual([0]);
    expect(failed.state.groups[0].consumers[0].currentPositions[0]).toBe(1);
    expect(failed.events[0]).toMatchObject({ type: "record-processing-failed", entityId: original.deliveries[0].messageId,
      details: { groupId: "analytics", consumerId: "worker", messageId: original.deliveries[0].messageId, partition: 0, offset: 0, attempt: 1, reason: "Payment service timed out" } });
    expect(processRecords(failed.state, "analytics", "worker").value).toEqual([]);
    for (const commit of [commitProcessedOffsets, commitCurrentOffsets]) {
      const result = commit(failed.state, "analytics", "worker");
      expect(result.value).toEqual({});
      expect(result.state.groups[0].committedOffsets[0]).toBe(0);
      if (commit === commitCurrentOffsets) {
        expect(result.events[0]).toMatchObject({ type: "offset-commit-blocked", details: { blockedOffset: 0, requestedOffset: 1 } });
        expect(result.events[0].message).toMatch(/Kafka itself permits explicit commits past failed work/);
      } else expect(result.events).toEqual([]);
    }
    assertInvariants(failed.state);
  });

  it("commits only the successful prefix before a failure, then closes the gap after an explicit successful retry", () => {
    let state = addConsumer(createSimulationState({ partitionCount: 1 }), "analytics", "worker").state;
    for (const value of ["first", "failed", "later-success"]) state = produceMessage(state, { value }).state;
    state = processRecords(pollConsumer(state, "analytics", "worker").state, "analytics", "worker").state;
    state = pollConsumer(state, "analytics", "worker").state;
    state = failRecordProcessing(state, "analytics", "worker", { partition: 0, offset: 1, reason: "Application rejected the order" }).state;
    state = processRecords(pollConsumer(state, "analytics", "worker").state, "analytics", "worker").state;
    for (const commit of [commitProcessedOffsets, commitCurrentOffsets]) {
      expect(commit(state, "analytics", "worker").value).toEqual({ 0: 1 });
    }
    state = commitProcessedOffsets(state, "analytics", "worker").state;
    const previous = getLatestDeliveryAttempt(state, "analytics", 0, 1)!;
    const retry = retryRecordProcessing(state, "analytics", "worker", { partition: 0, offset: 1 });
    expect(retry.value).toMatchObject({ messageId: previous.messageId, attempt: 2, applicationStatus: "pending", status: "read" });
    expect(retry.value?.id).not.toBe(previous.id);
    expect(retry.events[0]).toMatchObject({ type: "record-processing-retried", entityId: previous.messageId,
      details: { groupId: "analytics", consumerId: "worker", messageId: previous.messageId, partition: 0, offset: 1, attempt: 2, previousAttempt: 1 } });
    expect(retry.state.groups[0].committedOffsets[0]).toBe(1);

    const processed = processRecords(retry.state, "analytics", "worker", { partition: 0, offset: 1 });
    expect(processed.value).toHaveLength(1);
    expect(getLatestDeliveryAttempt(processed.state, "analytics", 0, 1)?.applicationStatus).toBe("succeeded");
    expect(processed.state.groups[0].committedOffsets[0]).toBe(1);
    const committed = commitProcessedOffsets(processed.state, "analytics", "worker").state;
    expect(committed.groups[0].committedOffsets[0]).toBe(3);
    expect(committed.deliveries.find((delivery) => delivery.id === previous.id)?.applicationStatus).toBe("failed");
    expect(committed.deliveries.find((delivery) => delivery.id === previous.id)?.status).toBe("failed");
    expect(getLatestDeliveryAttempt(committed, "analytics", 0, 1)?.status).toBe("committed");
    expect(committed.topic.partitions[0].messages).toHaveLength(3);
    assertInvariants(committed);
  });

  it("keeps application outcomes independent for two groups reading the same record", () => {
    let state = pendingRecord();
    state = failRecordProcessing(state, "analytics", "worker", { partition: 0, offset: 0, reason: "Failed analytics side effect" }).state;
    state = addConsumer(state, "payments", "payments-worker").state;
    state = pollConsumer(state, "payments", "payments-worker").state;
    state = processRecords(state, "payments", "payments-worker").state;
    state = commitProcessedOffsets(state, "payments", "payments-worker").state;
    expect(getLatestDeliveryAttempt(state, "analytics", 0, 0)?.applicationStatus).toBe("failed");
    expect(getLatestDeliveryAttempt(state, "payments", 0, 0)?.applicationStatus).toBe("succeeded");
    expect(state.groups.find((group) => group.id === "analytics")?.committedOffsets[0]).toBe(0);
    expect(state.groups.find((group) => group.id === "payments")?.committedOffsets[0]).toBe(1);
  });

  it("allows the current owner to retry after the failed owner's assignment changes", () => {
    const reference = createReferenceFlow();
    const state = removeConsumer(reference, "analytics-group", "consumer-1").state;
    expect(state.groups[0].assignments[1]).toBe("consumer-2");
    expect(pollConsumer(state, "analytics-group", "consumer-2", { partition: 1 }).value).toEqual([]);
    const retry = retryRecordProcessing(state, "analytics-group", "consumer-2", { partition: 1, offset: 0 });
    expect(retry.value).toMatchObject({ consumerId: "consumer-2", attempt: 2, applicationStatus: "pending" });
    assertInvariants(retry.state);
  });

  it("bounds superseded attempt history without discarding latest outcomes or resetting retry numbers", () => {
    let state = pendingRecord();
    state.config.maxDeliveries = 2;
    state = failRecordProcessing(state, "analytics", "worker", { partition: 0, offset: 0, reason: "Temporary failure" }).state;
    for (let index = 0; index < 8; index += 1) {
      state = retryRecordProcessing(state, "analytics", "worker", { partition: 0, offset: 0 }).state;
      state = failRecordProcessing(state, "analytics", "worker", { partition: 0, offset: 0, reason: "Still failing" }).state;
      expect(state.deliveries.length).toBeLessThanOrEqual(2);
    }
    expect(getLatestDeliveryAttempt(state, "analytics", 0, 0)?.attempt).toBe(9);
    for (const value of ["another", "one-more"]) {
      state = produceMessage(state, { value }).state;
      state = pollConsumer(state, "analytics", "worker").state;
    }
    expect(state.deliveries).toHaveLength(3);
    expect(getLatestDeliveryAttempt(state, "analytics", 0, 0)?.applicationStatus).toBe("failed");
    expect(retryRecordProcessing(state, "analytics", "worker", { partition: 0, offset: 0 }).value?.attempt).toBe(10);
  });

  it.each([
    { group: "missing", worker: "consumer-1", partition: 1, offset: 0 },
    { group: "analytics-group", worker: "missing", partition: 1, offset: 0 },
    { group: "analytics-group", worker: "consumer-0", partition: 1, offset: 0 },
    { group: "analytics-group", worker: "consumer-0", partition: 0, offset: 0 },
    { group: "analytics-group", worker: "consumer-1", partition: 1, offset: 1 },
    { group: "analytics-group", worker: "consumer-1", partition: 1, offset: -1 },
    { group: "analytics-group", worker: "consumer-1", partition: 1, offset: 0.5 },
    { group: "analytics-group", worker: "consumer-1", partition: 99, offset: 0 },
  ])("ignores invalid, committed, unread, or unassigned fail/retry targets $group/$worker/$partition/$offset", ({ group, worker, partition, offset }) => {
    const state = createReferenceFlow();
    for (const action of [
      () => failRecordProcessing(state, group, worker, { partition, offset, reason: "Unexpected error" }),
      () => retryRecordProcessing(state, group, worker, { partition, offset }),
    ]) {
      const result = action();
      expect(result.state).toBe(state);
      expect(result.value).toBeUndefined();
      expect(result.events).toEqual([]);
    }
  });
});

describe("acks admission uses live ISR and the configured minimum", () => {
  it("accepts two current ISR members at RF3/minISR2 and rejects fewer than the minimum", () => {
    let state = createBrokerCluster(createSimulationState({ partitionCount: 1 }), 3, 3, 2).state;
    state = setRequiredAcks(state, "all").state;
    state = failBroker(state, "broker-1").state;
    expect(canProduceWithAcks(state, 0)).toMatchObject({ accepted: true, inSyncReplicas: ["broker-2", "broker-3"] });
    state = failBroker(state, "broker-2").state;
    expect(canProduceWithAcks(state, 0)).toMatchObject({ accepted: false, inSyncReplicas: ["broker-3"] });
    expect(canProduceWithAcks(state, 0).reason).toMatch(/min.insync.replicas/);
    state = setRequiredAcks(state, "1").state;
    expect(canProduceWithAcks(state, 0).accepted).toBe(true);
  });

  it("does not count down brokers or duplicate replica IDs as live ISR capacity", () => {
    const state = createBrokerCluster(createSimulationState({ partitionCount: 1 }), 3, 3, 2).state;
    state.cluster!.requiredAcks = "all";
    state.cluster!.brokers[1].status = "down";
    state.cluster!.brokers[2].status = "down";
    state.cluster!.assignments[0].inSyncReplicas.push("broker-1");
    expect(canProduceWithAcks(state, 0)).toMatchObject({ accepted: false, inSyncReplicas: ["broker-1"] });
  });
});
