import { beforeEach, describe, expect, it } from "vitest";
import { learningFocus } from "@/learning/flow-learning";
import { runExperiment, commitExperiment } from "@/learning/experiments";
import { useKafkaStore } from "@/store/kafka-store";

describe("live learning diagnosis", () => {
  beforeEach(() => useKafkaStore.getState().actions.reset());

  it("pinpoints the actual failed record, then distinguishes retry success from saving", () => {
    useKafkaStore.getState().actions.loadReferenceFlow?.();
    const failed = learningFocus(useKafkaStore.getState().snapshot);
    expect(failed.partitionId).toBe(1);
    expect(failed.title).toContain("offset 0");
    expect(failed.detail).toContain("Inventory service timed out");
    const result = runExperiment("failure-retry-commit");
    const retried = learningFocus(useKafkaStore.getState().snapshot, result);
    expect(retried.title).toContain("awaiting commit");
    expect(retried.title).toContain(result.recordId);
    expect(retried.title).toContain("attempt 2");
    const saved = commitExperiment(result);
    const focus = learningFocus(useKafkaStore.getState().snapshot, saved);
    expect(focus.title).toContain("Checkpoint passed");
    expect(focus.detail).toContain("record is still stored");
  });

  it("does not claim that premature commit proves application success", () => {
    const result = runExperiment("commit-before-processing");
    const focus = learningFocus(useKafkaStore.getState().snapshot, result);
    expect(focus.title).toContain("Work skipped");
    expect(focus.detail).toContain("application work did not succeed");
  });

  it("discards captured record focus after workflow replacement and honors a chosen lane", () => {
    const result = runExperiment("failure-retry-commit");
    useKafkaStore.getState().actions.reset();
    const focus = learningFocus(useKafkaStore.getState().snapshot, result);
    expect(focus.partitionId).toBe(0);
    expect(focus.title).toContain("ready for records");
    expect(learningFocus(useKafkaStore.getState().snapshot, result, 2).partitionId).toBe(2);
    expect(learningFocus(useKafkaStore.getState().snapshot, result, 99).partitionId).toBe(0);
  });

  it("attributes a failed read to its actual attempt consumer after assignment changes", () => {
    useKafkaStore.getState().actions.loadReferenceFlow?.();
    useKafkaStore.getState().actions.removeConsumer("consumer-1");
    const focus = learningFocus(useKafkaStore.getState().snapshot);
    expect(focus.detail).toContain("was read by consumer-1");
    expect(focus.detail).toContain("Current owner: consumer-2");
  });
});
