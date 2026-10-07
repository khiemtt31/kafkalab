import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { GuideExperiments } from "@/components/GuideExperiments";
import { experiments, runExperiment, commitExperiment, canCommitExperiment } from "@/learning/experiments";
import { LEARNING_STORAGE_KEY, useLearningProgress, validateLearningProgress } from "@/learning/progress";
import { persistKafkaWorkspace, useKafkaStore } from "@/store/kafka-store";

describe("guided causal experiments with actual simulator evidence", () => {
  beforeEach(() => { vi.useFakeTimers(); localStorage.clear(); useKafkaStore.setState({ hydrated: true }); useKafkaStore.getState().actions.reset(); useLearningProgress.setState({ progress: {}, hydrated: false, notice: undefined }); });
  afterEach(() => { cleanup(); useKafkaStore.getState().actions.reset(); vi.restoreAllMocks(); vi.useRealTimers(); });
  it("requires prediction, keeps disclosures initially collapsed, and does not mutate the workflow when choosing", () => {
    const before = useKafkaStore.getState().simulation; render(<GuideExperiments />);
    expect(screen.getAllByRole("button", { name: "Run experiment", hidden: true })).toHaveLength(5);
    const detail = document.querySelector('[data-experiment-id="keyed-ordering"]')!;
    expect(detail.hasAttribute("open")).toBe(false); detail.setAttribute("open", "");
    expect(within(detail as HTMLElement).getByRole("button", { name: "Run experiment" })).toBeDisabled();
    fireEvent.click(within(detail as HTMLElement).getByLabelText(experiments[0].choices[1]));
    expect(useKafkaStore.getState().simulation).toBe(before);
    fireEvent.click(within(detail as HTMLElement).getByRole("button", { name: "Run experiment" }));
    expect(screen.getByText(/This run changes the prediction/)).toBeInTheDocument();
    expect(useKafkaStore.getState().snapshot.messageCount).toBe(3);
  });
  it("shows actual per-key partition-local offsets with untouched other lanes", () => {
    const result = runExperiment("keyed-ordering"); expect(result.before.stored).toBe(0); expect(result.after.stored).toBe(3);
    expect(new Set(result.after.records.map((r) => r.partition)).size).toBe(1);
    expect(result.after.records.map((r) => r.offset)).toEqual([0, 1, 2]);
    expect(result.after.positions.filter((p) => p.end === 0)).toHaveLength(2);
  });
  it("replays the same successful record before commit instead of appending another record", () => {
    const result = runExperiment("replay-before-commit");
    expect(result.before.records[0]).toMatchObject({ attempt: 1, outcome: "succeeded" });
    expect(result.after.records[0]).toMatchObject({ id: result.before.records[0].id, offset: 0, attempt: 2, outcome: "succeeded" });
    expect(result.after.stored).toBe(1); expect(result.after.positions[0].committed).toBe(0); expect(result.after.duplicateEvents).toBeGreaterThan(0);
  });
  it("captures the real empty restarted poll and retained record after an early commit", () => {
    const result = runExperiment("commit-before-processing");
    expect(result.before.positions[0]).toMatchObject({ read: 1, committed: 0 });
    expect(result.after.positions[0]).toMatchObject({ read: 1, committed: 1 });
    expect(result.after).toMatchObject({ stored: 1, processed: 0, skipped: 1, recordsReadAfterRestart: 0 });
  });
  it("leaves two workers idle and keeps one owner of a hot-key lane", () => {
    const result = runExperiment("worker-scaling");
    expect(result.before.workers).toBe(1); expect(result.after).toMatchObject({ partitions: 3, workers: 5, assigned: 3, idle: 2 });
    expect(new Set(result.after.records.map((r) => r.partition)).size).toBe(1);
    expect(new Set(result.after.positions.map((p) => p.owner)).size).toBe(3);
  });
  it("keeps retry identity and commit separate, and rejects the commit button after a new workflow", () => {
    const result = runExperiment("failure-retry-commit");
    expect(result.before.records[0]).toMatchObject({ id: result.recordId, attempt: 1, outcome: "failed" });
    expect(result.after.records[0]).toMatchObject({ id: result.recordId, attempt: 2, outcome: "succeeded" });
    expect(result.after.stored).toBe(result.before.stored);
    expect(result.after.positions[1].committed).toBe(0); expect(canCommitExperiment(result, useKafkaStore.getState().snapshot)).toBe(true);
    const committed = commitExperiment(result); expect(committed.committed?.positions[1].committed).toBe(1); expect(committed.committed?.stored).toBe(24);
    useKafkaStore.getState().actions.reset(); expect(canCommitExperiment(result, useKafkaStore.getState().snapshot)).toBe(false);
    expect(commitExperiment(result)).toBe(result);
  });
  it("persists only valid prediction/completion, and keeps all three reset domains independent", () => {
    useLearningProgress.getState().predict("keyed-ordering", 0); useLearningProgress.getState().complete("keyed-ordering");
    const actions = useKafkaStore.getState().actions; actions.selectPracticeQuestion?.("easy", 3);
    runExperiment("keyed-ordering"); expect(useKafkaStore.getState().practiceProgress.currentQuestion.easy).toBe(3);
    actions.reset(); expect(useLearningProgress.getState().progress["keyed-ordering"]?.completed).toBe(true);
    const simulation = useKafkaStore.getState().simulation;
    useLearningProgress.getState().reset(); expect(useKafkaStore.getState().simulation).toBe(simulation);
    expect(useKafkaStore.getState().practiceProgress.currentQuestion.easy).toBe(3);
    expect(validateLearningProgress({ version: 1, progress: { "keyed-ordering": { prediction: 99, completed: true }, unknown: { prediction: 1, completed: true } } })).toEqual({});
    useLearningProgress.setState({ progress: {}, hydrated: false });
    localStorage.setItem(LEARNING_STORAGE_KEY, JSON.stringify({ version: 1, progress: { "keyed-ordering": { prediction: 1, completed: true } } }));
    useLearningProgress.getState().hydrate(); expect(useLearningProgress.getState().progress["keyed-ordering"]).toEqual({ prediction: 1, completed: true });
    expect(useKafkaStore.getState().simulation).toBe(simulation);
  });
  it("rejects a retry commit after ownership changes without claiming a checkpoint advanced", () => {
    const result = runExperiment("failure-retry-commit");
    useKafkaStore.getState().actions.removeConsumer("consumer-1");
    expect(canCommitExperiment(result, useKafkaStore.getState().snapshot)).toBe(false);
    expect(commitExperiment(result)).toBe(result);
    expect(useKafkaStore.getState().snapshot.partitions[1].committedOffset).toBe(0);
    const fresh = runExperiment("failure-retry-commit");
    useKafkaStore.getState().actions.restartConsumer("consumer-1");
    expect(canCommitExperiment(fresh, useKafkaStore.getState().snapshot)).toBe(false);
    expect(commitExperiment(fresh)).toBe(fresh);
  });
  it("keeps learning usable when storage fails without overwriting simulator state", () => {
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => { throw new Error("denied"); });
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => { throw new Error("quota"); });
    useLearningProgress.getState().hydrate(); useLearningProgress.getState().predict("keyed-ordering", 2);
    expect(useLearningProgress.getState().progress["keyed-ordering"]?.prediction).toBe(2); expect(useLearningProgress.getState().notice).toMatch(/session/);
  });
  it("watermarks replacement/restored events and preserves the watermark through ordinary actions", () => {
    const actions = useKafkaStore.getState().actions; actions.loadReferenceFlow?.();
    const boundary = useKafkaStore.getState().snapshot.flowBaselineEventId;
    expect(boundary).toBe(useKafkaStore.getState().simulation.events.at(-1)?.id);
    actions.produce({ key: "new", value: "new" }); expect(useKafkaStore.getState().snapshot.flowBaselineEventId).toBe(boundary);
    persistKafkaWorkspace(); const saved = JSON.parse(localStorage.getItem("kafkalab:workspace:v1")!);
    expect(saved.flowBaselineEventId).toBeUndefined();
    actions.hydrate?.(); expect(useKafkaStore.getState().snapshot.flowBaselineEventId).toBe(useKafkaStore.getState().simulation.events.at(-1)?.id);
    act(() => actions.reset());
  });
});
