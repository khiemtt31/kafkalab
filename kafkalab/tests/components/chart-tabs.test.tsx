import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { KafkaVisualLab } from "@/components/KafkaVisualLab";
import { useKafkaLabStoreAdapter, useKafkaStore } from "@/store/kafka-store";
import { experiments } from "@/learning/experiments";
import { useLearningProgress } from "@/learning/progress";

vi.mock("@/components/KafkaProcessCanvas", () => ({
  KafkaProcessCanvas: ({ variant, panelId, focused, focusPartitionId }: { variant: string; panelId: string; focused: boolean; focusPartitionId: number }) => <div id={panelId} data-testid="chart-panel" data-focused={focused} data-partition={focusPartitionId}>{variant}</div>,
}));

vi.mock("@/hooks/useReducedMotion", () => ({ useReducedMotion: () => false }));

describe("chart tabs and accessible tool navigation", () => {
  afterEach(() => cleanup());
  beforeEach(() => {
    localStorage.clear();
    useLearningProgress.setState({ progress: {}, hydrated: false, notice: undefined });
    useKafkaStore.getState().actions.reset();
  });

  it("starts on Flow and switches through native tab semantics without Customize controls", () => {
    render(<KafkaVisualLab store={useKafkaStore.getState()} />);
    const tabs = screen.getAllByRole("tab");
    expect(tabs).toHaveLength(7);
    expect(screen.getByRole("tab", { name: "Flow" })).toHaveAttribute("aria-selected", "true");
    expect(screen.getByTestId("chart-panel")).toHaveTextContent("flow");
    expect(screen.queryByRole("button", { name: /Send order/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Load reference flow/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /customize/i })).not.toBeInTheDocument();

    fireEvent.keyDown(screen.getByRole("tab", { name: "Flow" }), { key: "ArrowRight" });
    expect(screen.getByRole("tab", { name: "Partitions" })).toHaveAttribute("aria-selected", "true");
    expect(screen.getByTestId("chart-panel")).toHaveTextContent("partitions");
    fireEvent.keyDown(screen.getByRole("tab", { name: "Partitions" }), { key: "End" });
    expect(screen.getByRole("tab", { name: "Activity" })).toHaveAttribute("aria-selected", "true");
  });

  it("focuses the failure and lets the learner expand or follow another lane without mutating Kafka", () => {
    useKafkaStore.getState().actions.loadReferenceFlow?.();
    const simulation = useKafkaStore.getState().simulation;
    render(<KafkaVisualLab store={useKafkaStore.getState()} />);
    expect(screen.getByTestId("chart-panel")).toHaveAttribute("data-partition", "1");
    expect(screen.getByTestId("chart-panel")).toHaveAttribute("data-focused", "true");
    expect(screen.getByRole("heading", { level: 1 })).toHaveTextContent("Where does the journey break?");
    fireEvent.click(screen.getByRole("button", { name: "Show whole system" }));
    expect(screen.getByTestId("chart-panel")).toHaveAttribute("data-focused", "false");
    fireEvent.change(screen.getByLabelText("Follow partition"), { target: { value: "2" } });
    expect(screen.getByTestId("chart-panel")).toHaveAttribute("data-partition", "2");
    expect(useKafkaStore.getState().simulation).toBe(simulation);
    fireEvent.click(screen.getByRole("tab", { name: "Delivery" }));
    expect(screen.queryByLabelText("Follow partition")).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { level: 1 })).toHaveTextContent("Did the application actually succeed?");
  });

  it("updates the real Flow after retry and separate commit and keeps Practice answers", () => {
    function LiveFlow() { return <KafkaVisualLab store={useKafkaLabStoreAdapter()} />; }
    useKafkaStore.getState().actions.answerPracticeQuestion?.("easy", "kafka-easy-01", "a");
    const practice = useKafkaStore.getState().practiceProgress;
    render(<LiveFlow />);
    expect(screen.getByRole("button", { name: "Run challenge" })).toBeDisabled();
    fireEvent.click(screen.getByLabelText(experiments[4].choices[0]));
    fireEvent.click(screen.getByRole("button", { name: "Run challenge" }));
    expect(screen.getByTestId("chart-panel")).toHaveAttribute("data-partition", "1");
    expect(screen.getByText(/Succeeded, awaiting commit/)).toBeInTheDocument();
    expect(useKafkaStore.getState().snapshot.partitions[1].committedOffset).toBe(0);
    fireEvent.click(screen.getByRole("button", { name: "Commit successful retry" }));
    expect(screen.getByText(/Checkpoint passed/)).toBeInTheDocument();
    expect(useKafkaStore.getState().snapshot.partitions[1].committedOffset).toBe(1);
    expect(useKafkaStore.getState().practiceProgress).toBe(practice);
  });

  it("does not expose global challenge mutations for an alternate embedded adapter", () => {
    const store = useKafkaStore.getState();
    render(<KafkaVisualLab store={{ ...store, actions: { ...store.actions } }} />);
    expect(screen.queryByRole("complementary", { name: "Interactive Kafka challenge" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Run flow" })).toBeInTheDocument();
  });
});
