import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { KafkaVisualLab } from "@/components/KafkaVisualLab";
import { useKafkaStore } from "@/store/kafka-store";

vi.mock("@/components/KafkaProcessCanvas", () => ({
  KafkaProcessCanvas: ({ variant, panelId }: { variant: string; panelId: string }) => <div id={panelId} data-testid="chart-panel">{variant}</div>,
}));

vi.mock("@/hooks/useReducedMotion", () => ({ useReducedMotion: () => false }));

describe("chart tabs and accessible tool navigation", () => {
  beforeEach(() => useKafkaStore.getState().actions.reset());

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
});
