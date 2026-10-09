import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ChartNodeFrame } from "@/components/KafkaProcessCanvas";
import { projectChart } from "@/components/chart-views";
import { useKafkaStore } from "@/store/kafka-store";

describe("chart node tooltips", () => {
  let anchorRect: DOMRect;
  let tooltipRect: DOMRect;
  let animationFrames: Map<number, FrameRequestCallback>;

  function rect(x: number, y: number, width: number, height: number): DOMRect {
    return { x, y, width, height, top: y, left: x, right: x + width, bottom: y + height, toJSON: () => ({}) };
  }

  function renderTopic(wrapped = false, focusOnMount = false) {
    const node = projectChart(useKafkaStore.getState().snapshot, "flow").nodes.find((candidate) => candidate.id === "topic")!;
    const frame = <ChartNodeFrame id={node.id} data={node.data} selected={false} />;
    const view = render(wrapped ? <div ref={focusOnMount ? (element) => { element?.focus(); } : undefined} className="react-flow__node" tabIndex={0} role="group" aria-label="Topic wrapper" aria-describedby="graph-keyboard-help" style={{ transform: "translate(300px, 200px) scale(.5)", overflow: "hidden" }}>{frame}</div> : frame);
    const chartNode = view.container.querySelector<HTMLElement>(".kl-flow-node")!;
    const wrapper = wrapped ? screen.getByRole("group", { name: "Topic wrapper" }) : chartNode;
    return { ...view, chartNode, wrapper };
  }

  function nextFrame() {
    const callbacks = [...animationFrames.values()];
    animationFrames.clear();
    act(() => callbacks.forEach((callback) => callback(0)));
  }

  beforeEach(() => {
    useKafkaStore.getState().actions.reset();
    vi.useFakeTimers();
    vi.stubGlobal("innerWidth", 1000);
    vi.stubGlobal("innerHeight", 700);
    anchorRect = rect(100, 150, 200, 80);
    tooltipRect = rect(0, 0, 260, 180);
    animationFrames = new Map();
    let frameId = 0;
    vi.stubGlobal("requestAnimationFrame", vi.fn((callback: FrameRequestCallback) => {
      animationFrames.set(++frameId, callback);
      return frameId;
    }));
    vi.stubGlobal("cancelAnimationFrame", vi.fn((id: number) => animationFrames.delete(id)));
    vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(function (this: HTMLElement) {
      return this.classList.contains("kl-chart-tooltip") ? tooltipRect : anchorRect;
    });
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("keeps the teaching sentence visible and opens extra details only on request", () => {
    const { chartNode } = renderTopic();
    const details = screen.getByRole("button", { name: "Details for orders" });
    expect(chartNode.querySelector(".kl-flow-node__lesson")).toHaveTextContent("same key");
    fireEvent.mouseEnter(chartNode);
    act(() => chartNode.focus());
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    expect(details).toHaveAttribute("aria-expanded", "false");
    fireEvent.click(details);
    expect(screen.getByRole("tooltip")).toHaveTextContent("Stored records");
    expect(details).toHaveAttribute("aria-expanded", "true");
    expect(details).toHaveAttribute("aria-controls", "chart-tooltip-topic");
    fireEvent.mouseLeave(chartNode);
    act(() => vi.advanceTimersByTime(500));
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
    fireEvent.click(details);
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("portals outside transformed and clipped ancestors and clamps the measured size", () => {
    anchorRect = rect(740, 530, 160, 80);
    const { container } = renderTopic(true);
    fireEvent.click(screen.getByRole("button", { name: "Details for orders" }));
    const tooltip = screen.getByRole("tooltip");
    expect(tooltip.parentElement).toBe(document.body);
    expect(container).not.toContainElement(tooltip);
    expect(tooltip).toHaveStyle({ left: "468px", top: "508px" });
    vi.stubGlobal("innerWidth", 320);
    vi.stubGlobal("innerHeight", 500);
    anchorRect = rect(20, 330, 250, 70);
    tooltipRect = rect(0, 0, 296, 260);
    nextFrame();
    expect(tooltip).toHaveStyle({ left: "12px", top: "58px" });
  });

  it("follows graph movement while requested details are open and stops tracking on close", () => {
    const { unmount } = renderTopic();
    fireEvent.click(screen.getByRole("button", { name: "Details for orders" }));
    expect(screen.getByRole("tooltip")).toHaveStyle({ left: "312px", top: "150px" });
    anchorRect = rect(800, 550, 100, 40);
    tooltipRect = rect(0, 0, 240, 220);
    nextFrame();
    expect(screen.getByRole("tooltip")).toHaveStyle({ left: "548px", top: "468px" });
    unmount();
    expect(animationFrames.size).toBe(0);
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("supports deliberate keyboard details and returns focus on Escape", () => {
    const { chartNode, wrapper } = renderTopic(true);
    expect(chartNode).toHaveAttribute("tabindex", "-1");
    act(() => wrapper.focus());
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    fireEvent.keyDown(wrapper, { key: "F2" });
    const tooltip = screen.getByRole("tooltip");
    expect(tooltip).toHaveFocus();
    expect(wrapper).toHaveAttribute("aria-describedby", "graph-keyboard-help chart-tooltip-topic");
    fireEvent.scroll(tooltip, { target: { scrollTop: 80 } });
    expect(tooltip.scrollTop).toBe(80);
    fireEvent.keyDown(tooltip, { key: "Escape" });
    expect(screen.getByRole("button", { name: "Details for orders" })).toHaveFocus();
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    expect(wrapper).toHaveAttribute("aria-describedby", "graph-keyboard-help");
  });

  it("dismisses requested details on an outside pointer action", () => {
    renderTopic();
    fireEvent.click(screen.getByRole("button", { name: "Details for orders" }));
    fireEvent.pointerDown(document.body);
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    expect(animationFrames.size).toBe(0);
  });

  it("closes the previous node's details and geometry tracking when keyboard focus leaves its wrapper and portal", () => {
    const nodes = projectChart(useKafkaStore.getState().snapshot, "flow").nodes;
    const first = nodes.find((node) => node.id === "topic")!;
    const second = nodes.find((node) => node.id === "partition-0")!;
    render(<>{[first, second].map((node) => <div key={node.id} className="react-flow__node" tabIndex={0}><ChartNodeFrame id={node.id} data={node.data} selected={false} /></div>)}</>);
    const firstButton = screen.getByRole("button", { name: "Details for orders" });
    const secondButton = screen.getByRole("button", { name: "Details for P0" });
    act(() => firstButton.focus());
    fireEvent.click(firstButton);
    fireEvent.keyDown(firstButton, { key: "F2" });
    expect(screen.getByRole("tooltip")).toHaveFocus();
    act(() => firstButton.focus());
    expect(screen.getByRole("tooltip")).toHaveAttribute("id", "chart-tooltip-topic");
    expect(animationFrames.size).toBe(1);

    act(() => secondButton.focus());
    expect(firstButton).toHaveAttribute("aria-expanded", "false");
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    expect(animationFrames.size).toBe(0);
    fireEvent.click(secondButton);
    expect(screen.getAllByRole("tooltip")).toHaveLength(1);
    expect(screen.getByRole("tooltip")).toHaveAttribute("id", "chart-tooltip-partition-0");
    expect(animationFrames.size).toBe(1);
    act(() => secondButton.blur());
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    expect(animationFrames.size).toBe(0);
  });

  it("does not open on initial wrapper focus or replay after a hidden page returns", () => {
    const { wrapper } = renderTopic(true, true);
    expect(wrapper).toHaveFocus();
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Details for orders" }));
    const visibility = vi.spyOn(document, "visibilityState", "get").mockReturnValue("hidden");
    fireEvent(document, new Event("visibilitychange"));
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    expect(animationFrames.size).toBe(0);
    visibility.mockReturnValue("visible");
    fireEvent(document, new Event("visibilitychange"));
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("keeps partition ownership and position details available", () => {
    const node = projectChart(useKafkaStore.getState().snapshot, "partitions").nodes.find((candidate) => candidate.id === "partition-0");
    expect(node?.data.tooltip.rows.map((row) => row.label)).toEqual(expect.arrayContaining(["Owner", "Records", "C / P / LEO", "Commit lag"]));
  });
});
