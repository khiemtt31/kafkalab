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

  it("shows a labelled tooltip on pointer hover and keyboard focus", () => {
    const node = projectChart(useKafkaStore.getState().snapshot, "flow").nodes.find((candidate) => candidate.id === "topic");
    expect(node).toBeDefined();
    render(<ChartNodeFrame id={node!.id} data={node!.data} selected={false} />);
    const chartNode = screen.getByRole("generic", { name: /topic orders/i });
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();

    fireEvent.mouseEnter(chartNode);
    expect(screen.getByRole("tooltip")).toHaveTextContent("Stored records");
    expect(chartNode).toHaveAttribute("aria-describedby", "chart-tooltip-topic");

    fireEvent.mouseLeave(chartNode);
    act(() => vi.advanceTimersByTime(200));
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    fireEvent.focus(chartNode);
    expect(screen.getByRole("tooltip")).toHaveAttribute("id", "chart-tooltip-topic");
  });

  it("portals outside transformed and clipped ancestors using the measured size", () => {
    anchorRect = rect(740, 530, 160, 80);
    const { chartNode, container } = renderTopic(true);
    fireEvent.mouseEnter(chartNode);
    const tooltip = screen.getByRole("tooltip");
    expect(tooltip.parentElement).toBe(document.body);
    expect(container).not.toContainElement(tooltip);
    expect(tooltip).toHaveStyle({ left: "468px", top: "508px" });
  });

  it("clamps a measured tooltip into a narrow viewport", () => {
    vi.stubGlobal("innerWidth", 320);
    vi.stubGlobal("innerHeight", 500);
    anchorRect = rect(20, 330, 250, 70);
    tooltipRect = rect(0, 0, 296, 260);
    const { chartNode } = renderTopic();
    fireEvent.mouseEnter(chartNode);
    expect(screen.getByRole("tooltip")).toHaveStyle({ left: "12px", top: "58px" });
  });

  it("follows drag, pan, zoom and changing tooltip size without scroll events", () => {
    const { chartNode, unmount } = renderTopic();
    fireEvent.mouseEnter(chartNode);
    expect(screen.getByRole("tooltip")).toHaveStyle({ left: "312px", top: "150px" });
    anchorRect = rect(800, 550, 100, 40);
    tooltipRect = rect(0, 0, 240, 220);
    nextFrame();
    expect(screen.getByRole("tooltip")).toHaveStyle({ left: "548px", top: "468px" });
    vi.stubGlobal("innerHeight", 400);
    nextFrame();
    expect(screen.getByRole("tooltip")).toHaveStyle({ top: "168px" });
    unmount();
    expect(animationFrames.size).toBe(0);
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("keeps details open while crossing the gap and scrolling on the tooltip", () => {
    const { chartNode } = renderTopic();
    fireEvent.mouseEnter(chartNode);
    fireEvent.mouseLeave(chartNode);
    act(() => vi.advanceTimersByTime(100));
    const tooltip = screen.getByRole("tooltip");
    fireEvent.mouseEnter(tooltip);
    fireEvent.scroll(tooltip, { target: { scrollTop: 80 } });
    act(() => vi.advanceTimersByTime(250));
    expect(screen.getByRole("tooltip")).toBe(tooltip);
    expect(tooltip.scrollTop).toBe(80);
    fireEvent.mouseLeave(tooltip);
    act(() => vi.advanceTimersByTime(200));
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("preserves focus visibility when the pointer leaves, and hover when focus leaves", () => {
    const { chartNode } = renderTopic();
    act(() => chartNode.focus());
    fireEvent.mouseEnter(chartNode);
    fireEvent.mouseLeave(chartNode);
    act(() => vi.advanceTimersByTime(250));
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
    fireEvent.mouseEnter(chartNode);
    act(() => chartNode.blur());
    act(() => vi.advanceTimersByTime(250));
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
    fireEvent.mouseLeave(chartNode);
    act(() => vi.advanceTimersByTime(200));
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("uses wrapper focus without a duplicate tab stop and preserves graph instructions", () => {
    const { chartNode, wrapper } = renderTopic(true);
    expect(chartNode).toHaveAttribute("tabindex", "-1");
    act(() => wrapper.focus());
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
    expect(wrapper).toHaveAttribute("aria-describedby", "graph-keyboard-help chart-tooltip-topic");
    fireEvent.keyDown(wrapper, { key: "Escape" });
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    expect(wrapper).toHaveFocus();
    expect(wrapper).toHaveAttribute("aria-describedby", "graph-keyboard-help");
    nextFrame();
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    act(() => wrapper.blur());
    act(() => wrapper.focus());
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
  });

  it("permits keyboard scrolling and restores wrapper focus on Escape", () => {
    const { wrapper } = renderTopic(true);
    act(() => wrapper.focus());
    fireEvent.keyDown(wrapper, { key: "F2" });
    const tooltip = screen.getByRole("tooltip");
    expect(tooltip).toHaveFocus();
    act(() => vi.advanceTimersByTime(250));
    expect(screen.getByRole("tooltip")).toBe(tooltip);
    fireEvent.keyDown(tooltip, { key: "Escape" });
    expect(wrapper).toHaveFocus();
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("dismisses pointer-only details on Escape and reopens on a new hover", () => {
    const { chartNode } = renderTopic();
    fireEvent.mouseEnter(chartNode);
    fireEvent.keyDown(window, { key: "Escape" });
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    expect(animationFrames.size).toBe(0);
    fireEvent.mouseLeave(chartNode);
    fireEvent.mouseEnter(chartNode);
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
  });

  it("uses context-specific detail rows for partition nodes", () => {
    const node = projectChart(useKafkaStore.getState().snapshot, "partitions").nodes.find((candidate) => candidate.id === "partition-0");
    expect(node?.data.tooltip.rows.map((row) => row.label)).toEqual(expect.arrayContaining(["Owner", "Records", "C / P / LEO", "Commit lag"]));
  });
  it("opens details if wrapper focus arrived before the native listeners attached", () => {
    const { wrapper } = renderTopic(true, true);
    expect(wrapper).toHaveFocus();
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
  });
  it("dismisses details and cancels geometry tracking while hidden without replay on return", () => {
    const { wrapper } = renderTopic(true);
    act(() => wrapper.focus());
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
    const visibility = vi.spyOn(document, "visibilityState", "get").mockReturnValue("hidden");
    fireEvent(document, new Event("visibilitychange"));
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    expect(animationFrames.size).toBe(0);
    visibility.mockReturnValue("visible");
    fireEvent(document, new Event("visibilitychange"));
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    act(() => wrapper.blur()); act(() => wrapper.focus());
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
  });
});
