import { act, cleanup, render } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FlowPacket } from "@/components/FlowPacket";
import type { TransitToken } from "@/components/flow-transit";

const pointAtLength = vi.fn((distance: number) => ({ x: distance, y: 0 }));
const totalLength = vi.fn(() => 100);
const token: TransitToken = { id: "event-1:record-1", sourceEventId: "event-1", messageId: "record-1", partition: 0,
  offset: 0, phase: "produced", attempt: 1, edgeId: "producer-a-topic", label: "order-A", delayMs: 0, durationMs: 680 };
const props = { token, path: "M0 0 L100 0", motion: true, staticX: 50, staticY: 0 };

describe("finite packet timing", () => {
  beforeEach(() => {
    vi.useFakeTimers(); pointAtLength.mockClear(); totalLength.mockClear();
    Object.defineProperty(SVGElement.prototype, "getTotalLength", { configurable: true, value: totalLength });
    Object.defineProperty(SVGElement.prototype, "getPointAtLength", { configurable: true, value: pointAtLength });
    vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => setTimeout(() => callback(performance.now()), 16));
    vi.stubGlobal("cancelAnimationFrame", (id: number) => clearTimeout(id));
  });
  afterEach(() => {
    cleanup(); vi.useRealTimers(); vi.unstubAllGlobals();
    Reflect.deleteProperty(SVGElement.prototype, "getTotalLength"); Reflect.deleteProperty(SVGElement.prototype, "getPointAtLength");
  });

  it("starts a late-mounted packet now and removes it at its deadline", () => {
    vi.advanceTimersByTime(10000);
    const { container } = render(<svg><FlowPacket {...props} /></svg>);
    act(() => vi.advanceTimersByTime(0));
    const first = container.querySelector(".kl-flow-token")?.getAttribute("transform");
    act(() => vi.advanceTimersByTime(160));
    expect(container.querySelector(".kl-flow-token")?.getAttribute("transform")).not.toBe(first);
    expect(pointAtLength).toHaveBeenCalledWith(100 * 160 / 680);
    expect(container.querySelector(".kl-flow-token")?.getAttribute("opacity")).toBe("1");
    act(() => vi.advanceTimersByTime(680)); expect(container.querySelector(".kl-flow-token")).toBeNull();
  });
  it("hides the routed stage, paints its exact destination, then removes it", () => {
    const { container } = render(<svg><FlowPacket {...props} token={{ ...token, delayMs: 760 }} /></svg>);
    expect(container.querySelector(".kl-flow-token")?.getAttribute("opacity")).toBe("0");
    act(() => vi.advanceTimersByTime(759)); expect(pointAtLength).not.toHaveBeenCalled();
    act(() => vi.advanceTimersByTime(1)); expect(pointAtLength).toHaveBeenCalledWith(0);
    act(() => vi.advanceTimersByTime(680));
    expect(container.querySelector(".kl-flow-token")?.getAttribute("transform")).toBe("translate(100 0)");
    act(() => vi.advanceTimersByTime(32));
    expect(container.querySelector(".kl-flow-token")).toBeNull();
  });
  it("does not restart when token objects or node paths change", () => {
    const { rerender, container } = render(<svg><FlowPacket {...props} /></svg>);
    act(() => vi.advanceTimersByTime(250));
    rerender(<svg><FlowPacket {...props} token={{ ...token }} path="M0 0 L200 100" /></svg>);
    expect(totalLength).toHaveBeenCalledTimes(2);
    act(() => vi.advanceTimersByTime(16));
    expect(pointAtLength.mock.calls.at(-1)?.[0]).toBeGreaterThan(100 * 250 / 680);
    act(() => vi.advanceTimersByTime(446)); expect(container.querySelector(".kl-flow-token")).toBeNull();
  });
  it("resumes only the remaining event lifetime after a remount", () => {
    const createdAt = Date.now(); act(() => vi.advanceTimersByTime(600));
    const { container } = render(<svg><FlowPacket {...props} token={{ ...token, createdAt }} /></svg>);
    act(() => vi.advanceTimersByTime(0)); expect(pointAtLength).toHaveBeenCalledWith(100 * 600 / 680);
    act(() => vi.advanceTimersByTime(112)); expect(container.querySelector(".kl-flow-token")).toBeNull();
  });
  it("keeps reduced-motion indicators finite and never replays after a preference change", () => {
    const { container, unmount } = render(<svg><FlowPacket {...props} motion={false} /></svg>);
    expect(container.querySelector("animateMotion")).toBeNull();
    expect(container.querySelector(".kl-flow-token")?.getAttribute("transform")).toBe("translate(50 0)");
    act(() => vi.advanceTimersByTime(680)); expect(container.querySelector(".kl-flow-token")).toBeNull();
    unmount();
    const mounted = render(<svg><FlowPacket {...props} /></svg>);
    act(() => vi.advanceTimersByTime(100));
    mounted.rerender(<svg><FlowPacket {...props} motion={false} /></svg>);
    act(() => vi.advanceTimersByTime(0));
    mounted.rerender(<svg><FlowPacket {...props} /></svg>);
    expect(mounted.container.querySelector(".kl-flow-token")).toBeNull();
  });
  it("cleans up delayed animation timers on unmount", () => {
    const { unmount } = render(<svg><FlowPacket {...props} token={{ ...token, delayMs: 760 }} /></svg>);
    unmount(); act(() => vi.advanceTimersByTime(2000)); expect(pointAtLength).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
  });
  it("moves a read backwards along an ownership relationship and finishes at the owner", () => {
    const { container } = render(<svg><FlowPacket {...props} token={{ ...token, reverse: true }} /></svg>);
    act(() => vi.advanceTimersByTime(0));
    expect(container.querySelector(".kl-flow-token")?.getAttribute("transform")).toBe("translate(100 0)");
    act(() => vi.advanceTimersByTime(160));
    expect(pointAtLength.mock.calls.at(-1)?.[0]).toBeLessThan(100);
    act(() => vi.advanceTimersByTime(520));
    expect(container.querySelector(".kl-flow-token")?.getAttribute("transform")).toBe("translate(0 0)");
    act(() => vi.advanceTimersByTime(32));
    expect(container.querySelector(".kl-flow-token")).toBeNull();
  });
});
