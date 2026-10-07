import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useFlowActivity } from "@/hooks/useFlowActivity";
import type { LabTimelineEvent } from "@/hooks/useKafkaLab";

const event = (id: string): LabTimelineEvent => ({ id, timestamp: 0, type: "message-produced", summary: id, concept: "append" });
type Props = { events: LabTimelineEvent[]; revision: number; baseline?: string; running: boolean; visible: boolean };
const initial: Props = { events: [event("seed")], revision: 0, baseline: "seed", running: false, visible: true };
const flush = () => act(() => vi.advanceTimersByTime(0));
describe("transient event activity", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => { cleanup(); vi.useRealTimers(); });
  it("never replays initial history, but animates a fresh paused send with a finite birth time", () => {
    const { result, rerender } = renderHook((p: Props) => useFlowActivity(p.events, p.revision, p.baseline, p.running, p.visible), { initialProps: initial });
    flush(); expect(result.current).toEqual([]);
    rerender({ ...initial, events: [...initial.events, event("fresh")] }); flush();
    expect(result.current).toHaveLength(1); expect(result.current[0].visualStartedAt).toBe(Date.now());
    act(() => vi.advanceTimersByTime(1800)); expect(result.current).toEqual([]);
  });
  it("drops pre-pause packets and permits a subsequent manual send", () => {
    const { result, rerender } = renderHook((p: Props) => useFlowActivity(p.events, p.revision, p.baseline, p.running, p.visible), { initialProps: { ...initial, running: true } });
    const events = [...initial.events, event("running")]; rerender({ ...initial, events, running: true }); flush();
    expect(result.current).toHaveLength(1);
    rerender({ ...initial, events }); flush(); expect(result.current).toEqual([]);
    rerender({ ...initial, events: [...events, event("manual")] }); flush(); expect(result.current.map((e) => e.id)).toEqual(["manual"]);
  });
  it("invalidates pending and active packets while hidden without replay on return", () => {
    const { result, rerender } = renderHook((p: Props) => useFlowActivity(p.events, p.revision, p.baseline, p.running, p.visible), { initialProps: initial });
    const events = [...initial.events, event("pending")]; rerender({ ...initial, events });
    rerender({ ...initial, events, visible: false }); flush(); expect(result.current).toEqual([]);
    const hiddenEvents = [...events, event("hidden")]; rerender({ ...initial, events: hiddenEvents, visible: false });
    rerender({ ...initial, events: hiddenEvents }); flush(); expect(result.current).toEqual([]);
    rerender({ ...initial, events: [...hiddenEvents, event("returned")] }); flush(); expect(result.current.map((e) => e.id)).toEqual(["returned"]);
  });
  it("suppresses replacement seed history while preserving a batched reset and new send", () => {
    const { result, rerender } = renderHook((p: Props) => useFlowActivity(p.events, p.revision, p.baseline, p.running, p.visible), { initialProps: initial });
    const replacement = [event("event-0"), event("event-1"), event("event-2")];
    rerender({ ...initial, revision: 1, baseline: "event-1", events: replacement }); flush();
    expect(result.current.map((e) => e.id)).toEqual(["event-2"]);
    rerender({ ...initial, revision: 2, baseline: "event-1", events: replacement.slice(0, 2) }); flush(); expect(result.current).toEqual([]);
  });
  it("uses bounded newer history when the replacement baseline was already trimmed", () => {
    const { result, rerender } = renderHook((p: Props) => useFlowActivity(p.events, p.revision, p.baseline, p.running, p.visible), { initialProps: initial });
    rerender({ ...initial, revision: 1, baseline: "trimmed", events: [event("newer")] }); flush(); expect(result.current.map((e) => e.id)).toEqual(["newer"]);
  });
  it("tracks processing, retry, skip, rebalance and broker transitions with the same finite lifetime", () => {
    const { result, rerender } = renderHook((p: Props) => useFlowActivity(p.events, p.revision, p.baseline, p.running, p.visible), { initialProps: initial });
    const fresh = ["record-processed", "record-processing-failed", "record-processing-retried", "lost-delivery", "assignment-created", "broker-failed"].map((type) => ({ ...event(type), type }));
    rerender({ ...initial, events: [...initial.events, ...fresh] }); flush();
    expect(result.current.map((event) => event.type)).toEqual(fresh.map((event) => event.type));
    act(() => vi.advanceTimersByTime(1800)); expect(result.current).toEqual([]);
  });
  it("invalidates interrupted activity on reduced-motion changes without replaying it", () => {
    type MotionProps = Props & { reducedMotion: boolean };
    const { result, rerender } = renderHook((p: MotionProps) => useFlowActivity(p.events, p.revision, p.baseline, p.running, p.visible, p.reducedMotion), { initialProps: { ...initial, reducedMotion: false } });
    const events = [...initial.events, event("fresh-motion")];
    rerender({ ...initial, events, reducedMotion: false }); flush(); expect(result.current).toHaveLength(1);
    rerender({ ...initial, events, reducedMotion: true }); flush(); expect(result.current).toEqual([]);
    rerender({ ...initial, events, reducedMotion: false }); flush(); expect(result.current).toEqual([]);
    rerender({ ...initial, events: [...events, event("after-motion")], reducedMotion: false }); flush();
    expect(result.current.map((event) => event.id)).toEqual(["after-motion"]);
  });
});
