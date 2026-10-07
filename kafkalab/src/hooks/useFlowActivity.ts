"use client";

import { useEffect, useRef, useState } from "react";
import type { KafkaLabSnapshot } from "./useKafkaLab";
import { captureOffsetChanges, CHART_ACTIVITY_EVENT_TYPES as PULSE_EVENT_TYPES } from "../components/chart-activity";
import type { VisualEvent } from "../components/flow-transit";

type Pulse = { event: VisualEvent; revision: number; createdAt: number };

/** Presentation history has its own finite lifetime and is never persisted. */
export function useFlowActivity(events: KafkaLabSnapshot["events"], revision: number, baseline: string | undefined, running: boolean, visible: boolean, reducedMotion = false, snapshot?: KafkaLabSnapshot) {
  const [pulses, setPulses] = useState<Record<string, Pulse>>({});
  const seen = useRef<Set<string> | null>(null);
  const previous = useRef({ revision, baseline, running, visible, reducedMotion });
  const previousSnapshot = useRef<KafkaLabSnapshot | null>(null);
  const timers = useRef(new Set<ReturnType<typeof setTimeout>>());

  useEffect(() => () => { timers.current.forEach(clearTimeout); timers.current.clear(); }, []);
  useEffect(() => {
    const before = previousSnapshot.current;
    previousSnapshot.current = snapshot ?? null;
    const recent = events.slice(-200);
    const changed = previous.current.revision !== revision || previous.current.baseline !== baseline;
    const interrupted = (previous.current.running && !running) || previous.current.visible !== visible || previous.current.reducedMotion !== reducedMotion;
    previous.current = { revision, baseline, running, visible, reducedMotion };
    if (changed || interrupted) {
      timers.current.forEach(clearTimeout);
      timers.current.clear();
      const clear = setTimeout(() => { timers.current.delete(clear); setPulses({}); }, 0);
      timers.current.add(clear);
      if (changed) {
        const boundary = baseline ? recent.findIndex((event) => event.id === baseline) : -1;
        // Missing bounded-history watermark means the retained events are newer.
        seen.current = new Set((baseline ? recent.slice(0, boundary + 1) : recent.filter((event) => !PULSE_EVENT_TYPES.has(event.type))).map((event) => event.id));
      } else seen.current = new Set(recent.map((event) => event.id));
    }
    if (!visible) { seen.current = new Set(recent.map((event) => event.id)); return; }
    if (!seen.current) { seen.current = new Set(recent.map((event) => event.id)); return; }
    const fresh = captureOffsetChanges(recent.filter((event) => PULSE_EVENT_TYPES.has(event.type) && !seen.current?.has(event.id)), snapshot, before);
    seen.current = new Set(recent.map((event) => event.id));
    if (!fresh.length) return;
    const createdAt = Date.now();
    const ids = new Set(recent.map((event) => event.id));
    const add = setTimeout(() => {
      timers.current.delete(add);
      setPulses((current) => {
        const next = Object.fromEntries(Object.entries(current).filter(([id]) => ids.has(id)));
        fresh.forEach((event) => { next[event.id] = { event, revision, createdAt }; });
        return next;
      });
    }, 0);
    timers.current.add(add);
    fresh.forEach((event) => {
      const expiry = setTimeout(() => {
        timers.current.delete(expiry);
        setPulses((current) => { const next = { ...current }; delete next[event.id]; return next; });
      }, 1800);
      timers.current.add(expiry);
    });
  }, [events, revision, baseline, running, visible, reducedMotion, snapshot]);

  return visible ? Object.values(pulses).filter((pulse) => pulse.revision === revision).map((pulse) => ({ ...pulse.event, visualStartedAt: pulse.createdAt, visualRevision: pulse.revision })) : [];
}
