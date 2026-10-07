"use client";

import { useEffect, useRef, useState } from "react";
import type { ChartTransitToken } from "./flow-transit";

/** A finite visual transit. Its clock belongs to the event, never the SVG page. */
export function FlowPacket({ token, path, motion, staticX, staticY }: {
  token: ChartTransitToken; path: string; motion: boolean; staticX: number; staticY: number;
}) {
  const packetRef = useRef<SVGGElement>(null);
  const geometryRef = useRef<{ path: SVGPathElement; length: number }>(null);
  const frameRef = useRef<number | undefined>(undefined);
  const [born] = useState(() => token.createdAt ?? Date.now());
  const [initialMotion] = useState(motion);
  const { delayMs, durationMs } = token;
  const [phase, setPhase] = useState<"waiting" | "active" | "done">(() =>
    Date.now() - born >= delayMs + durationMs ? "done" : initialMotion ? "waiting" : "active");

  useEffect(() => {
    const geometry = document.createElementNS("http://www.w3.org/2000/svg", "path");
    geometry.setAttribute("d", path);
    geometryRef.current = { path: geometry, length: geometry.getTotalLength() };
  }, [path]);

  useEffect(() => {
    const age = Math.max(0, Date.now() - born);
    let finishing = false;
    let settle: ReturnType<typeof setTimeout> | undefined;
    const finish = () => {
      if (finishing) return;
      finishing = true;
      if (frameRef.current !== undefined) cancelAnimationFrame(frameRef.current);
      frameRef.current = undefined;
      const geometry = geometryRef.current;
      const packet = packetRef.current;
      if (!initialMotion || !packet || !geometry) { setPhase("done"); return; }
      const point = geometry.path.getPointAtLength(token.reverse ? 0 : geometry.length);
      packet.setAttribute("transform", `translate(${point.x} ${point.y})`);
      const remove = () => {
        if (frameRef.current !== undefined) cancelAnimationFrame(frameRef.current);
        frameRef.current = undefined;
        if (settle !== undefined) clearTimeout(settle);
        setPhase("done");
      };
      // Keep the exact destination for one painted frame, then remove it.
      // The fallback also bounds cleanup if the browser stops serving frames.
      frameRef.current = requestAnimationFrame(() => { frameRef.current = requestAnimationFrame(remove); });
      settle = setTimeout(remove, 64);
    };
    const deadline = setTimeout(finish, Math.max(0, delayMs + durationMs - age));
    let start: ReturnType<typeof setTimeout> | undefined;
    if (initialMotion && age < delayMs + durationMs) {
      start = setTimeout(() => {
        const paint = () => {
          const elapsed = Math.max(0, Date.now() - born - delayMs);
          const geometry = geometryRef.current;
          const packet = packetRef.current;
          if (!packet || !geometry || elapsed >= durationMs) { finish(); return false; }
          const distance = geometry.length * elapsed / durationMs;
          const point = geometry.path.getPointAtLength(token.reverse ? geometry.length - distance : distance);
          packet.setAttribute("transform", `translate(${point.x} ${point.y})`);
          frameRef.current = requestAnimationFrame(paint);
          return true;
        };
        // Paint the real edge geometry from this event's clock. Updating a node
        // path changes the geometry without restarting the packet's lifetime.
        if (paint()) setPhase("active");
      }, Math.max(0, delayMs - age));
    }
    return () => {
      clearTimeout(deadline);
      if (settle !== undefined) clearTimeout(settle);
      if (start !== undefined) clearTimeout(start);
      if (frameRef.current !== undefined) cancelAnimationFrame(frameRef.current);
      frameRef.current = undefined;
    };
  }, [born, delayMs, durationMs, initialMotion, token.reverse]);

  useEffect(() => {
    if (!initialMotion || motion) return;
    if (frameRef.current !== undefined) cancelAnimationFrame(frameRef.current);
    frameRef.current = undefined;
    const cancel = setTimeout(() => setPhase("done"), 0);
    return () => clearTimeout(cancel);
  }, [initialMotion, motion]);

  if (phase === "done" || (initialMotion && !motion)) return null;
  const labelWidth = Math.max(42, token.label.length * 5.8 + 12);
  const description = `${token.label}; ${token.phase}${token.messageId ? `; record ${token.messageId}` : ""}${token.partition !== undefined ? `; partition ${token.partition}` : ""}${token.offset !== undefined ? `; offset ${token.offset}` : ""}${token.attempt !== undefined ? `; attempt ${token.attempt}` : ""}${token.producerId ? `; producer ${token.producerId}` : ""}${token.consumerId ? `; consumer ${token.consumerId}` : ""}`;
  return <g ref={packetRef} className="kl-flow-token" data-token-id={token.id} data-source-event-id={token.sourceEventId}
    data-message-id={token.messageId} data-partition={token.partition} data-offset={token.offset}
    data-phase={token.phase} data-attempt={token.attempt} data-producer-id={token.producerId}
    data-consumer-id={token.consumerId} data-group-id={token.groupId} data-edge-id={token.edgeId}
    data-previous-offset={token.previousOffset} data-current-offset={token.currentOffset}
    data-transit-state={phase} role="img" aria-label={description} aria-hidden={phase === "waiting"}
    opacity={phase === "waiting" ? 0 : 1} pointerEvents="none"
    transform={initialMotion ? undefined : `translate(${staticX} ${staticY})`}>
    <rect className="kl-flow-token__label" x={-5} y={-14} width={labelWidth} height={17} rx={3} />
    <circle className="kl-flow-token__dot" cx={0} cy={0} r={3.5} />
    <text x={2} y={-2}>{token.label}</text>
  </g>;
}
