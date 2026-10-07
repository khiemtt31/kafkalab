"use client";

import { useRef, useState, type KeyboardEvent } from "react";
import type { KafkaLabStore } from "../hooks/useKafkaLab";
import { useKafkaLab } from "../hooks/useKafkaLab";
import { useReducedMotion } from "../hooks/useReducedMotion";
import { CHART_VARIANTS, type ChartVariantId } from "./chart-views";
import { KafkaProcessCanvas } from "./KafkaProcessCanvas";
import { LabIcon } from "./LabIcon";

export type KafkaVisualLabProps = { store?: KafkaLabStore; className?: string };

export function KafkaVisualLab({ store, className }: KafkaVisualLabProps) {
  const { snapshot, actions, flowLayout } = useKafkaLab(store);
  const reducedMotion = useReducedMotion();
  const [variant, setVariant] = useState<ChartVariantId>("flow");
  const tabRefs = useRef<Array<HTMLButtonElement | null>>([]);

  const selectVariant = (next: ChartVariantId, focus = false) => {
    setVariant(next);
    if (focus) requestAnimationFrame(() => tabRefs.current[CHART_VARIANTS.findIndex((candidate) => candidate.id === next)]?.focus());
  };

  const onTabKeyDown = (event: KeyboardEvent<HTMLButtonElement>, currentIndex: number) => {
    let nextIndex: number | undefined;
    if (event.key === "ArrowRight") nextIndex = (currentIndex + 1) % CHART_VARIANTS.length;
    if (event.key === "ArrowLeft") nextIndex = (currentIndex - 1 + CHART_VARIANTS.length) % CHART_VARIANTS.length;
    if (event.key === "Home") nextIndex = 0;
    if (event.key === "End") nextIndex = CHART_VARIANTS.length - 1;
    if (nextIndex === undefined) return;
    event.preventDefault();
    selectVariant(CHART_VARIANTS[nextIndex].id, true);
  };

  return (
    <main id="main-content" className={`kl-workspace ${className ?? ""}`}>
      <div className="kl-workspace-bar">
        <h1 className="kl-sr-only">Kafka chart views</h1>
        <div className="kl-toolbar" aria-label="Flow controls">
          <button className="kl-icon-button" type="button" onClick={actions.reset} aria-label="Reset workflow" title="Reset workflow"><LabIcon name="reset" /></button>
          <button className="kl-button kl-button--red" type="button" onClick={() => snapshot.running ? actions.pause?.() : actions.start?.()}><LabIcon name={snapshot.running ? "pause" : "play"} /><span>{snapshot.running ? "Pause flow" : "Run flow"}</span></button>
        </div>
      </div>

      <div className="kl-canvas-stage">
        <KafkaProcessCanvas key={`${variant}:${snapshot.selectedGroupId}`} snapshot={snapshot} actions={actions} reducedMotion={reducedMotion} flowLayout={flowLayout} variant={variant} panelId={`chart-panel-${variant}`} />
      </div>

      <div className="kl-chart-tabs" role="tablist" aria-label="Kafka chart views">
        {CHART_VARIANTS.map((chart, index) => (
          <button
            key={chart.id}
            id={`chart-tab-${chart.id}`}
            ref={(element) => { tabRefs.current[index] = element; }}
            type="button"
            role="tab"
            aria-selected={variant === chart.id}
            aria-controls={`chart-panel-${chart.id}`}
            tabIndex={variant === chart.id ? 0 : -1}
            title={chart.description}
            onClick={() => selectVariant(chart.id)}
            onKeyDown={(event) => onTabKeyDown(event, index)}
          >
            <span>{chart.label}</span>
          </button>
        ))}
      </div>
    </main>
  );
}

export { KafkaProcessCanvas } from "./KafkaProcessCanvas";
