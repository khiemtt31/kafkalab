"use client";

import { useRef, useState, type KeyboardEvent } from "react";
import type { KafkaLabStore } from "../hooks/useKafkaLab";
import { useKafkaLab } from "../hooks/useKafkaLab";
import { useReducedMotion } from "../hooks/useReducedMotion";
import { chartQuestions, learningFocus } from "../learning/flow-learning";
import type { ExperimentId, ExperimentResult } from "../learning/experiments";
import { useKafkaStore } from "../store/kafka-store";
import { CHART_VARIANTS, type ChartVariantId } from "./chart-views";
import { GuideExperiments } from "./GuideExperiments";
import { KafkaProcessCanvas } from "./KafkaProcessCanvas";
import { LabIcon } from "./LabIcon";
import "./flow-learning.css";

export type KafkaVisualLabProps = { store?: KafkaLabStore; className?: string };

export function KafkaVisualLab({ store, className }: KafkaVisualLabProps) {
  const { snapshot, actions, flowLayout } = useKafkaLab(store);
  const reducedMotion = useReducedMotion();
  const [variant, setVariant] = useState<ChartVariantId>("flow");
  const [wholeSystem, setWholeSystem] = useState(false);
  const [selectedPartition, setSelectedPartition] = useState<number>();
  const [result, setResult] = useState<ExperimentResult>();
  const focus = learningFocus(snapshot, result, selectedPartition);
  const globalView = variant === "delivery" || variant === "activity";
  const question = chartQuestions[variant];
  // The shared runner acts on this app's store. Embedded alternate adapters render their own canvas only.
  const sharedRuntime = actions === useKafkaStore.getState().actions;
  const tabRefs = useRef<Array<HTMLButtonElement | null>>([]);

  function selectExperiment(id: ExperimentId) {
    setResult(undefined);
    setSelectedPartition(undefined);
    setWholeSystem(id === "worker-scaling");
    setVariant("flow");
  }

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
    <main id="main-content" className={`kl-workspace kl-learning-workspace ${sharedRuntime ? "" : "kl-learning-workspace--standalone"} ${className ?? ""}`}>
      <div className="kl-workspace-bar">
        <div className="kl-flow-question"><h1>{question.question}</h1><p>{question.hint}</p></div>
        <div className="kl-toolbar" aria-label="Flow controls">
          <button className="kl-icon-button" type="button" onClick={() => { actions.reset(); setResult(undefined); setSelectedPartition(undefined); }} aria-label="Reset workflow" title="Reset workflow"><LabIcon name="reset" /></button>
          <button className="kl-button kl-button--red" type="button" onClick={() => snapshot.running ? actions.pause?.() : actions.start?.()}><LabIcon name={snapshot.running ? "pause" : "play"} /><span>{snapshot.running ? "Pause flow" : "Run flow"}</span></button>
        </div>
      </div>

      <div className="kl-flow-learning-layout">
        <div className="kl-flow-visual">
          <div className="kl-flow-scope">
            {globalView ? <span>{variant === "activity" ? "Recent simulator events" : `All partitions · ${snapshot.selectedGroupId}`}</span> : <>
              <label>Follow partition <select aria-label="Follow partition" value={focus.partitionId ?? ""} onChange={(event) => setSelectedPartition(Number(event.target.value))}>{snapshot.partitions.map((lane) => <option key={lane.id} value={lane.id}>P{lane.id}</option>)}</select></label>
              <button type="button" aria-pressed={wholeSystem} onClick={() => setWholeSystem((current) => !current)}>{wholeSystem ? "Focus this path" : "Show whole system"}</button>
            </>}
          </div>
          <div className="kl-canvas-stage">
            <KafkaProcessCanvas key={`${variant}:${snapshot.selectedGroupId}:${wholeSystem}:${focus.partitionId}`} snapshot={snapshot} actions={actions} reducedMotion={reducedMotion} flowLayout={flowLayout} variant={variant} panelId={`chart-panel-${variant}`} focusPartitionId={focus.partitionId} focused={!wholeSystem && !globalView} />
          </div>
          <div className={`kl-flow-diagnosis is-${focus.tone}`} role="status" aria-live={snapshot.running ? "off" : "polite"}><LabIcon name={focus.tone === "failure" ? "activity" : "flow"} width={18} height={18} /><div><strong>{focus.title}</strong><p>{focus.detail}</p></div></div>

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
            onClick={() => selectVariant(chart.id)}
            onKeyDown={(event) => onTabKeyDown(event, index)}
          >
            <span>{chart.label}</span>
          </button>
        ))}
          </div>
        </div>
        {sharedRuntime ? <aside className="kl-flow-challenge" aria-label="Interactive Kafka challenge">
          <GuideExperiments compact initialExperiment="failure-retry-commit" onExperimentChange={selectExperiment} onResult={(next) => { setResult(next); setSelectedPartition(undefined); }} />
        </aside> : null}
      </div>
    </main>
  );
}

export { KafkaProcessCanvas } from "./KafkaProcessCanvas";
