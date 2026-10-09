"use client";

import { useEffect, useId, useState } from "react";
import Link from "next/link";
import { experiments, runExperiment, commitExperiment, canCommitExperiment, type ExperimentId, type ExperimentResult } from "../learning/experiments";
import { useLearningProgress } from "../learning/progress";
import { useKafkaStore } from "../store/kafka-store";
import { LabIcon } from "./LabIcon";
import { LearningEvidence } from "./LearningEvidence";
import "./guide-learning.css";

type Run = ExperimentResult & { prediction: number };
type GuideExperimentsProps = {
  compact?: boolean;
  initialExperiment?: ExperimentId;
  onExperimentChange?: (id: ExperimentId) => void;
  onResult?: (result: ExperimentResult) => void;
};

const challengeCopy: Record<ExperimentId, { label: string; symptom: string; why: string; next: string }> = {
  "keyed-ordering": { label: "Keep an order in sequence", symptom: "Created → paid → shipped. Can three events lose their order?", why: "The same key selects one partition. Its offsets show the append order; other partitions keep their own sequence.", next: "Now test a crash: does a successful read also save your place?" },
  "replay-before-commit": { label: "Find the duplicate", symptom: "The handler succeeded. After a crash, the order arrives again.", why: "The saved next offset stayed at 0. Restart reads from there, so the same stored record gets a second attempt.", next: "Try saving too early. Can a healthy checkpoint hide unfinished work?" },
  "commit-before-processing": { label: "Spot the missing work", symptom: "The checkpoint advanced, but nobody finished the order.", why: "Saving next offset 1 tells the group to resume after record 0. The record remains stored, but its processing is skipped.", next: "Retry a failed record, then save only after its handler succeeds." },
  "worker-scaling": { label: "Diagnose idle workers", symptom: "Five workers, three partitions, one busy key. Why is work still waiting?", why: "A partition has one owner in a group. Two workers get no partition, and the busy key still belongs to one owner.", next: "Inspect the busy partition and its owner in Flow before deciding to add workers." },
  "failure-retry-commit": { label: "Rescue a failed order", symptom: "One failed order is holding back the saved position. What fixes it?", why: "Retry changes the attempt and outcome of the same record. Saving progress is a separate action after success.", next: "Commit the successful retry below, then compare the saved next offset." },
};

const nextChallenge: Record<ExperimentId, ExperimentId> = {
  "keyed-ordering": "replay-before-commit",
  "replay-before-commit": "commit-before-processing",
  "commit-before-processing": "failure-retry-commit",
  "worker-scaling": "failure-retry-commit",
  "failure-retry-commit": "replay-before-commit",
};

export function GuideExperiments({ compact = false, initialExperiment = "keyed-ordering", onExperimentChange, onResult }: GuideExperimentsProps) {
  const { progress, hydrated, notice, hydrate, predict, complete, reset } = useLearningProgress();
  const snapshot = useKafkaStore((store) => store.snapshot);
  const [activeId, setActiveId] = useState<ExperimentId>(initialExperiment);
  const [runs, setRuns] = useState<Partial<Record<ExperimentId, Run>>>({});
  const [error, setError] = useState<string>();
  const instanceId = useId();
  const titleId = `${instanceId}-challenge-title`;
  const lesson = experiments.find((candidate) => candidate.id === activeId)!;
  const lessonIndex = experiments.findIndex((candidate) => candidate.id === activeId);
  const copy = challengeCopy[activeId];
  const entry = progress[activeId];
  const result = runs[activeId];
  const completed = experiments.filter((candidate) => progress[candidate.id]?.completed).length;
  const commitAllowed = result ? canCommitExperiment(result, snapshot) : false;
  const stale = result && (result.revision !== (snapshot.resetRevision ?? 0) || result.after.group !== snapshot.selectedGroupId);
  useEffect(() => hydrate(), [hydrate]);

  function choose(id: ExperimentId) {
    setActiveId(id);
    setError(undefined);
    onExperimentChange?.(id);
  }

  function run() {
    const prediction = entry?.prediction;
    if (prediction === undefined) return;
    try {
      const captured = runExperiment(activeId);
      setRuns((current) => ({ ...current, [activeId]: { ...captured, prediction } }));
      complete(activeId);
      setError(undefined);
      onResult?.(captured);
    } catch {
      setError("The challenge could not finish. Run it again to start fresh.");
    }
  }

  function commit() {
    if (!result || !commitAllowed) return;
    const captured = commitExperiment(result);
    setRuns((current) => ({ ...current, [activeId]: { ...captured, prediction: result.prediction } }));
    onResult?.(captured);
  }

  return <section className={`kl-learning-workbench${compact ? " kl-learning-workbench--compact" : ""}`} aria-labelledby={titleId}>
    <header className="kl-learning-heading">
      <span className="kl-overline">{compact ? "LEARNING CHALLENGE" : "CHANGE ONE THING. FOLLOW THE RESULT."}</span>
      <h2 id={titleId}>{compact ? "Predict what happens" : "What would happen if…?"}</h2>
    </header>
    <div className="kl-learning-progress">
      <span>{completed} / {experiments.length} challenges explored</span>
      <progress max={experiments.length} value={completed} aria-label="Challenges explored" />
    </div>
    {compact ? <label className="kl-learning-select">Choose a challenge
      <select value={activeId} onChange={(event) => choose(event.target.value as ExperimentId)}>
        {experiments.map((candidate, index) => <option key={candidate.id} value={candidate.id}>{index + 1}. {challengeCopy[candidate.id].label}{progress[candidate.id]?.completed ? " ✓" : ""}</option>)}
      </select>
    </label> : <nav className="kl-learning-chooser" aria-label="Choose a learning challenge">
      {experiments.map((candidate, index) => <button type="button" key={candidate.id} aria-pressed={activeId === candidate.id} onClick={() => choose(candidate.id)}>
        <span className="kl-learning-choice-number" aria-hidden="true">{progress[candidate.id]?.completed ? <LabIcon name="check" width="16" height="16" /> : index + 1}</span>
        <span>{challengeCopy[candidate.id].label}</span>
        {progress[candidate.id]?.completed ? <span className="kl-learning-sr-only">Explored</span> : null}
      </button>)}
    </nav>}
    {notice ? <p className="kl-learning-notice" role="status">{notice}</p> : null}
    {error ? <p className="kl-learning-notice" role="alert">{error}</p> : null}
    <article className="kl-learning-challenge" data-experiment-id={activeId}>
      <div className="kl-learning-problem"><span>CHALLENGE {lessonIndex + 1}</span><h3>{copy.symptom}</h3></div>
      <fieldset className="kl-learning-prediction">
        <legend>{lesson.question}</legend>
        {lesson.choices.map((choice, index) => <label key={choice} className={entry?.prediction === index ? "is-selected" : undefined}>
          <input type="radio" name={`${instanceId}-prediction-${activeId}`} value={index} checked={entry?.prediction === index}
            disabled={!hydrated} onChange={() => predict(activeId, index)} />
          <span>{choice}</span>
        </label>)}
      </fieldset>
      <div className="kl-learning-run">
        <button type="button" className="kl-button kl-button--red" disabled={!hydrated || entry?.prediction === undefined} onClick={run}>
          <LabIcon name="play" width="16" height="16" />{result ? "Run again" : "Run challenge"}
        </button>
        <p>Run replaces the current workflow with this paused challenge. Practice answers are kept.</p>
      </div>
      {result ? <div className="kl-learning-result" role="region" aria-label={`${lesson.title} captured evidence`}>
        {stale ? <p className="kl-learning-notice kl-learning-stale">The live workflow has changed. These are captured results from your earlier run.</p> : null}
        <div className={`kl-learning-feedback${result.prediction === lesson.correct ? " is-correct" : ""}`} role="status">
          <strong>{result.prediction === lesson.correct ? "Your prediction matches the evidence." : "The evidence tells a different story."}</strong>
          {result.prediction !== lesson.correct ? <p>You chose: {lesson.choices[result.prediction]}. Observe: {lesson.choices[lesson.correct]}.</p> : null}
          {entry?.prediction !== result.prediction ? <p>Prediction updated. Run again to test it.</p> : null}
        </div>
        <LearningEvidence result={result} compact={compact} />
        {activeId === "failure-retry-commit" ? <div className="kl-learning-commit">
          <button type="button" className="kl-button kl-button--dark" disabled={!commitAllowed} onClick={commit}>
            <LabIcon name="check" width="16" height="16" />{result.committed ? "Progress committed" : "Commit successful retry"}
          </button>
          {!result.committed && !commitAllowed ? <p>The live workflow has changed. Run this challenge again before committing.</p> : null}
        </div> : null}
        <div className="kl-learning-why"><h4>Why it happened</h4><p>{copy.why}</p></div>
        <div className="kl-learning-next-step"><strong>Try next</strong><p>{result.committed ? "The saved next offset moved. Test a crash before saving to discover why records replay." : copy.next}</p>
          {activeId !== "failure-retry-commit" || result.committed ? <button type="button" className="kl-learning-text-button" onClick={() => choose(nextChallenge[activeId])}>Next challenge <LabIcon name="arrow" width="14" height="14" /></button> : null}
          {!compact ? <Link href="/">Inspect current Flow <LabIcon name="arrow" width="14" height="14" /></Link> : null}
        </div>
        <details className="kl-learning-reference"><summary>Interview follow-up & model limits</summary><p>{lesson.followUp}</p><p>{lesson.boundary}</p></details>
      </div> : <p className="kl-learning-prompt">{entry?.completed ? "You explored this challenge earlier. Run it again for fresh evidence." : "Choose your prediction, then watch the change."}</p>}
    </article>
    <button type="button" className="kl-learning-text-button kl-learning-reset" onClick={() => { reset(); setRuns({}); }}>Clear lesson progress</button>
  </section>;
}
