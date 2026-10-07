"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { experiments, runExperiment, commitExperiment, canCommitExperiment, type ExperimentId, type ExperimentResult, type Evidence } from "../learning/experiments";
import { useLearningProgress } from "../learning/progress";
import { useKafkaStore } from "../store/kafka-store";
import { LabIcon } from "./LabIcon";

type Run = ExperimentResult & { prediction: number };

function evidenceRows(id: ExperimentId, evidence: Evidence, partition?: number) {
  const lane = evidence.positions.find((candidate) => candidate.partition === (partition ?? evidence.records[0]?.partition)) ?? evidence.positions[0];
  const record = evidence.records[0];
  if (id === "worker-scaling") return [
    ["Partitions", evidence.partitions], ["Consumers", evidence.workers], ["Assigned members", evidence.assigned],
    ["Idle members", evidence.idle], ["Hot partition", record ? `P${record.partition}` : "—"], ["Hot partition owner", lane?.owner ?? "—"],
    ["Assignment map", evidence.positions.map((position) => `P${position.partition} → ${position.owner}`).join("; ")],
    ["Idle member IDs", evidence.idleIds.join(", ") || "none"],
  ];
  const rows: (string | number)[][] = [["Stored records", evidence.stored], ["Read position P", lane?.read ?? 0], ["Committed next offset C", lane?.committed ?? 0], ["Log end LEO", lane?.end ?? 0]];
  if (id === "keyed-ordering") rows.push(["Partition-local offsets", evidence.records.map((message) => `P${message.partition}:${message.offset}`).join(", ") || "empty"]);
  else if (id === "commit-before-processing") rows.push(["Successful outcomes", evidence.processed], ["Skipped outcomes", evidence.skipped], ["Read after restart", evidence.recordsReadAfterRestart ?? "not restarted"]);
  else rows.push(["Record identity", record?.id ?? "—"], ["Delivery attempt", record?.attempt ?? 0], ["Application outcome", record?.outcome ?? "not read"]);
  if (id === "replay-before-commit") rows.push(["Replay events", evidence.duplicateEvents]);
  if (id === "failure-retry-commit") rows.push(["Failure reason", record?.error ?? "none"]);
  return rows;
}

export function GuideExperiments() {
  const { progress, hydrated, notice, hydrate, predict, complete, reset } = useLearningProgress();
  const snapshot = useKafkaStore((store) => store.snapshot);
  const [runs, setRuns] = useState<Partial<Record<ExperimentId, Run>>>({});
  const [error, setError] = useState<string>();
  useEffect(() => hydrate(), [hydrate]);
  const completed = experiments.filter((lesson) => progress[lesson.id]?.completed).length;

  function run(id: ExperimentId) {
    const prediction = progress[id]?.prediction;
    if (prediction === undefined) return;
    try {
      const result = runExperiment(id);
      setRuns((current) => ({ ...current, [id]: { ...result, prediction } }));
      complete(id); setError(undefined);
    } catch { setError("The experiment could not finish. Reload this page and try again."); }
  }

  return <section className="kl-guide-experiments" id="guided-experiments" aria-labelledby="guide-experiments-title">
    <div className="kl-guide-learning-heading"><span className="kl-overline">LEARN BY CHANGING THE SYSTEM</span>
      <h2 id="guide-experiments-title">Predict it. Run it. Explain it.</h2>
      <p>Five small experiments connect the chart to interview reasoning. Choose what you expect, then compare the actual state changes.</p>
    </div>
    <div className="kl-experiment-progress"><span>{completed} / 5 experiments run</span>
      <button type="button" className="kl-text-button" onClick={() => { reset(); setRuns({}); }}>Clear learning progress</button></div>
    {notice ? <p className="kl-experiment-notice" role="status">{notice}</p> : null}
    {error ? <p role="alert">{error}</p> : null}
    <div className="kl-guide-disclosures">{experiments.map((lesson) => {
      const entry = progress[lesson.id]; const result = runs[lesson.id];
      const before = result ? evidenceRows(lesson.id, result.before, result.partition) : [];
      const after = result ? evidenceRows(lesson.id, result.after, result.partition) : [];
      const committed = result?.committed ? evidenceRows(lesson.id, result.committed, result.partition) : undefined;
      return <details className="kl-guide-disclosure kl-experiment" key={lesson.id} data-experiment-id={lesson.id}>
        <summary>{lesson.title}<small>{entry?.completed ? "Completed" : "Try experiment"}</small></summary>
        <div className="kl-guide-disclosure-body">
          <fieldset className="kl-experiment-prediction"><legend><span>Predict</span>{lesson.question}</legend>
            {lesson.choices.map((choice, index) => <label key={choice}>
              <input type="radio" name={`prediction-${lesson.id}`} value={index} checked={entry?.prediction === index}
                disabled={!hydrated} onChange={() => predict(lesson.id, index)} /><span>{choice}</span>
            </label>)}
          </fieldset>
          <div className="kl-experiment-run"><button type="button" className="kl-button kl-button--red" disabled={entry?.prediction === undefined}
            onClick={() => run(lesson.id)}><LabIcon name="play" width="16" height="16" />{result ? "Run again" : "Run experiment"}</button>
            <p>Run replaces your simulator workflow with this paused experiment. Practice answers are kept.</p></div>
          {result ? <div className="kl-experiment-result" role="region" aria-label={`${lesson.title} captured evidence`}>
            <p className="kl-experiment-feedback" role="status">{result.prediction === lesson.correct ? "Your prediction matches this run." : `This run changes the prediction: ${lesson.choices[lesson.correct]}.`}</p>
            <p className="kl-experiment-scroll-hint" id={`comparison-hint-${lesson.id}`}>Swipe table to compare columns →</p>
            <div className="kl-experiment-evidence" tabIndex={0} role="region" aria-label={`${lesson.title} comparison table`} aria-describedby={`comparison-hint-${lesson.id}`}><table><caption>Observe · captured evidence from this run · group {result.after.group}</caption>
              <thead><tr><th scope="col">State</th><th scope="col">Before</th><th scope="col">After{committed ? " retry" : ""}</th>{committed ? <th scope="col">After commit</th> : null}</tr></thead>
              <tbody>{before.map(([label, value], index) => <tr key={label}><th scope="row">{label}</th><td>{value}</td><td>{after[index]?.[1]}</td>{committed ? <td>{committed[index]?.[1]}</td> : null}</tr>)}</tbody>
            </table></div>
            <ul className="kl-experiment-records" aria-label="Stored records captured after the experiment">{result.after.records.map((record) => <li key={record.id}>
              <span>P{record.partition}:{record.offset}</span><strong>{record.value}</strong><small>{record.key ?? "unkeyed"} · {record.id}</small>
            </li>)}</ul>
            {lesson.id === "failure-retry-commit" ? <div className="kl-experiment-commit">
              <button type="button" className="kl-button kl-button--dark" disabled={!canCommitExperiment(result, snapshot)}
                onClick={() => setRuns((current) => ({ ...current, [lesson.id]: { ...commitExperiment(result), prediction: result.prediction } }))}>
                {result.committed ? "Progress committed" : "Commit successful retry"}</button>
              {!result.committed && !canCommitExperiment(result, snapshot) ? <p>The live workflow has changed. Run this experiment again before committing.</p> : null}
            </div> : null}
            <div className="kl-experiment-explain"><h3>Explain</h3><p>{lesson.explanation}</p><h3>Interview follow-up</h3><p>{lesson.followUp}</p></div>
            <details className="kl-experiment-boundary"><summary>What this experiment models</summary><p>{lesson.boundary}</p>
              <a href="https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html" target="_blank" rel="noreferrer">Kafka consumer positions and groups</a></details>
            <div className="kl-experiment-links"><Link href="/">View current Flow <LabIcon name="arrow" width="14" height="14" /></Link></div>
          </div> : entry?.completed ? <p className="kl-experiment-notice">Your prediction and completion are saved. Run again to capture new evidence.</p> : null}
        </div>
      </details>;
    })}</div>
    <p className="kl-experiment-scope">Completed means you ran the local experiment. Deep production study also needs a real client, broker failures, operational metrics, and an application whose side effects you can verify.</p>
  </section>;
}
