"use client";

import { useEffect } from "react";
import Link from "next/link";
import { useKafkaLabStoreAdapter, useKafkaStore } from "../store/kafka-store";
import { kafkaQuizSources } from "../data/kafka-quiz/sources";
import { LabIcon } from "./LabIcon";
import { GuideExperiments } from "./GuideExperiments";

const firstSampleValue = "order-created";
const learningSections = [
  { title: "Kafka concepts", items: [
    { question: "Why is ordering scoped to a partition?", answer: "Each partition has its own offsets. With an unchanged partition count and partitioner, the same key keeps selecting the same ordered lane. Different lanes can progress independently." },
    { question: "What does a consumer group own?", answer: "A group assigns each partition to one member at a time. Joining, leaving, or failing can move ownership in a rebalance. Extra members remain idle once every partition has an owner." },
    { question: "Read position, saved offset, and lag?", answer: "Read position is the next record to fetch. The saved offset is where the group resumes after restart. This lab's lag is the distance from that saved offset to the log end, so successful unsaved work can still count as lag." },
  ] },
  { title: "Interview Q&A", items: [
    { question: "How do you make replay safe?", answer: "Use a durable idempotency key or inbox entry in the same transaction as the side effect. Producer idempotence does not deduplicate consumer effects. Kafka transactions can couple Kafka outputs and offsets; an external database needs its own transaction boundary." },
    { question: "How do you diagnose a hot partition?", answer: "Compare lag and key frequency per partition, handler latency, and downstream limits. More workers cannot split an existing partition. A routing-key change may help parallelism while changing the ordering guarantee." },
  ] },
  { title: "Common traps", items: [
    { question: "Does a commit prove success?", answer: "No. A checkpoint can advance before the handler finishes. Save after the required side effect is durable, and make the crash-before-save replay window safe." },
    { question: "Can partition expansion change key routing?", answer: "Yes. A new partition count can send later records for a key to another lane. Plan the migration and validate the producer's partitioner when order matters." },
  ] },
  { title: "Real project situations", items: [
    { question: "A malformed record blocks the lane", answer: "Bound retries. Keep the original topic, partition, offset, key, and error when quarantining a record. Alert on that path and define replay after correction. Decide explicitly whether order or availability wins." },
    { question: "Backlog grows during a traffic spike", answer: "Separate publish pressure from slow handling. Check per-partition lag, processing latency, and rebalances. Remove the bottleneck or increase safe parallelism without overwhelming downstream services." },
  ] },
] as const;

export function GuidePage() {
  const { snapshot, actions } = useKafkaLabStoreAdapter();
  const worker = snapshot.consumers[0];
  const tutorialRecord = snapshot.partitions.flatMap((partition) => partition.messages).find((message) => message.key === "order-7F3K9" && message.value === firstSampleValue);
  const tutorialReady = !!tutorialRecord && snapshot.messageCount === 1 && snapshot.consumers.length === 1 && worker?.autoCommit === false;
  const read = tutorialReady ? snapshot.partitions.reduce((total, partition) => total + (partition.currentOffset ?? 0), 0) : 0;
  const saved = tutorialReady ? snapshot.partitions.reduce((total, partition) => total + (partition.committedOffset ?? 0), 0) : 0;

  useEffect(() => {
    if (tutorialReady && snapshot.running && !snapshot.producerRunning && read > 0) actions.pause?.();
  }, [tutorialReady, snapshot.running, snapshot.producerRunning, read, actions]);

  function sendExample() {
    actions.reset();
    const consumer = useKafkaStore.getState().snapshot.consumers[0];
    if (consumer) actions.setConsumerAutoCommit?.(consumer.id, false);
    actions.produce({ key: "order-7F3K9", value: firstSampleValue });
  }

  return <main id="main-content" className="kl-guide-page kl-learning-guide">
    <header className="kl-learning-guide-intro"><span className="kl-overline">GUIDE / LEARN BY DOING</span><h1>Make the failure make sense.</h1><p>Predict a change. Run it. Trace the record, the worker, and the saved position.</p></header>
    <GuideExperiments />
    <section className="kl-first-record" aria-labelledby="exercise-title">
      <div className="kl-first-record-heading"><span className="kl-overline">NEW TO KAFKA?</span><h2 id="exercise-title">Move your first order</h2><p>Send example replaces the current workflow with one record and automatic saving off.</p></div>
      <ol className="kl-first-record-steps">
        <li data-complete={tutorialReady}><span className="kl-first-record-number">1</span><strong>Send → store</strong><p>An order enters one partition.</p><button type="button" className="kl-button kl-button--red" onClick={sendExample}><LabIcon name="send" width="16" height="16" />{tutorialReady ? "Start fresh" : "Send example"}</button></li>
        <li data-complete={read > 0}><span className="kl-first-record-number">2</span><strong>Read → process</strong><p>Read next moves. Saved next stays.</p><button type="button" className="kl-button kl-button--white" onClick={() => actions.startReading?.()} disabled={!tutorialReady || read > 0}><LabIcon name={read > 0 ? "check" : "play"} width="16" height="16" />{read > 0 ? "Record read" : "Read record"}</button></li>
        <li data-complete={saved > 0}><span className="kl-first-record-number">3</span><strong>Save → resume safely</strong><p>Commit the next reading position.</p><button type="button" className="kl-button kl-button--white" onClick={() => worker && actions.commitConsumer(worker.id)} disabled={!tutorialReady || read === 0 || saved > 0}><LabIcon name="check" width="16" height="16" />{saved > 0 ? "Progress saved" : "Save progress"}</button></li>
      </ol>
      {tutorialReady ? <dl className="kl-first-record-evidence"><div><dt>Stored record</dt><dd>P{tutorialRecord.partition}:{tutorialRecord.offset}</dd></div><div><dt>Read next</dt><dd>{read}</dd></div><div><dt>Saved next</dt><dd>{saved}</dd></div></dl> : null}
      <div className="kl-first-record-status" role="status">{tutorialReady ? <><span>{saved > 0 ? "Done. You sent, read, and committed your first record." : read > 0 ? "Read, but not saved yet. Notice the two positions are different." : `Stored in partition ${tutorialRecord.partition}, at position ${tutorialRecord.offset}.`}</span><Link href="/">See it on the canvas <LabIcon name="arrow" width="14" height="14" /></Link></> : <span>Send the example to begin.</span>}</div>
    </section>
    <section className="kl-learning-reading" aria-labelledby="guide-reference-title"><h2 id="guide-reference-title">Keep these answers nearby</h2><div className="kl-learning-reading-sections">{learningSections.map((section) => <details className="kl-learning-reference" key={section.title}><summary>{section.title}</summary><div className="kl-learning-reading-items">{section.items.map((item) => <article key={item.question}><h3>{item.question}</h3><p>{item.answer}</p></article>)}</div></details>)}</div>
      <p className="kl-learning-sources">Kafka reference: <a href={kafkaQuizSources.design} target="_blank" rel="noreferrer">design & semantics</a> · <a href={kafkaQuizSources.topics} target="_blank" rel="noreferrer">partitions</a> · <a href={kafkaQuizSources.consumerConfigs} target="_blank" rel="noreferrer">consumer configuration</a></p>
    </section>
    <div className="kl-learning-practice-link"><span>Test the reasoning with a new question.</span><Link href="/practice" className="kl-button kl-button--dark">Open Practice <LabIcon name="arrow" width="16" height="16" /></Link></div>
  </main>;
}
