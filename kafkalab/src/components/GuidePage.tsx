"use client";

import { useEffect } from "react";
import Link from "next/link";
import { useKafkaLabStoreAdapter, useKafkaStore } from "../store/kafka-store";
import { kafkaQuizSources } from "../data/kafka-quiz/sources";
import { LabIcon, type LabIconName } from "./LabIcon";
import { GuideExperiments } from "./GuideExperiments";

const firstSampleValue = "order-created";

const steps: Array<{ title: string; term: string; description: string; icon: LabIconName }> = [
  { title: "Send", term: "Producer", description: "An app creates a record, like an order being placed.", icon: "send" },
  { title: "Store", term: "Topic & partitions", description: "Kafka keeps it in an ordered lane. Its offset is the position in that lane.", icon: "layers" },
  { title: "Read", term: "Consumer", description: "A worker reads the record. A team of workers shares the lanes.", icon: "worker" },
  { title: "Save", term: "Commit", description: "The worker saves the next position so it knows where to continue.", icon: "check" },
];

const learningSections = [
  {
    title: "Kafka concepts",
    description: "Use these ideas to explain what the canvas is showing when a system grows beyond one process.",
    items: [
      { question: "Why is ordering scoped to a partition?", answer: "Kafka appends records to each partition and assigns a monotonically increasing offset there. A key is normally hashed to choose a partition, so records for that key stay ordered while records in different partitions can be processed concurrently. Increasing the partition count can change the key-to-partition mapping, so a design that depends on per-key order should plan that change carefully." },
      { question: "What does a consumer group own?", answer: "A group divides partition ownership among its members. At a given moment one member reads a partition, and a rebalance moves ownership when members join, leave, or fail. Adding workers only adds parallelism while unclaimed partitions exist; it cannot make one partition run in parallel without changing the data model." },
      { question: "How do current position, committed offset, and lag differ?", answer: "The current position is where a consumer will read next. A committed offset is the durable checkpoint the group can resume from after a restart or rebalance. Lag measures the distance from the log end to the group's saved next position, so a consumer can have read a record and still show lag until it commits. Retention can move the earliest available record forward while a group is behind." },
    ],
  },
  {
    title: "Interview Q&A",
    description: "These answers connect the small in-memory example to tradeoffs that come up in production interviews.",
    items: [
      { question: "How would you choose at-least-once or exactly-once delivery?", answer: "At-least-once is the common default: retryable failures can deliver a record twice, so handlers need idempotency keys or a deduplication record. Kafka transactions can combine produced records and consumed offsets for Kafka-to-Kafka pipelines, but an external database still needs an outbox, an inbox, or another transaction boundary. Exactly-once is a system design property, not a checkbox on one consumer." },
      { question: "How do you diagnose a hot partition?", answer: "Compare per-partition throughput, consumer lag, request latency, and key frequency rather than looking only at group totals. A low-cardinality or skewed key can send most traffic to one partition while other workers sit idle. The fix may be a better routing key, a controlled key-suffix strategy, or a new topic; changing consumers alone cannot rebalance records already concentrated in one lane. Validate any key change against the ordering guarantee you need." },
      { question: "What protects ordering when a handler retries?", answer: "Keep work for a key on one partition and avoid allowing concurrent in-flight retries to overtake earlier records. Idempotent producers protect the write path, while bounded poll intervals, explicit retry topics, and a dead-letter policy keep a slow or poisoned record from blocking a group forever. The correct choice depends on whether order or throughput has priority for that stream." },
    ],
  },
  {
    title: "Common traps",
    description: "Small configuration details can make a healthy-looking graph report the wrong business progress.",
    items: [
      { question: "Auto-commit is not the same as processed", answer: "A consumer may advance its position before the handler finishes. If the process dies after that checkpoint, the record can be skipped on restart. Commit after the side effect is durable, or make the side effect idempotent and accept the replay behavior explicitly. Kafka's newer consumer group protocol changes heartbeat configuration, but it does not turn a commit into a business completion signal." },
      { question: "More consumers do not always improve throughput", answer: "A group cannot assign more than one active reader to a partition. Extra members can be useful for failover, but they also increase rebalance work and connection cost. Check partition count, handler time, fetch settings, and downstream capacity before scaling the group." },
      { question: "Partition expansion can surprise keyed workflows", answer: "When the partition count changes, the partitioner can map an existing key to a different partition. New records for that key may then follow a different lane and create a new ordering boundary. Treat expansion as a compatibility decision and validate it with the producer's partitioner." },
    ],
  },
  {
    title: "Real project situations",
    description: "A useful explanation starts with the symptom, then narrows it using partition and consumer evidence.",
    items: [
      { question: "A backlog grows during a traffic spike", answer: "First separate producer pressure from consumer pressure: compare publish rate, fetch rate, per-partition lag, handler latency, and rebalance events. If one partition is hot, adding consumers will not help; if every partition is behind, increase safe parallelism or remove downstream bottlenecks. Set a recovery objective so catch-up traffic does not overload the dependency you are trying to protect." },
      { question: "One malformed record keeps blocking a partition", answer: "Bound the retry loop and preserve the original topic, partition, offset, key, and error in a retry or dead-letter record. Alert on the quarantine path and provide a replay procedure after the data is corrected. Skipping silently loses an ordering decision, while retrying forever can make the rest of the partition unavailable." },
      { question: "A schema change must roll out without stopping producers", answer: "Use a compatibility rule that supports the reader and writer versions during the rollout, register the new schema before sending it, and deploy consumers that can read both versions. Keep defaults and field meaning explicit. A schema registry validates shape compatibility; it does not make a semantic change safe by itself." },
    ],
  },
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

  return (
    <main id="main-content" className="kl-guide-page">
      <div className="kl-guide-hero"><span className="kl-overline">A TWO-MINUTE GUIDE</span><h1>A message.<br /><span>Four simple steps.</span></h1><p>Kafka moves data between applications. This playground lets you see the journey, one connection at a time.</p><Link href="/" className="kl-button kl-button--dark">Explore the flow <LabIcon name="arrow" width="16" height="16" /></Link></div>

      <div className="kl-guide-journey" aria-label="Message journey: send, store, read, save">{steps.map((step, index) => <article key={step.title}><span className={`kl-guide-step-icon ${step.title === "Send" ? "is-red" : ""}`}><LabIcon name={step.icon} width="28" height="28" /></span><h2>{step.title}</h2><span className="kl-guide-term">{step.term}</span><p>{step.description}</p>{index < steps.length - 1 ? <LabIcon name="arrow" className="kl-guide-step-arrow" /> : null}</article>)}</div>

      <section className="kl-guide-exercise" aria-labelledby="exercise-title"><div><span className="kl-overline">TRY IT YOURSELF</span><h2 id="exercise-title">Follow your first order</h2><p>This exercise resets the workflow and turns off automatic saving so you can see each step.</p></div><ol><li><span>1</span><div><strong>Send one order</strong><small>One record appears in one partition.</small></div><button className="kl-button kl-button--red" onClick={sendExample}><LabIcon name="send" width="16" height="16" />{tutorialReady ? "Start fresh" : "Send example"}</button></li><li><span>2</span><div><strong>Let the worker read it</strong><small>Reading moves the current position. The saved position stays put.</small></div><button className="kl-button kl-button--white" onClick={() => actions.startReading?.()} disabled={!tutorialReady || read > 0}><LabIcon name={read > 0 ? "check" : "play"} width="16" height="16" />{read > 0 ? "Record read" : "Read record"}</button></li><li><span>3</span><div><strong>Save the progress</strong><small>The next reading position is saved. This is a commit.</small></div><button className="kl-button kl-button--white" onClick={() => worker && actions.commitConsumer(worker.id)} disabled={!tutorialReady || read === 0 || saved > 0}><LabIcon name="check" width="16" height="16" />{saved > 0 ? "Progress saved" : "Save progress"}</button></li></ol><div className="kl-guide-result" role="status">{tutorialReady ? <><span className="kl-tiny-dot" /><span>{saved > 0 ? "Done. You sent, read, and committed your first record." : read > 0 ? "Read, but not saved yet. Notice the two positions are different." : `Stored in partition ${tutorialRecord.partition}, at position ${tutorialRecord.offset}.`}</span><Link href="/">See it on the canvas <LabIcon name="arrow" width="16" height="16" /></Link></> : <><LabIcon name="flow" width="18" height="18" /><span>Send the example to begin.</span></>}</div></section>

      <GuideExperiments />

      <section className="kl-guide-basics" aria-labelledby="guide-basics-title"><div><span className="kl-overline">READING THE CANVAS</span><h2 id="guide-basics-title">A few things to look for</h2></div><div className="kl-guide-notes"><article><i className="kl-guide-motion-dot" /><h3>Red means activity</h3><p>Packets move when records are sent or read. A paused flow keeps its state so you can inspect it.</p></article><article><LabIcon name="layers" /><h3>Each lane has an order</h3><p>Positions start at zero in each partition. A repeated key stays on one partition while the partition count and producer partitioner stay unchanged.</p></article><article><LabIcon name="worker" /><h3>Workers share the job</h3><p>Each partition has one active owner in a group. Adding a worker redistributes the lanes.</p></article><article><LabIcon name="activity" /><h3>Processed can still await commit</h3><p>Lag follows the group’s saved position. It may include work that already succeeded, because processing and committing are separate steps.</p></article></div></section>

      <section className="kl-guide-learning" aria-labelledby="guide-learning-title"><div className="kl-guide-learning-heading"><span className="kl-overline">GO FURTHER</span><h2 id="guide-learning-title">Explain the tradeoffs</h2><p>Open a section when you want the production reasoning behind the four steps. These notes are reference material and do not change the simulator.</p></div><div className="kl-guide-disclosures">{learningSections.map((section) => <details className="kl-guide-disclosure" key={section.title}><summary>{section.title}<span aria-hidden="true">+</span></summary><div className="kl-guide-disclosure-body"><p>{section.description}</p><div className="kl-guide-disclosure-items">{section.items.map((item) => <article key={item.question}><h3>{item.question}</h3><p>{item.answer}</p></article>)}</div></div></details>)}</div></section>

      <div className="kl-guide-next"><div><h2>Ready to explore the flow?</h2><p>Return to the live chart and switch between the Flow, partition, consumer, offset, delivery, replication, and activity views.</p></div><Link className="kl-button kl-button--red" href="/">Open chart views <LabIcon name="arrow" width="16" height="16" /></Link></div>
      <p className="kl-guide-footnote">State persists locally in this browser, and a reload resumes the workflow in its paused state.</p>
      <p className="kl-guide-sources">Kafka 4.3 sources: <a href={kafkaQuizSources.design} target="_blank" rel="noreferrer">design and semantics</a> · <a href={kafkaQuizSources.topics} target="_blank" rel="noreferrer">topics and partitions</a> · <a href={kafkaQuizSources.producerConfigs} target="_blank" rel="noreferrer">producer configs</a> · <a href={kafkaQuizSources.consumerConfigs} target="_blank" rel="noreferrer">consumer configs</a> · <a href={kafkaQuizSources.topicConfigs} target="_blank" rel="noreferrer">topic configs</a></p>
    </main>
  );
}
