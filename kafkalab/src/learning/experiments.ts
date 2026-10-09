import { useKafkaStore } from "../store/kafka-store";
import type { KafkaLabSnapshot } from "../hooks/useKafkaLab";

export const experiments = [
  { id: "keyed-ordering", title: "Same key, ordered lane", question: "Three events share an order key. Where do they land?",
    choices: ["One partition, offsets 0, 1, 2", "One new partition per event", "One global sequence across the topic"], correct: 0,
    explanation: "With the same partition count and partitioner, this key selects one partition. Its offsets describe append order in that lane. Another partition has its own offsets and may progress independently.",
    followUp: "If you increase the partition count, can new events for this key move? Yes: its partition mapping can change. Plan migration when per-key ordering matters.",
    boundary: "This lab uses a stable FNV teaching hash, not every Kafka client partitioner. The drawing illustrates an append path; real clients select a partition and communicate with its broker leader." },
  { id: "replay-before-commit", title: "Success, crash, replay", question: "A handler succeeds, then crashes before committing. What can happen on restart?",
    choices: ["Kafka deletes the successful record", "The same record can be delivered again", "A new topic offset is created"], correct: 1,
    explanation: "Restart resumes from the committed next offset. The previous successful attempt did not move that checkpoint, so the same stored record is delivered again with a later attempt.",
    followUp: "How do you protect a payment handler? Use a durable idempotency key or inbox record in the side-effect transaction. A producer's idempotence alone does not deduplicate consumer side effects.",
    boundary: "Crash and rebalance are synchronous local actions. No payment or external database is executed, and this experiment does not prove external exactly-once effects." },
  { id: "commit-before-processing", title: "Save too early, skip work", question: "A worker saves the next offset before processing, then restarts. What happens?",
    choices: ["The commit proves processing succeeded", "The topic record is deleted", "The group can skip work while the record stays stored"], correct: 2,
    explanation: "A checkpoint tells the group where to resume. Saving position 1 before processing offset 0 can skip that application work after a restart. The stored record remains in the topic.",
    followUp: "When should you commit? Usually after the required side effect succeeds. A crash between the side effect and commit then creates a replay window, so the handler must tolerate duplicates.",
    boundary: "This is the explicit commit-too-soon lesson. The lab separately guards commits across failed attempts; real Kafka clients can commit past failed or unfinished work. Retention can eventually remove stored records." },
  { id: "worker-scaling", title: "More workers, one hot partition", question: "Five consumers share three partitions holding one hot key. Can all five read that key in parallel?",
    choices: ["Yes, each worker reads a fifth of the key", "No: two members are idle, and the hot partition has one owner", "Kafka moves old records to balance all five workers"], correct: 1,
    explanation: "Within one group, each partition has one owner. Five members sharing three partitions leave two idle. Adding members does not redistribute stored records or split the hot partition.",
    followUp: "What would you measure before scaling? Per-partition lag, key distribution, handler latency, and downstream limits. A key or partition change may increase parallelism while changing the ordering boundary.",
    boundary: "Ownership is a deterministic local assignment. This experiment neither measures production throughput nor executes Kafka's distributed rebalance protocol." },
  { id: "failure-retry-commit", title: "Retry the record, then save", question: "A failed record is retried with manual commits. What should change first?",
    choices: ["Its attempt and processing outcome, with the same stored identity", "Its stored offset and message identity", "Its committed offset automatically"], correct: 0,
    explanation: "A retry is another delivery attempt for the same stored record. A successful retry changes its application outcome; the separate commit step advances the group's checkpoint.",
    followUp: "What if retries keep failing? Bound the retry policy and decide whether to block the lane or quarantine the record with its original identity. Retry topics and dead-letter paths can trade strict order for availability.",
    boundary: "The handler failure and success are local outcomes. The lab guards failed-work commit gaps, but does not execute an external handler, real retry topic, or dead-letter integration." },
] as const;

export type ExperimentId = typeof experiments[number]["id"];
export type Evidence = ReturnType<typeof capture>;
export type ExperimentResult = { id: ExperimentId; before: Evidence; after: Evidence; revision: number; recordId?: string; partition?: number; committed?: Evidence };

function capture(snapshot: KafkaLabSnapshot, recordId?: string) {
  const records = snapshot.partitions.flatMap((partition) => partition.messages);
  return {
    group: snapshot.selectedGroupId, stored: records.length, partitions: snapshot.partitions.length,
    workers: snapshot.consumers.length, assigned: snapshot.consumers.filter((worker) => worker.assignedPartitions.length > 0).length,
    idle: snapshot.consumers.filter((worker) => worker.assignedPartitions.length === 0).length,
    idleIds: snapshot.consumers.filter((worker) => worker.assignedPartitions.length === 0).map((worker) => worker.id),
    processed: records.filter((record) => record.delivery?.applicationStatus === "succeeded").length,
    skipped: records.filter((record) => record.delivery?.status === "lost").length,
    duplicateEvents: snapshot.events.filter((event) => event.type === "duplicate-delivery").length,
    recordsReadAfterRestart: snapshot.inspector?.observations?.recordsReadAfterRestart,
    positions: snapshot.partitions.map((partition) => ({ partition: partition.id, read: partition.currentOffset ?? 0,
      committed: partition.committedOffset ?? 0, end: partition.logEndOffset, owner: partition.assignedConsumerId ?? "unassigned" })),
    records: (recordId ? records.filter((record) => record.id === recordId) : records.slice(0, 6)).map((record) => ({ id: record.id, key: record.key, value: record.value,
      partition: record.partition, offset: record.offset, attempt: record.delivery?.attempt ?? 0,
      outcome: record.delivery?.applicationStatus ?? "not read", error: record.delivery?.failureReason })),
  };
}

/** Experiment actions go through the shared store; evidence is captured, not invented. */
export function runExperiment(id: ExperimentId): ExperimentResult {
  const actions = useKafkaStore.getState().actions;
  const current = () => useKafkaStore.getState().snapshot;
  actions.pause?.();
  if (id === "replay-before-commit") actions.loadScenario?.("duplicate-processing");
  else if (id === "commit-before-processing") actions.loadScenario?.("loss-before-processing");
  else if (id === "failure-retry-commit") actions.loadReferenceFlow?.();
  else actions.configureTopology?.({ topicName: "orders", partitionCount: 3 });
  current().consumers.forEach((worker) => actions.setConsumerAutoCommit?.(worker.id, false));
  if (id === "worker-scaling") actions.produceBatch?.({ keyPrefix: "order-HOT", valuePrefix: "hot-order", count: 3 });
  let recordId: string | undefined;
  let partition: number | undefined;
  if (id === "failure-retry-commit") {
    const failed = current().partitions.flatMap((lane) => lane.messages).find((record) => record.delivery?.applicationStatus === "failed");
    if (!failed) throw new Error("The failure fixture has no failed record to retry.");
    recordId = failed.id; partition = failed.partition;
  }
  const before = capture(current(), recordId);
  if (id === "keyed-ordering") {
    ["order-created", "order-paid", "order-shipped"].forEach((value) => actions.produce({ key: "order-LEARN", value }));
  } else if (id === "worker-scaling") {
    while (current().consumers.length < 5) actions.addConsumer(current().selectedGroupId);
  } else if (id === "failure-retry-commit") {
    const failed = current().partitions.flatMap((lane) => lane.messages).find((record) => record.delivery?.applicationStatus === "failed");
    if (!failed) throw new Error("The failure fixture has no failed record to retry.");
    recordId = failed.id; partition = failed.partition;
    actions.retryRecordProcessing?.({ partition: failed.partition, offset: failed.offset });
  } else actions.runScenario?.();
  const after = capture(current(), recordId);
  actions.closeInspector?.();
  return { id, before, after, revision: current().resetRevision ?? 0, recordId, partition };
}

export function canCommitExperiment(result: ExperimentResult, snapshot: KafkaLabSnapshot) {
  const record = snapshot.partitions.flatMap((lane) => lane.messages).find((candidate) => candidate.id === result.recordId);
  const lane = snapshot.partitions.find((candidate) => candidate.id === result.partition);
  const owner = snapshot.consumers.find((candidate) => candidate.id === lane?.assignedConsumerId);
  const capturedRecord = result.after.records.find((candidate) => candidate.id === result.recordId);
  const capturedLane = result.after.positions.find((candidate) => candidate.partition === result.partition);
  if (result.id !== "failure-retry-commit" || result.committed || snapshot.running
    || result.revision !== (snapshot.resetRevision ?? 0) || result.after.group !== snapshot.selectedGroupId
    || !record || !lane || !owner || !capturedRecord || !capturedLane
    || record.delivery?.applicationStatus !== "succeeded" || owner.id !== record.delivery.consumerId || owner.status === "dead"
    || owner.groupId !== result.after.group || capturedLane.owner !== owner.id
    || record.partition !== capturedRecord.partition || record.partition !== lane.id || record.offset !== capturedRecord.offset
    || record.delivery.attempt !== capturedRecord.attempt || capturedLane.read <= record.offset) return false;

  // commitConsumer saves all of this owner's lanes. Every affected assignment,
  // read position, and checkpoint must still match the captured retry evidence.
  const capturedOwned = result.after.positions.filter((position) => position.owner === owner.id);
  const liveOwned = snapshot.partitions.filter((position) => position.assignedConsumerId === owner.id);
  if (capturedOwned.length !== owner.assignedPartitions.length || capturedOwned.length !== liveOwned.length) return false;
  return capturedOwned.every((position) => {
    const live = liveOwned.find((candidate) => candidate.id === position.partition);
    return owner.assignedPartitions.includes(position.partition) && !!live
      && (owner.currentOffsets[position.partition] ?? owner.committedOffsets[position.partition] ?? 0) === position.read
      && (live.currentOffset ?? 0) === position.read
      && (owner.committedOffsets[position.partition] ?? 0) === position.committed
      && (live.committedOffset ?? 0) === position.committed;
  });
}

export function commitExperiment(result: ExperimentResult): ExperimentResult {
  const { snapshot, actions } = useKafkaStore.getState();
  if (!canCommitExperiment(result, snapshot)) return result;
  const owner = snapshot.partitions.find((lane) => lane.id === result.partition)?.assignedConsumerId;
  if (!owner) return result;
  actions.commitConsumer(owner);
  const committed = capture(useKafkaStore.getState().snapshot, result.recordId);
  const record = result.after.records.find((candidate) => candidate.id === result.recordId);
  if (!record || (committed.positions.find((lane) => lane.partition === result.partition)?.committed ?? 0) <= record.offset) return result;
  return { ...result, committed };
}
