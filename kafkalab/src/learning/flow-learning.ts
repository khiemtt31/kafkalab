import type { KafkaLabSnapshot } from "../hooks/useKafkaLab";
import type { ExperimentResult } from "./experiments";

/** Teaching copy is derived from the current group; captured runs never replace live evidence. */
export function learningFocus(snapshot: KafkaLabSnapshot, result?: ExperimentResult, selectedPartition?: number) {
  const validResult = result?.revision === (snapshot.resetRevision ?? 0) && result.after.group === snapshot.selectedGroupId ? result : undefined;
  const failedLane = snapshot.partitions.find((lane) => lane.messages.some((record) => record.delivery?.applicationStatus === "failed"));
  const preferredId = selectedPartition ?? validResult?.partition ?? validResult?.after.records[0]?.partition;
  const lane = snapshot.partitions.find((candidate) => candidate.id === preferredId)
    ?? failedLane ?? snapshot.partitions.find((candidate) => candidate.messages.length) ?? snapshot.partitions[0];
  if (!lane) return { partitionId: undefined, title: "No partition to follow", detail: "Run a challenge to create a record path.", tone: "neutral" };
  const record = lane.messages.find((candidate) => candidate.id === validResult?.recordId)
    ?? lane.messages.find((candidate) => candidate.delivery?.applicationStatus === "failed")
    ?? lane.messages.find((candidate) => candidate.delivery?.status === "lost")
    ?? lane.messages.find((candidate) => candidate.delivery?.applicationStatus === "succeeded" && candidate.offset >= (lane.committedOffset ?? 0))
    ?? lane.messages.at(-1);
  const committed = lane.committedOffset ?? 0;
  const identity = record ? `P${lane.id} · offset ${record.offset} · ${record.key ?? "unkeyed"} · ${record.id}${record.delivery ? ` · attempt ${record.delivery.attempt}` : ""}` : `P${lane.id}`;
  const owner = lane.assignedConsumerName ?? lane.assignedConsumerId ?? "No assigned consumer";
  const positions = `Read next ${lane.currentOffset ?? committed} · saved next ${committed} · log end ${lane.logEndOffset}`;
  if (record?.delivery?.status === "lost") return { partitionId: lane.id, title: `Work skipped at ${identity}`, detail: `Saved next ${committed} is past this record. The record remains stored; application work did not succeed.`, tone: "failure" };
  if (record?.delivery?.applicationStatus === "failed") return { partitionId: lane.id, title: `Handler failed at ${identity}`, detail: `${record.delivery.failureReason ?? "Application processing failed."} Attempt ${record.delivery.attempt} was read by ${record.delivery.consumerId}. Current owner: ${owner}. Retry and commit are separate steps.`, tone: "failure" };
  if (record?.delivery?.applicationStatus === "succeeded") return { partitionId: lane.id,
    title: record.offset < committed ? `Checkpoint passed ${identity}` : `Succeeded, awaiting commit · ${identity}`,
    detail: `${positions}. ${record.offset < committed ? "The checkpoint changed; the record is still stored." : "The handler succeeded. The saved next offset has not passed this record."}`, tone: "success" };
  return { partitionId: lane.id, title: record ? `Follow ${identity}` : `P${lane.id} is ready for records`,
    detail: `${owner} · ${positions}. ${record ? "Read position and application outcome are separate evidence." : "Run a challenge to see this lane change."}`, tone: "neutral" };
}

export const chartQuestions = {
  flow: { question: "Where does the journey break?", hint: "Follow the record, its handler outcome, and the separate checkpoint." },
  partitions: { question: "Which lane holds the work?", hint: "Each partition has its own records, owner, and ordered offsets." },
  consumers: { question: "Would another worker help?", hint: "One owner per partition in this group. Extra members can be idle." },
  offsets: { question: "Read, processed, or saved?", hint: "Compare saved next, read next, and log end. Lag uses the saved position." },
  delivery: { question: "Did the application actually succeed?", hint: "Group-wide outcomes: reading a record and completing its work are separate." },
  replication: { question: "Where are this partition’s copies?", hint: "Compare its leader and in-sync replicas. Replication protects the log." },
  activity: { question: "What changed, and in what order?", hint: "Recent simulator events show the transitions behind the chart." },
} as const;
