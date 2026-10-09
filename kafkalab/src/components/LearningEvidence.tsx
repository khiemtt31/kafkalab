import { type Evidence, type ExperimentId, type ExperimentResult } from "../learning/experiments";

function focusedLane(evidence: Evidence, partition?: number) {
  return evidence.positions.find((lane) => lane.partition === (partition ?? evidence.records[0]?.partition)) ?? evidence.positions[0];
}

function evidenceRows(id: ExperimentId, evidence: Evidence, partition?: number): Array<[string, string | number]> {
  const lane = focusedLane(evidence, partition);
  const record = evidence.records[0];
  if (id === "worker-scaling") return [["Partitions", evidence.partitions], ["Consumers", evidence.workers], ["Assigned members", evidence.assigned], ["Idle members", evidence.idle],
    ["Hot partition", record ? `P${record.partition}` : "—"], ["Hot partition owner", lane?.owner ?? "—"],
    ["Assignment map", evidence.positions.map((position) => `P${position.partition} → ${position.owner}`).join("; ")], ["Idle member IDs", evidence.idleIds.join(", ") || "none"]];
  const rows: Array<[string, string | number]> = [["Stored records", evidence.stored], ["Read next offset", lane?.read ?? 0], ["Saved next offset", lane?.committed ?? 0], ["Log end offset", lane?.end ?? 0]];
  if (id === "keyed-ordering") rows.push(["Partition-local offsets", evidence.records.map((message) => `P${message.partition}:${message.offset}`).join(", ") || "empty"], ["Record identities", evidence.records.map((message) => message.id).join(", ") || "none"]);
  else if (id === "commit-before-processing") rows.push(["Successful outcomes", evidence.processed], ["Skipped outcomes", evidence.skipped], ["Read after restart", evidence.recordsReadAfterRestart ?? "not restarted"]);
  else rows.push(["Record identity", record?.id ?? "—"], ["Delivery attempt", record?.attempt ?? 0], ["Application outcome", record?.outcome ?? "not read"]);
  if (id === "replay-before-commit") rows.push(["Replay events", evidence.duplicateEvents]);
  if (id === "failure-retry-commit") rows.push(["Failure reason", record?.error ?? "none"]);
  return rows;
}

function changes(result: ExperimentResult): Array<[string, Array<string | number>]> {
  const stages = [result.before, result.after, ...(result.committed ? [result.committed] : [])];
  const lanes = stages.map((stage) => focusedLane(stage, result.partition ?? result.after.records[0]?.partition));
  if (result.id === "keyed-ordering") return [["Stored records", stages.map((stage) => stage.stored)], ["Key's partitions", stages.map((stage) => new Set(stage.records.map((record) => record.partition)).size)], ["Offsets", stages.map((stage) => stage.records.map((record) => record.offset).join(", ") || "empty")]];
  if (result.id === "worker-scaling") return [["Workers", stages.map((stage) => stage.workers)], ["With a partition", stages.map((stage) => stage.assigned)], ["Idle workers", stages.map((stage) => stage.idle)]];
  if (result.id === "commit-before-processing") return [["Stored records", stages.map((stage) => stage.stored)], ["Saved next", lanes.map((lane) => lane?.committed ?? 0)], ["Successful work", stages.map((stage) => stage.processed)]];
  return [["Attempt", stages.map((stage) => stage.records[0]?.attempt ?? 0)], ["Handler", stages.map((stage) => stage.records[0]?.outcome ?? "not read")], ["Saved next", lanes.map((lane) => lane?.committed ?? 0)]];
}

function EvidencePath({ evidence, id, partition, stage }: { evidence: Evidence; id: ExperimentId; partition?: number; stage: string }) {
  const lane = focusedLane(evidence, partition);
  const record = evidence.records[0];
  const skipped = id === "commit-before-processing" && evidence.skipped > 0;
  const failed = record?.outcome === "failed";
  return <section className="kl-evidence-stage" aria-label={`${stage} state path`}>
    <h5>{stage}</h5>
    <ol className="kl-evidence-path">
      <li className="kl-evidence-node"><span className="kl-evidence-node-label">Stored in topic</span>
        {id === "keyed-ordering" ? <><strong>{evidence.records.length ? `Key ${record?.key} → P${record?.partition}` : "No records yet"}</strong><div className="kl-evidence-record-chips">{evidence.records.map((item) => <span key={item.id}>P{item.partition}:{item.offset}<small>{item.value}</small></span>)}</div></> : <><strong>{record ? `P${record.partition}:${record.offset}` : "No record"}</strong><small>{record?.value ?? "empty topic"}</small><span className="kl-evidence-identity">{record?.id ?? "—"}</span></>}
      </li>
      <li className={`kl-evidence-node${failed || skipped ? " is-attention" : ""}`}><span className="kl-evidence-node-label">{id === "worker-scaling" ? "Partition ownership" : "Worker / handler"}</span>
        <strong>{lane?.owner ?? "unassigned"}</strong>
        {id === "worker-scaling" ? <><div className="kl-evidence-workers">{Array.from(new Set(evidence.positions.map((position) => position.owner))).filter((owner) => owner !== "unassigned").map((owner) => <span key={owner}>{owner}<small>owns {evidence.positions.filter((position) => position.owner === owner).map((position) => `P${position.partition}`).join(", ")}</small></span>)}{evidence.idleIds.map((owner) => <span key={owner} className="is-idle">{owner}<small>idle · no partition</small></span>)}</div><small>Hot key → P{record?.partition} → {lane?.owner}</small></> : <><span className="kl-evidence-outcome">{skipped ? "Work skipped on restart" : record ? `${record.outcome} · attempt ${record.attempt}` : "Nothing to read"}</span>{failed ? <small>{record?.error}</small> : null}</>}
      </li>
      <li className="kl-evidence-node kl-evidence-checkpoint"><span className="kl-evidence-node-label">Group {evidence.group} checkpoint</span><strong>Saved next: {lane?.committed ?? 0}</strong><small>Read next: {lane?.read ?? 0}</small>
        {skipped ? <span className="kl-evidence-outcome">Restart read {evidence.recordsReadAfterRestart ?? "unknown"} records</span> : id === "worker-scaling" ? <small>{evidence.assigned} assigned · {evidence.idle} idle</small> : null}
      </li>
    </ol>
  </section>;
}

export function LearningEvidence({ result, compact = false }: { result: ExperimentResult; compact?: boolean }) {
  const stages = [{ label: "Before", evidence: result.before }, { label: result.id === "failure-retry-commit" ? "After retry" : "After", evidence: result.after }, ...(result.committed ? [{ label: "After commit", evidence: result.committed }] : [])];
  const partition = result.partition ?? result.after.records[0]?.partition;
  const rows = stages.map((stage) => evidenceRows(result.id, stage.evidence, partition));
  const paths = <div className="kl-evidence-stages" data-stage-count={stages.length}>{stages.map((stage) => <EvidencePath key={stage.label} evidence={stage.evidence} id={result.id} partition={partition} stage={stage.label} />)}</div>;
  return <div className={`kl-learning-evidence${compact ? " kl-learning-evidence--compact" : ""}`}>
    <div className="kl-evidence-heading"><h4>What changed?</h4><span>Captured from this run</span></div>
    <dl className="kl-evidence-deltas">{changes(result).map(([label, values]) => <div key={label}><dt>{label}</dt><dd>{values.map((value, index) => <span key={index}>{index > 0 ? <span className="kl-evidence-delta-arrow" aria-label="to">→</span> : null}<b>{value}</b></span>)}</dd></div>)}</dl>
    {compact ? <details className="kl-learning-reference"><summary>Trace before and after</summary>{paths}</details> : paths}
    <details className="kl-learning-reference"><summary>Compare exact evidence</summary>
      <div className="kl-evidence-table-scroll" tabIndex={0} role="region" aria-label="Captured evidence comparison table"><table><caption>Group {result.after.group} · this run, not live state</caption>
        <thead><tr><th scope="col">State</th>{stages.map((stage) => <th key={stage.label} scope="col">{stage.label}</th>)}</tr></thead>
        <tbody>{rows[0].map(([label], index) => <tr key={label}><th scope="row">{label}</th>{rows.map((stageRows, stageIndex) => <td key={stageIndex}>{stageRows[index]?.[1]}</td>)}</tr>)}</tbody>
      </table></div>
    </details>
  </div>;
}
