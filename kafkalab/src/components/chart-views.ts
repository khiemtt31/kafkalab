import type { Edge, Node, XYPosition } from "@xyflow/react";
import type { KafkaLabSnapshot } from "../hooks/useKafkaLab";
import type { LabIconName } from "./LabIcon";
import { compactFlowPositions, projectPartitionSummary, recordState } from "./flow-projection";
import { CHART_ACTIVITY_CATEGORIES } from "./chart-activity";

export const CHART_VARIANTS = [
  { id: "flow", label: "Flow", description: "Follow the path from producer to topic, partition, consumer, and checkpoint." },
  { id: "partitions", label: "Partitions", description: "Compare each partition's owner, backlog, outcomes, and local offsets." },
  { id: "consumers", label: "Consumers", description: "See how the selected consumer group owns partitions and processes work." },
  { id: "offsets", label: "Offsets & lag", description: "Read committed, current, and log-end positions as separate Kafka concepts." },
  { id: "delivery", label: "Delivery", description: "Trace the selected group's latest records through delivery outcomes." },
  { id: "replication", label: "Replication", description: "Compare partition leaders, replicas, and in-sync brokers." },
  { id: "activity", label: "Activity", description: "See bounded recent simulator activity grouped by meaningful transitions." },
] as const;

export type ChartVariantId = typeof CHART_VARIANTS[number]["id"];
export type FlowState = "active" | "attention" | "idle";

export type ChartTooltipRow = { label: string; value: string };
export type ChartNodeTooltip = {
  title: string;
  eyebrow: string;
  summary: string;
  rows: ChartTooltipRow[];
  note?: string;
};

export type ChartInlineRow = { label: string; value: string; tone?: "accent" | "failure" };

export type ChartNodeData = {
  kind: string;
  title: string;
  eyebrow: string;
  metric: string;
  status: string;
  state: FlowState;
  icon: LabIconName;
  inline?: ChartInlineRow[];
  tooltip: ChartNodeTooltip;
};

export type ChartNode = Node<ChartNodeData, "chart">;
export type ChartEdge = Edge<{ label: string }, "kl-flow">;

export type ChartProjection = {
  variant: ChartVariantId;
  label: string;
  description: string;
  nodes: ChartNode[];
  edges: ChartEdge[];
  focusNodeId?: string;
  ariaLabel: string;
};

type PartitionView = {
  partitionId: number;
  owner: string;
  records: number;
  orders: number;
  unread: number;
  inFlight: number;
  processed: number;
  failed: number;
  committedRecords: number;
  skipped: number;
  committed: number;
  current: number;
  leo: number;
  lag: number;
  leader?: string;
  replicas: string[];
  inSyncReplicas: string[];
};

const groupFor = (snapshot: KafkaLabSnapshot) => snapshot.consumerGroups.find((group) => group.id === snapshot.selectedGroupId) ?? snapshot.consumerGroups[0];

function partitionView(snapshot: KafkaLabSnapshot, partitionId: number): PartitionView {
  const partition = snapshot.partitions.find((candidate) => candidate.id === partitionId);
  const group = groupFor(snapshot);
  const messages = partition?.messages ?? [];
  const committed = partition?.committedOffset ?? group?.committedOffsets[partitionId] ?? 0;
  const current = partition?.currentOffset ?? committed;
  const leo = partition?.logEndOffset ?? 0;
  const summary = projectPartitionSummary(messages, committed, { current, logEndOffset: leo });
  const assignment = snapshot.cluster?.assignments.find((candidate) => candidate.topic === snapshot.topicName && candidate.partition === partitionId);
  return {
    partitionId,
    owner: group?.assignments[partitionId] ?? "unassigned",
    records: messages.length,
    orders: summary.orders,
    unread: summary.unread,
    inFlight: summary.inFlight,
    processed: summary.processed,
    failed: summary.failed,
    committedRecords: summary.committed,
    skipped: summary.skipped,
    committed,
    current,
    leo,
    lag: Math.max(0, partition?.lag ?? leo - committed),
    leader: assignment?.leader,
    replicas: assignment?.replicas ?? [],
    inSyncReplicas: assignment?.inSyncReplicas ?? [],
  };
}

function partitionTooltip(snapshot: KafkaLabSnapshot, details: PartitionView, summary = `Partition ${details.partitionId} in ${snapshot.topicName || "the topic"}.`): ChartNodeTooltip {
  return {
    title: `Partition ${details.partitionId}`,
    eyebrow: "PARTITION",
    summary,
    rows: [
      { label: "Owner", value: details.owner },
      { label: "Orders", value: String(details.orders) },
      { label: "Records", value: String(details.records) },
      { label: "Unread", value: String(details.unread) },
      { label: "In flight", value: String(details.inFlight) },
      { label: "Processed", value: String(details.processed) },
      { label: "Failed", value: String(details.failed) },
      { label: "Committed records", value: String(details.committedRecords) },
      { label: "Skipped", value: String(details.skipped) },
      { label: "C / P / LEO", value: `${details.committed} / ${details.current} / ${details.leo}` },
      { label: "Commit lag", value: String(details.lag) },
      { label: "Leader", value: details.leader ?? "unavailable" },
      { label: "Replicas", value: details.replicas.length ? details.replicas.join(", ") : "unavailable" },
      { label: "In-sync replicas", value: details.inSyncReplicas.length ? details.inSyncReplicas.join(", ") : "unavailable" },
    ],
    note: "C is the committed next offset, P is the current read position, and LEO is where the next record would be appended.",
  };
}

function node(id: string, position: XYPosition, data: ChartNodeData): ChartNode {
  return {
    id,
    type: "chart",
    position,
    data,
    ariaLabel: `${data.eyebrow.toLowerCase()} ${data.title}. ${data.status}. ${data.metric}. ${data.tooltip.summary}`,
  };
}

function edge(id: string, source: string, target: string, label: string): ChartEdge {
  return { id, type: "kl-flow", source, target, data: { label } };
}

function stack(ids: string[], x: number, startY = 80, gap = 150): Record<string, XYPosition> {
  return Object.fromEntries(ids.map((id, index) => [id, { x, y: startY + index * gap }]));
}

function producerIds(snapshot: KafkaLabSnapshot) {
  return snapshot.producers?.length ? snapshot.producers : [{ id: "producer-1", producedCount: snapshot.messageCount }];
}

function producerNode(snapshot: KafkaLabSnapshot, producerId: string, position: XYPosition, active = false): ChartNode {
  const producer = producerIds(snapshot).find((candidate) => candidate.id === producerId);
  const count = producer?.producedCount ?? 0;
  const cluster = snapshot.cluster;
  return node(`producer-${producerId}`, position, {
    kind: "producer",
    icon: "send",
    eyebrow: "PRODUCER",
    title: producerId,
    metric: `${count} records`,
    status: active ? "publishing" : count ? "has produced" : "waiting",
    state: active ? "active" : count ? "attention" : "idle",
    inline: [{ label: "acks", value: cluster?.requiredAcks ?? "—" }],
    tooltip: {
      title: producerId,
      eyebrow: "PRODUCER",
      summary: "A local teaching identity that appends records to the topic.",
      rows: [
        { label: "Produced", value: `${count} records` },
        { label: "Acknowledgements", value: cluster?.requiredAcks ?? "unavailable" },
        { label: "Producer rate", value: `${snapshot.producerRate} records/s` },
        { label: "Automatic production", value: snapshot.producerRunning ? "running" : "paused" },
      ],
      note: "The producer identity is local teaching provenance; it is not an intrinsic Kafka record field.",
    },
  });
}

function topicNode(snapshot: KafkaLabSnapshot, position: XYPosition): ChartNode {
  const partitionCount = snapshot.partitions.length;
  return node("topic", position, {
    kind: "topic",
    icon: "layers",
    eyebrow: "TOPIC",
    title: snapshot.topicName || "orders",
    metric: `${partitionCount} partitions`,
    status: `${snapshot.messageCount} stored records`,
    state: snapshot.totalLag ? "attention" : "idle",
    inline: [
      { label: "RF", value: String(snapshot.cluster?.replicationFactor ?? "—") },
      { label: "min ISR", value: String(snapshot.cluster?.minInSyncReplicas ?? "—") },
    ],
    tooltip: {
      title: snapshot.topicName || "orders",
      eyebrow: "TOPIC",
      summary: "An append-only named stream divided into independent partition logs.",
      rows: [
        { label: "Partitions", value: String(partitionCount) },
        { label: "Stored records", value: String(snapshot.messageCount) },
        { label: "Selected group", value: groupFor(snapshot)?.name ?? snapshot.selectedGroupId },
        { label: "Replication factor", value: String(snapshot.cluster?.replicationFactor ?? "unavailable") },
        { label: "Minimum ISR", value: String(snapshot.cluster?.minInSyncReplicas ?? "unavailable") },
      ],
      note: "Offsets are local to a partition; the topic does not have one global offset.",
    },
  });
}

function partitionNode(snapshot: KafkaLabSnapshot, details: PartitionView, position: XYPosition, variant: string): ChartNode {
  const failed = details.failed > 0;
  return node(`partition-${details.partitionId}`, position, {
    kind: "partition",
    icon: "layers",
    eyebrow: "PARTITION",
    title: `P${details.partitionId}`,
    metric: `${details.orders} orders · ${details.records} records`,
    status: details.owner,
    state: failed || details.lag > 0 ? "attention" : "idle",
    inline: variant === "offsets"
      ? [{ label: "C", value: String(details.committed) }, { label: "P", value: String(details.current) }, { label: "LEO", value: String(details.leo) }, { label: "lag", value: String(details.lag), tone: details.lag ? "accent" : undefined }]
      : [{ label: "unread", value: String(details.unread) }, { label: "in flight", value: String(details.inFlight) }, { label: "failed", value: String(details.failed), tone: failed ? "failure" : undefined }],
    tooltip: partitionTooltip(snapshot, details),
  });
}

function consumerNode(snapshot: KafkaLabSnapshot, consumerId: string, position: XYPosition): ChartNode {
  const group = groupFor(snapshot);
  const consumer = group?.consumers.find((candidate) => candidate.id === consumerId);
  const assigned = consumer?.assignedPartitions ?? [];
  const assignedPartitions = snapshot.partitions.filter((partition) => assigned.includes(partition.id));
  const succeeded = assignedPartitions.flatMap((partition) => partition.messages).filter((message) => message.delivery?.consumerId === consumerId && message.delivery.applicationStatus === "succeeded").length;
  const failed = assignedPartitions.flatMap((partition) => partition.messages).filter((message) => message.delivery?.consumerId === consumerId && message.delivery.applicationStatus === "failed").length;
  const inFlight = assignedPartitions.flatMap((partition) => partition.messages).filter((message) => message.delivery?.consumerId === consumerId && recordState(message, consumer?.committedOffsets[message.partition] ?? 0) === "in-flight").length;
  const lag = assignedPartitions.reduce((sum, partition) => sum + (partition.lag ?? 0), 0);
  const status = consumer?.status === "dead" ? "crashed" : consumer?.status === "paused" ? "paused" : !assigned.length ? "idle" : snapshot.running ? "reading" : "ready";
  return node(`consumer-${consumerId}`, position, {
    kind: "consumer",
    icon: "worker",
    eyebrow: "CONSUMER",
    title: consumer?.name ?? consumerId,
    metric: assigned.length ? `${assigned.length} partitions` : "idle",
    status,
    state: failed || consumer?.status === "dead" ? "attention" : "idle",
    inline: [{ label: "in flight", value: String(inFlight) }, { label: "done", value: String(succeeded) }, { label: "failed", value: String(failed), tone: failed ? "failure" : undefined }],
    tooltip: {
      title: consumer?.name ?? consumerId,
      eyebrow: "CONSUMER",
      summary: "A member of the selected consumer group that owns assigned partitions.",
      rows: [
        { label: "Group", value: group?.name ?? snapshot.selectedGroupId },
        { label: "Status", value: status },
        { label: "Assigned partitions", value: assigned.length ? assigned.map((id) => `P${id}`).join(", ") : "idle" },
        { label: "In flight", value: String(inFlight) },
        { label: "Succeeded", value: String(succeeded) },
        { label: "Failed", value: String(failed) },
        { label: "Commit lag", value: String(lag) },
        { label: "Rate", value: `${consumer?.rate ?? 0} records/s` },
        { label: "Commit mode", value: consumer?.autoCommit ? "automatic" : "manual" },
      ],
      note: "One active consumer owns a partition at a time inside this selected group.",
    },
  });
}

function commitNode(snapshot: KafkaLabSnapshot, position: XYPosition): ChartNode {
  const group = groupFor(snapshot);
  const checkpoints = group ? Object.entries(group.committedOffsets).map(([partition, offset]) => `P${partition} → ${offset}`).join(" · ") : "—";
  return node("commit", position, {
    kind: "commit",
    icon: "check",
    eyebrow: "GROUP PROGRESS",
    title: "Checkpoint",
    metric: group ? `${Object.keys(group.committedOffsets).length} positions` : "waiting",
    status: "records stay in the log",
    state: snapshot.totalLag ? "attention" : "idle",
    inline: [{ label: "saved", value: checkpoints || "—" }],
    tooltip: {
      title: "Checkpoint",
      eyebrow: "GROUP PROGRESS",
      summary: "The selected group stores the next offset it should read for each partition.",
      rows: [
        { label: "Group", value: group?.name ?? snapshot.selectedGroupId },
        { label: "Positions", value: checkpoints || "none" },
        { label: "Commit lag", value: String(snapshot.totalLag) },
      ],
      note: "A checkpoint does not delete a topic record or prove that application work succeeded.",
    },
  });
}

function flowProjection(snapshot: KafkaLabSnapshot): ChartProjection {
  const group = groupFor(snapshot);
  const producerList = producerIds(snapshot);
  const producerPosition = compactFlowPositions(producerList.map((producer) => producer.id), snapshot.partitions.map((partition) => partition.id), group?.consumers.map((consumer) => consumer.id) ?? []).producer;
  const positions = compactFlowPositions(producerList.map((producer) => producer.id), snapshot.partitions.map((partition) => partition.id), group?.consumers.map((consumer) => consumer.id) ?? []);
  const nodes = producerList.map((producer) => producerNode(snapshot, producer.id, positions[`producer-${producer.id}`] ?? producerPosition ?? { x: 0, y: 120 })) as ChartNode[];
  nodes.push(topicNode(snapshot, positions.topic ?? { x: 250, y: 220 }));
  snapshot.partitions.forEach((partition) => nodes.push(partitionNode(snapshot, partitionView(snapshot, partition.id), positions[`partition-${partition.id}`] ?? { x: 500, y: 120 }, "flow")));
  (group?.consumers ?? []).forEach((consumer) => nodes.push(consumerNode(snapshot, consumer.id, positions[`consumer-${consumer.id}`] ?? { x: 850, y: 120 })));
  nodes.push(commitNode(snapshot, positions.commit ?? { x: 1150, y: 220 }));
  const edges: ChartEdge[] = [];
  producerList.forEach((producer) => edges.push(edge(`producer-${producer.id}-topic`, `producer-${producer.id}`, "topic", "send")));
  snapshot.partitions.forEach((partition) => {
    edges.push(edge(`topic-partition-${partition.id}`, "topic", `partition-${partition.id}`, "route"));
    const owner = group?.assignments[partition.id];
    if (owner) edges.push(edge(`partition-consumer-${partition.id}-${owner}`, `partition-${partition.id}`, `consumer-${owner}`, "read"));
  });
  (group?.consumers ?? []).forEach((consumer) => edges.push(edge(`consumer-commit-${consumer.id}`, `consumer-${consumer.id}`, "commit", "save")));
  return { variant: "flow", label: "Flow", description: CHART_VARIANTS[0].description, nodes, edges, focusNodeId: snapshot.partitions[0] ? "partition-0" : "topic", ariaLabel: "Kafka producer to topic, partition, consumer, and checkpoint flow" };
}

function partitionsProjection(snapshot: KafkaLabSnapshot): ChartProjection {
  const group = groupFor(snapshot);
  const positions: Record<string, XYPosition> = { topic: { x: 40, y: 190 }, ...stack(snapshot.partitions.map((partition) => `partition-${partition.id}`), 420, 50, 150), ...stack((group?.consumers ?? []).map((consumer) => `consumer-${consumer.id}`), 880, 85, 150) };
  const nodes: ChartNode[] = [topicNode(snapshot, positions.topic)];
  snapshot.partitions.forEach((partition) => nodes.push(partitionNode(snapshot, partitionView(snapshot, partition.id), positions[`partition-${partition.id}`], "partitions")));
  (group?.consumers ?? []).forEach((consumer) => nodes.push(consumerNode(snapshot, consumer.id, positions[`consumer-${consumer.id}`])));
  const edges = snapshot.partitions.flatMap((partition) => {
    const owner = group?.assignments[partition.id];
    return [edge(`topic-partition-${partition.id}`, "topic", `partition-${partition.id}`, "partition"), ...(owner ? [edge(`partition-consumer-${partition.id}-${owner}`, `partition-${partition.id}`, `consumer-${owner}`, "owned by")] : [])];
  });
  return { variant: "partitions", label: "Partitions", description: CHART_VARIANTS[1].description, nodes, edges, focusNodeId: snapshot.partitions[0] ? "partition-0" : "topic", ariaLabel: "Kafka partition comparison chart" };
}

function consumersProjection(snapshot: KafkaLabSnapshot): ChartProjection {
  const group = groupFor(snapshot);
  const positions: Record<string, XYPosition> = { group: { x: 40, y: 190 }, ...stack((group?.consumers ?? []).map((consumer) => `consumer-${consumer.id}`), 410, 70, 150), ...stack(snapshot.partitions.map((partition) => `partition-${partition.id}`), 850, 50, 150) };
  const groupNode = node("group", positions.group, {
    kind: "group", icon: "worker", eyebrow: "CONSUMER GROUP", title: group?.name ?? snapshot.selectedGroupId, metric: `${group?.consumers.length ?? 0} consumers`, status: `${snapshot.totalLag} commit lag`, state: snapshot.totalLag ? "attention" : "idle",
    tooltip: { title: group?.name ?? snapshot.selectedGroupId, eyebrow: "CONSUMER GROUP", summary: "A group shares partition ownership while maintaining its own checkpoints.", rows: [{ label: "Consumers", value: String(group?.consumers.length ?? 0) }, { label: "Partitions", value: String(snapshot.partitions.length) }, { label: "Commit lag", value: String(snapshot.totalLag) }] },
  });
  const nodes: ChartNode[] = [groupNode];
  (group?.consumers ?? []).forEach((consumer) => nodes.push(consumerNode(snapshot, consumer.id, positions[`consumer-${consumer.id}`])));
  snapshot.partitions.forEach((partition) => nodes.push(partitionNode(snapshot, partitionView(snapshot, partition.id), positions[`partition-${partition.id}`], "consumers")));
  const edges: ChartEdge[] = [];
  (group?.consumers ?? []).forEach((consumer) => {
    edges.push(edge(`group-consumer-${consumer.id}`, "group", `consumer-${consumer.id}`, "member"));
    consumer.assignedPartitions.forEach((partitionId) => edges.push(edge(`consumer-${consumer.id}-partition-${partitionId}`, `consumer-${consumer.id}`, `partition-${partitionId}`, "owns")));
  });
  return { variant: "consumers", label: "Consumers", description: CHART_VARIANTS[2].description, nodes, edges, focusNodeId: group?.consumers[0] ? `consumer-${group.consumers[0].id}` : "group", ariaLabel: "Kafka consumer group assignment chart" };
}

function offsetsProjection(snapshot: KafkaLabSnapshot): ChartProjection {
  const nodes: ChartNode[] = [];
  const edges: ChartEdge[] = [];
  nodes.push(node("offset-model", { x: 30, y: 190 }, {
    kind: "offset-model", icon: "layers", eyebrow: "OFFSET MODEL", title: "Partition positions", metric: `${snapshot.partitions.length} partitions`, status: "C / P / LEO", state: "idle",
    tooltip: { title: "Partition positions", eyebrow: "OFFSET MODEL", summary: "Kafka tracks committed progress, current reading, and the next append position independently for each partition.", rows: [{ label: "C", value: "Committed next offset" }, { label: "P", value: "Current read position" }, { label: "LEO", value: "Log end offset" }, { label: "Total lag", value: String(snapshot.totalLag) }] },
  }));
  snapshot.partitions.forEach((partition, index) => {
    const details = partitionView(snapshot, partition.id);
    const y = 55 + index * 190;
    const partitionId = `partition-${partition.id}`;
    nodes.push(partitionNode(snapshot, details, { x: 350, y }, "offsets"));
    const metrics = [
      ["committed", "C", details.committed, "committed next offset"],
      ["current", "P", details.current, "current read position"],
      ["end", "LEO", details.leo, "log end offset"],
    ] as const;
    metrics.forEach(([id, label, value, meaning], metricIndex) => {
      const metricId = `offset-${id}-${partition.id}`;
      nodes.push(node(metricId, { x: 770, y: y - 28 + metricIndex * 58 }, {
        kind: "offset", icon: "activity", eyebrow: label, title: `P${partition.id} ${label}`, metric: String(value), status: meaning, state: label === "LEO" || value !== details.committed ? "attention" : "idle",
        tooltip: { title: `P${partition.id} ${label}`, eyebrow: label, summary: meaning, rows: [{ label: "Partition", value: `P${partition.id}` }, { label: label, value: String(value) }, { label: "Commit lag", value: String(details.lag) }] },
      }));
      edges.push(edge(`offset-${partition.id}-${id}`, partitionId, metricId, label));
    });
    edges.push(edge(`offset-model-${partition.id}`, "offset-model", partitionId, "tracks"));
  });
  return { variant: "offsets", label: "Offsets & lag", description: CHART_VARIANTS[3].description, nodes, edges, focusNodeId: "offset-model", ariaLabel: "Kafka committed, current, and log-end offset chart" };
}

function deliveryProjection(snapshot: KafkaLabSnapshot): ChartProjection {
  const group = groupFor(snapshot);
  const messages = snapshot.partitions.flatMap((partition) => partition.messages);
  const counts = new Map<string, number>(["queued", "in-flight", "succeeded", "failed", "committed", "skipped"].map((key) => [key, 0]));
  snapshot.partitions.forEach((partition) => partition.messages.forEach((message) => {
    const outcome = recordState(message, partition.committedOffset ?? group?.committedOffsets[partition.id] ?? 0);
    const state = outcome === "processed" ? "succeeded" : outcome;
    counts.set(state, (counts.get(state) ?? 0) + 1);
  }));
  const stages = ["queued", "in-flight", "succeeded", "committed", "failed", "skipped"] as const;
  const nodes = stages.map((stage, index) => node(`delivery-${stage}`, { x: 45 + (index < 4 ? index : index - 3) * 300, y: index < 4 ? 80 : 270 }, {
    kind: "delivery", icon: stage === "committed" ? "check" : "activity", eyebrow: "DELIVERY STATE", title: stage === "in-flight" ? "In flight" : stage[0].toUpperCase() + stage.slice(1), metric: `${counts.get(stage) ?? 0} records`, status: stage === "failed" ? "application failure" : stage === "skipped" ? "position advanced" : "latest selected-group state", state: stage === "failed" && (counts.get(stage) ?? 0) > 0 ? "attention" : "idle",
    tooltip: { title: stage === "in-flight" ? "In flight" : stage[0].toUpperCase() + stage.slice(1), eyebrow: "DELIVERY STATE", summary: `Selected-group records currently represented as ${stage}.`, rows: [{ label: "Records", value: String(counts.get(stage) ?? 0) }, { label: "Stored records", value: String(messages.length) }, { label: "Selected group", value: group?.name ?? snapshot.selectedGroupId }], note: stage === "failed" ? "A failed application attempt does not remove the topic record or automatically advance the committed offset." : undefined },
  }));
  const edges = [
    edge("delivery-queued-in-flight", "delivery-queued", "delivery-in-flight", "read"),
    edge("delivery-in-flight-succeeded", "delivery-in-flight", "delivery-succeeded", "success"),
    edge("delivery-succeeded-committed", "delivery-succeeded", "delivery-committed", "commit"),
    edge("delivery-in-flight-failed", "delivery-in-flight", "delivery-failed", "failure"),
    edge("delivery-failed-in-flight", "delivery-failed", "delivery-in-flight", "retry"),
    edge("delivery-in-flight-skipped", "delivery-in-flight", "delivery-skipped", "skip"),
  ];
  return { variant: "delivery", label: "Delivery", description: CHART_VARIANTS[4].description, nodes, edges, focusNodeId: "delivery-in-flight", ariaLabel: "Kafka delivery lifecycle chart" };
}

function replicationProjection(snapshot: KafkaLabSnapshot): ChartProjection {
  const cluster = snapshot.cluster;
  if (!cluster) {
    const nodes = [topicNode(snapshot, { x: 130, y: 180 }), node("replication-unavailable", { x: 540, y: 150 }, {
      kind: "empty", icon: "layers", eyebrow: "REPLICATION", title: "No broker cluster", metric: "unavailable", status: "use a reference or broker scenario", state: "idle",
      tooltip: { title: "No broker cluster", eyebrow: "REPLICATION", summary: "This saved workflow does not currently contain broker, leader, or ISR data.", rows: [{ label: "Partitions", value: String(snapshot.partitions.length) }, { label: "Replication factor", value: "unavailable" }, { label: "In-sync replicas", value: "unavailable" }], note: "The chart stays honest instead of inventing broker health." },
    })];
    return { variant: "replication", label: "Replication", description: CHART_VARIANTS[5].description, nodes, edges: [edge("replication-unavailable-edge", "topic", "replication-unavailable", "health")], focusNodeId: "replication-unavailable", ariaLabel: "Kafka replication chart without broker data" };
  }
  const brokerIds = cluster.brokers.map((broker) => broker.id);
  const positions: Record<string, XYPosition> = { topic: { x: 20, y: 190 }, ...stack(snapshot.partitions.map((partition) => `partition-${partition.id}`), 370, 50, 150), ...stack(brokerIds.map((id) => `broker-${id}`), 850, 100, 180) };
  const nodes: ChartNode[] = [topicNode(snapshot, positions.topic)];
  snapshot.partitions.forEach((partition) => nodes.push(partitionNode(snapshot, partitionView(snapshot, partition.id), positions[`partition-${partition.id}`], "replication")));
  cluster.brokers.forEach((broker) => nodes.push(node(`broker-${broker.id}`, positions[`broker-${broker.id}`], {
    kind: "broker", icon: "layers", eyebrow: "BROKER", title: broker.id, metric: broker.status, status: broker.status === "up" ? "available" : "failed", state: broker.status === "up" ? "idle" : "attention",
    inline: [{ label: "status", value: broker.status }], tooltip: { title: broker.id, eyebrow: "BROKER", summary: "An educational broker node used to show replica placement and ISR membership.", rows: [{ label: "Status", value: broker.status }, { label: "Replication factor", value: String(cluster.replicationFactor) }, { label: "Minimum ISR", value: String(cluster.minInSyncReplicas) }, { label: "Required acks", value: cluster.requiredAcks }] },
  })));
  const edges: ChartEdge[] = [];
  snapshot.partitions.forEach((partition) => {
    const assignment = cluster.assignments.find((candidate) => candidate.topic === snapshot.topicName && candidate.partition === partition.id);
    edges.push(edge(`replication-topic-${partition.id}`, "topic", `partition-${partition.id}`, "partition"));
    assignment?.replicas.forEach((brokerId) => edges.push(edge(`replication-replica-${partition.id}-${brokerId}`, `partition-${partition.id}`, `broker-${brokerId}`, assignment.leader === brokerId ? "leader" : assignment.inSyncReplicas.includes(brokerId) ? "ISR replica" : "replica")));
  });
  return { variant: "replication", label: "Replication", description: CHART_VARIANTS[5].description, nodes, edges, focusNodeId: "topic", ariaLabel: "Kafka partition replication and in-sync replica chart" };
}

function activityProjection(snapshot: KafkaLabSnapshot): ChartProjection {
  const categories = CHART_ACTIVITY_CATEGORIES;
  const nodes: ChartNode[] = [node("activity-root", { x: 40, y: 190 }, {
    kind: "activity-root", icon: "activity", eyebrow: "RECENT ACTIVITY", title: "Simulator events", metric: `${snapshot.events.length} retained`, status: "bounded history", state: "idle",
    tooltip: { title: "Simulator events", eyebrow: "RECENT ACTIVITY", summary: "A bounded view of real transitions emitted by the local simulator.", rows: [{ label: "Retained events", value: String(snapshot.events.length) }, { label: "Latest", value: snapshot.events.at(-1)?.summary ?? "none" }], note: "This view is intentionally bounded; it does not claim that the logical topic history is capped." },
  })];
  const edges: ChartEdge[] = [];
  categories.forEach((category, index) => {
    const events = snapshot.events.filter((event) => category.types.some((type) => type === event.type));
    const latest = events.at(-1);
    nodes.push(node(`activity-${category.id}`, { x: 390, y: 35 + index * 85 }, {
      kind: "activity", icon: "activity", eyebrow: "EVENT GROUP", title: category.label, metric: `${events.length} events`, status: latest?.summary ?? "no events yet", state: category.id === "failed" && events.length ? "attention" : "idle",
      tooltip: { title: category.label, eyebrow: "EVENT GROUP", summary: `Recent simulator events grouped as ${category.label.toLowerCase()}.`, rows: [{ label: "Events", value: String(events.length) }, { label: "Latest time", value: latest ? new Date(latest.timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "none" }, { label: "Latest detail", value: latest?.summary ?? "none" }, { label: "Concept", value: latest?.concept ?? "none" }] },
    }));
    edges.push(edge(`activity-root-${category.id}`, "activity-root", `activity-${category.id}`, "observes"));
  });
  return { variant: "activity", label: "Activity", description: CHART_VARIANTS[6].description, nodes, edges, focusNodeId: "activity-root", ariaLabel: "Kafka simulator activity chart" };
}

export function getChartVariant(variant: ChartVariantId) {
  return CHART_VARIANTS.find((candidate) => candidate.id === variant) ?? CHART_VARIANTS[0];
}

export function projectChart(snapshot: KafkaLabSnapshot, variant: ChartVariantId): ChartProjection {
  switch (variant) {
    case "partitions": return partitionsProjection(snapshot);
    case "consumers": return consumersProjection(snapshot);
    case "offsets": return offsetsProjection(snapshot);
    case "delivery": return deliveryProjection(snapshot);
    case "replication": return replicationProjection(snapshot);
    case "activity": return activityProjection(snapshot);
    case "flow":
    default: return flowProjection(snapshot);
  }
}

export function chartLayout(flowLayout: { positions: Record<string, XYPosition>; viewport: { x: number; y: number; zoom: number } | null; views?: Partial<Record<ChartVariantId, { positions: Record<string, XYPosition>; viewport: { x: number; y: number; zoom: number } | null }>> } | undefined, variant: ChartVariantId) {
  const legacy = variant === "flow" ? flowLayout : undefined;
  return flowLayout?.views?.[variant] ?? legacy ?? { positions: {}, viewport: null };
}
