import type { KafkaLabSnapshot } from "../hooks/useKafkaLab";
import type { ChartProjection } from "./chart-views";
import { buildTransitTokens, numberMetadata, stringMetadata, type ChartTransitToken, type VisualEvent } from "./flow-transit";

export const CHART_ACTIVITY_CATEGORIES = [
  { id: "produced", label: "Produced", types: ["message-produced"] },
  { id: "read", label: "Read", types: ["consumer-polled", "record-processing-retried"] },
  { id: "processed", label: "Processed", types: ["record-processed"] },
  { id: "failed", label: "Failed", types: ["record-processing-failed", "lost-delivery"] },
  { id: "committed", label: "Committed", types: ["offset-committed", "offset-commit-blocked"] },
  { id: "rebalance", label: "Rebalance", types: ["rebalance-started", "assignments-revoked", "assignment-created", "rebalance-completed", "consumer-joined", "consumer-left", "consumer-restarted", "consumer-crashed"] },
  { id: "broker", label: "Broker", types: ["broker-failed", "broker-recovered", "leader-elected"] },
];

export const CHART_ACTIVITY_EVENT_TYPES = new Set(CHART_ACTIVITY_CATEGORIES.flatMap((category) => category.types));

/** Capture a real displayed read-position change on its matching group transition. */
export function captureOffsetChanges(events: VisualEvent[], snapshot?: KafkaLabSnapshot, previous?: KafkaLabSnapshot | null): VisualEvent[] {
  if (!snapshot || !previous || snapshot.selectedGroupId !== previous.selectedGroupId || snapshot.resetRevision !== previous.resetRevision) return events;
  const changes = new Map<string, NonNullable<VisualEvent["offsetChanges"]>>();
  for (const partition of snapshot.partitions) {
    const priorPartition = previous.partitions.find((candidate) => candidate.id === partition.id);
    const before = priorPartition?.currentOffset;
    const current = partition.currentOffset;
    if (before === undefined || current === undefined || before === current) continue;
    const cause = events.findLast((event) => ["consumer-crashed", "consumer-restarted", "assignment-created"].includes(event.type)
      && stringMetadata(event, "groupId") === snapshot.selectedGroupId
      && ((event.type === "assignment-created" && numberMetadata(event, "partition") === partition.id)
        || (event.type === "consumer-restarted" && stringMetadata(event, "consumerId") === partition.assignedConsumerId)
        || (event.type === "consumer-crashed" && stringMetadata(event, "consumerId") === priorPartition?.assignedConsumerId)));
    if (!cause) continue;
    const list = changes.get(cause.id) ?? [];
    list.push({ partition: partition.id, previous: before, current });
    changes.set(cause.id, list);
  }
  return events.map((event) => changes.has(event.id) ? { ...event, offsetChanges: changes.get(event.id) } : event);
}

/** Project real transitions onto the current chart's relationships, never into domain state. */
export function buildChartTransitTokens(events: VisualEvent[], snapshot: KafkaLabSnapshot, projection: ChartProjection): ChartTransitToken[] {
  const group = snapshot.consumerGroups.find((candidate) => candidate.id === snapshot.selectedGroupId) ?? snapshot.consumerGroups[0];
  const edges = new Set(projection.edges.map((edge) => edge.id));
  const tokens: ChartTransitToken[] = [];
  // Apply the visual cap after selecting this chart's relationships, so busy
  // producers cannot evict unrelated consumer/checkpoint activity prematurely.
  const base = buildTransitTokens(events, snapshot, group, Infinity);
  const add = (token: ChartTransitToken, edgeId: string, overrides: Partial<ChartTransitToken> = {}) => {
    if (!edges.has(edgeId)) return;
    tokens.push({ ...token, ...overrides, edgeId, id: `${projection.variant}:${token.id}:${edgeId}` });
  };
  const signal = (event: VisualEvent, edgeId: string, label: string, recordIdentity = true, overrides: Partial<ChartTransitToken> = {}) => add({
    id: event.id, sourceEventId: event.id, edgeId, label, phase: event.type,
    partition: numberMetadata(event, "partition"), consumerId: stringMetadata(event, "consumerId"),
    groupId: stringMetadata(event, "groupId"), createdAt: event.visualStartedAt,
    messageId: recordIdentity ? stringMetadata(event, "messageId") ?? (["message-produced", "consumer-polled", "record-processed", "record-processing-failed", "record-processing-retried", "lost-delivery"].includes(event.type) ? event.entityId : undefined) : undefined,
    offset: recordIdentity ? numberMetadata(event, "offset") : undefined,
    attempt: recordIdentity ? numberMetadata(event, "attempt") : undefined,
    generation: event.visualRevision, delayMs: 0, durationMs: 720,
  }, edgeId, overrides);

  for (const event of events) {
    if (projection.variant === "activity") {
      const category = CHART_ACTIVITY_CATEGORIES.find((category) => category.types.includes(event.type));
      if (category) signal(event, `activity-root-${category.id}`, event.type === "offset-commit-blocked" ? "Commit blocked" : event.type === "lost-delivery" ? "Skipped" : event.type === "record-processing-retried" ? "Retry" : category.label);
      continue;
    }
    const eventTokens = base.filter((token) => token.sourceEventId === event.id);
    for (const token of eventTokens) {
      switch (projection.variant) {
        case "flow": add(token, token.edgeId); break;
        case "partitions":
          if (token.phase === "routed" || token.phase === "polled") add(token, token.edgeId, { delayMs: 0 });
          break;
        case "consumers":
          if (token.phase === "polled") {
            // The ownership link is drawn consumer → partition; reads travel back to the owner.
            add(token, `consumer-${token.consumerId}-partition-${token.partition}`, { reverse: true });
            signal(event, `group-consumer-${token.consumerId}`, `P${token.partition} read`);
          }
          if (token.phase === "committed") signal(event, `group-consumer-${token.consumerId}`, `P${token.partition} checkpoint`);
          break;
        case "offsets": {
          const metric = token.phase === "routed" ? "end" : token.phase === "polled" ? "current" : token.phase === "committed" ? "committed" : undefined;
          if (metric) add(token, `offset-${token.partition}-${metric}`, { delayMs: 0, label: `P${token.partition} ${metric === "end" ? "LEO" : metric === "current" ? "P" : "C"}` });
          break;
        }
        case "delivery":
          if (token.phase === "polled") add(token, "delivery-queued-in-flight");
          // A checkpoint can include skipped work. Never turn that into a success animation.
          if (token.phase === "committed") {
            const skipped = events.some((candidate) => candidate.type === "lost-delivery" && stringMetadata(candidate, "groupId") === group?.id
              && (stringMetadata(candidate, "messageId") ?? candidate.entityId) === token.messageId);
            const message = snapshot.partitions.find((partition) => partition.id === token.partition)?.messages.find((message) => message.id === token.messageId);
            if (!skipped && message?.delivery?.applicationStatus === "succeeded") add(token, "delivery-succeeded-committed");
          }
          break;
        case "replication":
          if (token.phase === "routed") {
            add(token, `replication-topic-${token.partition}`, { delayMs: 0 });
            const assignment = snapshot.cluster?.assignments.find((assignment) => assignment.topic === snapshot.topicName && assignment.partition === token.partition);
            // @Simplification: highlight modeled ISR placement, not measured broker-to-broker traffic.
            assignment?.inSyncReplicas.filter((brokerId) => snapshot.cluster?.brokers.some((broker) => broker.id === brokerId && broker.status === "up"))
              .forEach((brokerId) => signal(event, `replication-replica-${token.partition}-${brokerId}`, "ISR placement", false));
          }
          break;
      }
    }

    const partition = numberMetadata(event, "partition");
    const consumerId = stringMetadata(event, "consumerId");
    const eventGroupId = stringMetadata(event, "groupId");
    if (projection.variant === "offsets" && eventGroupId === group?.id) for (const change of event.offsetChanges ?? []) {
      signal(event, `offset-${change.partition}-current`, `P ${change.previous}→${change.current}`, false,
        { partition: change.partition, previousOffset: change.previous, currentOffset: change.current });
    }
    if (projection.variant === "consumers" && eventGroupId === group?.id) {
      if (CHART_ACTIVITY_CATEGORIES[5].types.includes(event.type) && consumerId) {
        signal(event, `group-consumer-${consumerId}`, "assignment");
        if (partition !== undefined && group?.assignments[partition] === consumerId) signal(event, `consumer-${consumerId}-partition-${partition}`, `P${partition} owner`);
      }
      if (["record-processed", "record-processing-failed", "record-processing-retried"].includes(event.type) && consumerId) {
        const messageId = stringMetadata(event, "messageId") ?? event.entityId;
        const message = snapshot.partitions.find((candidate) => candidate.id === partition)?.messages.find((candidate) => candidate.id === messageId && candidate.offset === numberMetadata(event, "offset"));
        if (message && group?.assignments[message.partition] === consumerId) signal(event, `group-consumer-${consumerId}`, event.type === "record-processed" ? "processed" : event.type === "record-processing-failed" ? "failed" : "retry");
      }
    }
    if (projection.variant === "partitions" && event.type === "assignment-created" && eventGroupId === group?.id && partition !== undefined && consumerId && group?.assignments[partition] === consumerId) {
      signal(event, `partition-consumer-${partition}-${consumerId}`, "owner assigned");
    }
    if (projection.variant === "delivery" && eventGroupId === group?.id) {
      const offset = numberMetadata(event, "offset");
      const messageId = stringMetadata(event, "messageId") ?? event.entityId;
      const message = snapshot.partitions.find((candidate) => candidate.id === partition)?.messages.find((candidate) => candidate.offset === offset && candidate.id === messageId);
      if (!message || !consumerId || group?.assignments[message.partition] !== consumerId) continue;
      const token: ChartTransitToken = {
        id: event.id, sourceEventId: event.id, messageId: message.id, partition: message.partition, offset: message.offset,
        consumerId, groupId: eventGroupId, attempt: numberMetadata(event, "attempt") ?? message.delivery?.attempt,
        phase: event.type, label: (message.key ?? `P${message.partition}:${message.offset}`).slice(0, 14), edgeId: "",
        delayMs: 0, durationMs: 680, createdAt: event.visualStartedAt, generation: event.visualRevision,
      };
      if (event.type === "record-processed") {
        add(token, "delivery-in-flight-succeeded");
      }
      if (event.type === "record-processing-failed") add(token, "delivery-in-flight-failed");
      if (event.type === "record-processing-retried") add(token, "delivery-failed-in-flight");
      if (event.type === "lost-delivery") add(token, "delivery-in-flight-skipped");
    }
    if (projection.variant === "replication" && ["broker-failed", "broker-recovered", "leader-elected"].includes(event.type)) {
      const brokerId = event.entityId;
      snapshot.cluster?.assignments.filter((assignment) => assignment.topic === snapshot.topicName && (partition === undefined || assignment.partition === partition))
        .filter((assignment) => brokerId && assignment.replicas.includes(brokerId))
        .forEach((assignment) => signal(event, `replication-replica-${assignment.partition}-${brokerId}`, event.type === "broker-failed" ? "failed" : event.type === "broker-recovered" ? "recovered" : "leader"));
    }
  }
  return [...new Map(tokens.map((token) => [token.id, token])).values()].slice(-24);
}
