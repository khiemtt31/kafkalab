import type { KafkaLabSnapshot, LabMessage, LabTimelineEvent } from "../hooks/useKafkaLab";

export type TransitPhase = "produced" | "routed" | "polled" | "committed";

/**
 * A transit is a visual projection of one real event. It deliberately lives
 * outside React Flow's node model and is never sent to the persistence layer.
 * The same message ID can have several attempts after a duplicate poll.
 */
export type TransitToken = {
  id: string;
  sourceEventId: string;
  messageId: string;
  partition: number;
  offset: number;
  phase: TransitPhase;
  attempt: number;
  producerId?: string;
  consumerId?: string;
  groupId?: string;
  edgeId: string;
  label: string;
  delayMs: number;
  durationMs: number;
  /** Browser presentation clock; excluded from stored simulator events. */
  createdAt?: number;
  generation?: number;
};

export function transitMotionEnabled(reducedMotion: boolean, pageVisible: boolean) {
  return !reducedMotion && pageVisible;
}

export function numberMetadata(event: LabTimelineEvent, key: string): number | undefined {
  const value = event.metadata?.[key];
  return typeof value === "number" ? value : undefined;
}

export function stringMetadata(event: LabTimelineEvent, key: string): string | undefined {
  const value = event.metadata?.[key];
  return typeof value === "string" ? value : undefined;
}

function stringListMetadata(event: LabTimelineEvent, key: string): string[] | undefined {
  const value = event.metadata?.[key];
  return Array.isArray(value) && value.every((item) => typeof item === "string") ? value : undefined;
}

function messageForEvent(snapshot: KafkaLabSnapshot, event: LabTimelineEvent): LabMessage | undefined {
  const partition = numberMetadata(event, "partition");
  const offset = numberMetadata(event, "offset");
  if (partition === undefined || offset === undefined) return undefined;
  const message = snapshot.partitions.find((candidate) => candidate.id === partition)?.messages.find((candidate) => candidate.offset === offset);
  if (!message) return undefined;
  const entityId = (event as LabTimelineEvent & { entityId?: unknown }).entityId;
  const declaredMessageId = stringMetadata(event, "messageId") ?? (typeof entityId === "string" ? entityId : undefined);
  if ((event.type === "message-produced" || event.type === "consumer-polled") && !declaredMessageId) return undefined;
  return declaredMessageId && declaredMessageId !== message.id ? undefined : message;
}

function tokenLabel(message: LabMessage, attempt: number, phase: TransitPhase) {
  if (phase === "committed") return `P${message.partition} checkpoint`;
  const suffix = attempt > 1 ? ` · a${attempt}` : "";
  const base = message.key?.replace(/^order-/i, "") || `record ${message.offset}`;
  const available = 10;
  return `${base.slice(0, available)}${base.length > available ? "…" : ""}${suffix}`;
}

function latestPollAttempt(events: LabTimelineEvent[], groupId: string, consumerId: string, partition: number, offset: number, fallback = 1) {
  return events.reduce((latest, event) => {
    if (event.type !== "consumer-polled" || stringMetadata(event, "groupId") !== groupId || stringMetadata(event, "consumerId") !== consumerId) return latest;
    if (numberMetadata(event, "partition") !== partition || numberMetadata(event, "offset") !== offset) return latest;
    return Math.max(latest, numberMetadata(event, "attempt") ?? latest);
  }, fallback);
}

export type VisualEvent = LabTimelineEvent & {
  visualStartedAt?: number;
  visualRevision?: number;
  offsetChanges?: Array<{ partition: number; previous: number; current: number }>;
};

/** Relationship signals need not represent a record (for example a rebalance). */
export type ChartTransitToken = Omit<TransitToken, "messageId" | "partition" | "offset" | "attempt" | "phase"> & {
  messageId?: string;
  partition?: number;
  offset?: number;
  attempt?: number;
  phase: string;
  reverse?: boolean;
  previousOffset?: number;
  currentOffset?: number;
};
export function buildTransitTokens(events: VisualEvent[], snapshot: KafkaLabSnapshot, group: KafkaLabSnapshot["consumerGroups"][number] | undefined, limit = 24): TransitToken[] {
  if (!group) return [];
  const tokens: TransitToken[] = [];
  const addToken = (event: VisualEvent, message: LabMessage, phase: TransitPhase, edgeId: string, attempt: number, delayMs = 0, durationMs = 720, routing: { producerId?: string; consumerId?: string; groupId?: string } = {}) => {
    tokens.push({
      id: `${event.id}:${phase}:${message.id}:${attempt}:${edgeId}`,
      sourceEventId: event.id,
      messageId: message.id,
      partition: message.partition,
      offset: message.offset,
      phase,
      attempt,
      producerId: routing.producerId,
      consumerId: routing.consumerId,
      groupId: routing.groupId,
      edgeId,
      label: phase === "committed" && numberMetadata(event, "committedOffset") !== undefined
        ? `P${message.partition} C→${numberMetadata(event, "committedOffset")}`
        : tokenLabel(message, attempt, phase),
      delayMs,
      durationMs,
      createdAt: event.visualStartedAt,
      generation: event.visualRevision,
    });
  };

  for (const event of events) {
    const partition = numberMetadata(event, "partition");
    if (partition === undefined) continue;
    const message = messageForEvent(snapshot, event);

    if (event.type === "message-produced") {
      if (!message) continue;
      const producerId = stringMetadata(event, "producerId");
      // Legacy adapters expose one source. Real producer snapshots require the
      // event's exact source so an unknown producer cannot light another path.
      if (snapshot.producers === undefined) addToken(event, message, "produced", "producer-topic", 1, 0, 680, { producerId });
      else if (producerId && (!message.producerId || message.producerId === producerId)
        && snapshot.producers.some((producer) => producer.id === producerId)) addToken(event, message, "produced", `producer-${producerId}-topic`, 1, 0, 680, { producerId });
      addToken(event, message, "routed", `topic-partition-${partition}`, 1, 760, 680);
      continue;
    }

    if (event.type === "consumer-polled") {
      const eventGroupId = stringMetadata(event, "groupId");
      const consumerId = stringMetadata(event, "consumerId");
      if (!message || eventGroupId !== group.id || !consumerId || group.assignments[partition] !== consumerId || !group.consumers.some((consumer) => consumer.id === consumerId)) continue;
      addToken(event, message, "polled", `partition-consumer-${partition}-${consumerId}`, numberMetadata(event, "attempt") ?? 1, 0, 720, { consumerId, groupId: eventGroupId });
      continue;
    }

    if (event.type === "offset-committed") {
      const eventGroupId = stringMetadata(event, "groupId");
      const consumerId = stringMetadata(event, "consumerId");
      const previous = numberMetadata(event, "previous");
      const committedOffset = numberMetadata(event, "committedOffset");
      const committedMessageIds = stringListMetadata(event, "messageIds");
      if (eventGroupId !== group.id || !consumerId || !group.consumers.some((candidate) => candidate.id === consumerId)) continue;
      const partitionMessages = snapshot.partitions.find((candidate) => candidate.id === partition)?.messages ?? [];
      const committedMessages = committedMessageIds
        ? partitionMessages.filter((candidate) => committedMessageIds.includes(candidate.id))
        : previous !== undefined && committedOffset !== undefined && committedOffset > previous
          ? partitionMessages.filter((candidate) => candidate.offset >= previous && candidate.offset < committedOffset)
          : [];
      for (const committedMessage of committedMessages) {
        const attempt = committedMessage.delivery?.groupId === group.id ? committedMessage.delivery.attempt : 1;
        addToken(event, committedMessage, "committed", `consumer-commit-${consumerId}`, latestPollAttempt(events, group.id, consumerId, partition, committedMessage.offset, attempt), 0, 720, { consumerId, groupId: eventGroupId });
      }
    }
  }

  // The event stream remains the complete history. Only the newest visual
  // transits are mounted so a batch or fast producer cannot grow the SVG.
  return tokens.slice(-limit);
}
