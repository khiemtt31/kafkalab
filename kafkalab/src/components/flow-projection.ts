export type ProjectionDelivery = {
  status?: string;
  applicationStatus?: "pending" | "succeeded" | "failed";
  failureReason?: string;
  consumerId?: string;
  attempt?: number;
};

export type ProjectionMessage = {
  offset: number;
  key?: string | null;
  delivery?: ProjectionDelivery;
};

export type RecordState = "queued" | "in-flight" | "processed" | "succeeded" | "failed" | "committed" | "skipped";

export function recordState(message: ProjectionMessage, committed: number): RecordState {
  if (message.delivery?.applicationStatus === "failed") return "failed";
  if (message.delivery?.status === "lost") return "skipped";
  if (message.delivery?.status === "committed") return message.delivery.applicationStatus === "succeeded" ? "committed" : "skipped";
  if (message.delivery?.applicationStatus === "succeeded") return "succeeded";
  if (message.delivery?.status === "processed") return "processed";
  if (message.offset < committed) return "skipped";
  if (message.delivery?.applicationStatus === "pending" || message.delivery?.status === "read" || message.delivery?.status === "duplicate") return "in-flight";
  return "queued";
}

export function recordStateLabel(state: RecordState) {
  return state === "in-flight" ? "in flight" : state;
}

export function recordStateClass(state: RecordState) {
  return `is-${state}`;
}

export function visiblePartitionMessages<T extends { offset: number }>(messages: readonly T[], limit = 6) {
  const ordered = [...messages].sort((left, right) => left.offset - right.offset);
  const size = Math.max(1, Math.floor(limit));
  const failed = ordered.find((message) => (message as ProjectionMessage).delivery?.applicationStatus === "failed");
  if (!failed || ordered.length <= size) return ordered.slice(-size);
  // Keep the oldest unresolved failure reachable even as later records arrive.
  const newest = ordered.filter((message) => message !== failed).slice(-(size - 1));
  return (size === 1 ? [failed] : [failed, ...newest]).sort((left, right) => left.offset - right.offset);
}

export function projectPartitionStats<T extends ProjectionMessage>(messages: readonly T[], committed: number) {
  let read = 0;
  let processed = 0;
  let failed = 0;
  let committedCount = 0;
  for (const message of messages) {
    if (message.delivery) read += 1;
    const state = recordState(message, committed);
    if (state === "processed" || state === "succeeded" || state === "committed") processed += 1;
    if (state === "committed") committedCount += 1;
    if (state === "failed") failed += 1;
  }
  return { read, processed, failed, committed: committedCount };
}

/** Compact counts are projected from the selected group's latest delivery state. */
export function projectPartitionSummary<T extends ProjectionMessage>(messages: readonly T[], committed: number, position?: { current: number; logEndOffset: number }) {
  const orders = new Set<string>();
  let unread = 0;
  let inFlight = 0;
  let skipped = 0;
  for (const message of messages) {
    if (message.key?.startsWith("order-")) orders.add(message.key);
    const state = recordState(message, committed);
    if (state === "queued") unread += 1;
    if (state === "in-flight") inFlight += 1;
    if (state === "skipped") skipped += 1;
  }
  // A rewind can make previously delivered records readable again. Backlog
  // follows the current cursor, while outcomes retain the latest group attempt.
  if (position) unread = Math.max(0, position.logEndOffset - position.current);
  return { orders: orders.size, records: messages.length, unread, inFlight, skipped, ...projectPartitionStats(messages, committed) };
}

/** Record volume never changes the overview's canonical topology spacing. */
export function compactFlowPositions(producerIds: readonly string[] | undefined, partitionIds: readonly number[], consumerIds: readonly string[]) {
  const partitionHeight = 142;
  const consumerHeight = 132;
  const producerHeight = 110;
  const rowGap = 166;
  const producerGap = 144;
  const producerCount = producerIds?.length ?? 1;
  const stackHeight = (count: number, gap: number, height: number) => count ? (count - 1) * gap + height : 0;
  const graphHeight = Math.max(142, stackHeight(partitionIds.length, rowGap, partitionHeight), stackHeight(consumerIds.length, rowGap, consumerHeight), stackHeight(producerCount, producerGap, producerHeight));
  const positions: Record<string, { x: number; y: number }> = {
    topic: { x: 240, y: (graphHeight - 122) / 2 },
    commit: { x: 1150, y: (graphHeight - 142) / 2 },
  };
  const producerStart = (graphHeight - stackHeight(producerCount, producerGap, producerHeight)) / 2;
  if (producerIds === undefined) positions.producer = { x: 0, y: producerStart };
  else producerIds.forEach((id, index) => { positions[`producer-${id}`] = { x: 0, y: producerStart + index * producerGap }; });
  const partitionStart = (graphHeight - stackHeight(partitionIds.length, rowGap, partitionHeight)) / 2;
  partitionIds.forEach((id, index) => { positions[`partition-${id}`] = { x: 500, y: partitionStart + index * rowGap }; });
  const consumerStart = (graphHeight - stackHeight(consumerIds.length, rowGap, consumerHeight)) / 2;
  consumerIds.forEach((id, index) => { positions[`consumer-${id}`] = { x: 850, y: consumerStart + index * rowGap }; });
  return positions;
}
