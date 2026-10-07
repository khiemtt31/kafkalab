import { commitOffsets } from "./offsets";
import { pollConsumer, processRecords } from "./consumer";
import { addEvent, getGroup, advanceClock, appendMetric, appendLagSnapshot, cloneSimulationState, getGroup as findGroup, result } from "./simulation";
import { getGroupLag } from "./lag";
import { canProduceWithAcks } from "./cluster";
import { produceMessage } from "./producer";
import { selectPartition } from "./partitioner";
import type { KafkaMessage, KafkaSimulationState, MetricsPoint, SimulationEvent, SimulationResult } from "./types";

export type TickOptions = {
  elapsedMs?: number;
  producerRate?: number;
  consumerRate?: number;
  producerId?: string;
  autoCommit?: boolean;
};

// @KafkaConcept
// The logical simulation clock is independent from React rendering. A tick may
// process a batch, while the UI can sample the resulting state less frequently.
export function tickSimulation(state: KafkaSimulationState, options: TickOptions = {}): SimulationResult<MetricsPoint> {
  const elapsedMs = Math.max(1, Math.floor(options.elapsedMs ?? 100));
  let current = advanceClock(state, elapsedMs);
  const events: SimulationEvent[] = [];
  const beforeProduced = current.topic.partitions.reduce((total, partition) => total + partition.messages.length, 0);
  const producerRate = Math.max(0, options.producerRate ?? 0);
  const toProduce = Math.floor((producerRate * elapsedMs) / 1000);
  if (toProduce > 0) {
    for (let index = 0; index < toProduce; index += 1) {
      const key = null;
      const partition = selectPartition(current.topic, key).partition;
      const acknowledgement = canProduceWithAcks(current, partition);
      if (!acknowledgement.accepted) {
        const rejected = cloneSimulationState(current);
        events.push(addEvent(rejected, "produce-rejected", `Tick produce rejected for ${rejected.topic.name}[${partition}]`, "required acknowledgements", `partition-${partition}`, {
          partition,
          reason: acknowledgement.reason,
          inSyncReplicas: acknowledgement.inSyncReplicas,
        }));
        current = rejected;
        continue;
      }
      const produced = produceMessage(current, { producerId: options.producerId, key, value: `tick-${index + 1}` });
      current = produced.state;
      events.push(...produced.events);
    }
  }

  const consumerRate = Math.max(0, options.consumerRate ?? 0);
  const budget = Math.floor((consumerRate * elapsedMs) / 1000);
  let consumed = 0;
  if (budget > 0) {
    for (const group of [...current.groups]) {
      let remaining = budget;
      const active = group.consumers.filter((consumer) => consumer.status === "running").sort((left, right) => left.id.localeCompare(right.id));
      let cursor = 0;
      while (remaining > 0 && active.length > 0) {
        const consumer = active[cursor % active.length];
        const polled = pollConsumer(current, group.id, consumer.id, { maxRecords: 1 });
        current = polled.state;
        events.push(...polled.events);
        if (polled.value.length > 0) {
          const processed = processRecords(current, group.id, consumer.id);
          current = processed.state;
          events.push(...processed.events);
          if (options.autoCommit ?? true) {
            const committed = commitOffsets(current, group.id, consumer.id, "processed");
            current = committed.state;
            events.push(...committed.events);
          }
          consumed += polled.value.length;
          remaining -= polled.value.length;
        } else {
          remaining -= 1;
        }
        cursor += 1;
      }
    }
  }

  const produced = current.topic.partitions.reduce((total, partition) => total + partition.messages.length, 0) - beforeProduced;
  const totalLag = current.groups.reduce((total, group) => total + getGroupLag(current, group.id).total, 0);
  const metric: MetricsPoint = { at: current.clock, produced, consumed, committed: options.autoCommit ?? true ? consumed : 0, totalLag };
  const next = cloneSimulationState(current);
  appendMetric(next, metric);
  for (const group of next.groups) {
    const lag = getGroupLag(next, group.id);
    appendLagSnapshot(next, { at: next.clock, groupId: group.id, total: lag.total, byPartition: lag.byPartition });
  }
  return result(next, metric, events);
}

export function runTicks(state: KafkaSimulationState, count: number, options: TickOptions = {}): SimulationResult<MetricsPoint[]> {
  let current = state;
  const points: MetricsPoint[] = [];
  const events: SimulationEvent[] = [];
  for (let index = 0; index < Math.max(0, Math.floor(count)); index += 1) {
    const tick = tickSimulation(current, options);
    current = tick.state;
    points.push(tick.value);
    events.push(...tick.events);
  }
  return result(current, points, events);
}
