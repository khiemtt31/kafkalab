import { addEvent, cloneSimulationState, nextId, result } from "./simulation";
import { selectPartition } from "./partitioner";
import type {
  KafkaMessage,
  KafkaSimulationState,
  ProducedMessageInput,
  Producer,
  SimulationEvent,
  SimulationResult,
} from "./types";

export function addProducer(state: KafkaSimulationState, id?: string): SimulationResult<Producer> {
  const next = cloneSimulationState(state);
  const producer: Producer = { id: id ?? nextId(next, "producer", "producer"), producedCount: 0 };
  if (next.producers.some((candidate) => candidate.id === producer.id)) {
    throw new Error(`Producer ${producer.id} already exists`);
  }
  next.producers.push(producer);
  return result(next, producer);
}

function ensureProducer(state: KafkaSimulationState, id: string): Producer {
  let producer = state.producers.find((candidate) => candidate.id === id);
  if (!producer) {
    producer = { id, producedCount: 0 };
    state.producers.push(producer);
  }
  return producer;
}

// @Producer
// Conceptually, a Kafka producer sends a record through a broker to a topic
// partition. This educational engine skips the network and appends directly.
export function produceMessage(
  state: KafkaSimulationState,
  input: ProducedMessageInput,
): SimulationResult<KafkaMessage> {
  const next = cloneSimulationState(state);
  const producerId = input.producerId ?? "producer-1";
  const producer = ensureProducer(next, producerId);
  const key = input.key ?? null;
  const selected = selectPartition(next.topic, key);
  const partitionId = input.partition ?? selected.partition;
  const partition = next.topic.partitions.find((candidate) => candidate.id === partitionId);
  if (!partition) {
    throw new Error(`Partition ${partitionId} does not exist on topic ${next.topic.name}`);
  }
  if (input.partition === undefined) {
    next.topic.nextUnkeyedPartition = selected.nextUnkeyedPartition;
  }

  // @Offset
  // Offset allocation is local to this append-only partition and starts at 0.
  const message: KafkaMessage = {
    id: nextId(next, "message", "message"),
    producerId,
    key,
    value: input.value,
    timestamp: input.timestamp ?? next.clock,
    partition: partition.id,
    offset: partition.logEndOffset,
  };
  partition.messages.push(message);
  partition.logEndOffset += 1;
  producer.producedCount += 1;
  const event = addEvent(
    next,
    "message-produced",
    `Produced ${message.id} to ${next.topic.name}[${message.partition}] at offset ${message.offset}`,
    "producer → topic → partition → offset",
    message.id,
    {
      producerId,
      key: key ?? null,
      value: message.value,
      partition: message.partition,
      offset: message.offset,
    },
  );
  return result(next, message, [event]);
}

export function produceBatch(
  state: KafkaSimulationState,
  inputs: ProducedMessageInput[],
): SimulationResult<KafkaMessage[]> {
  let current = state;
  const messages: KafkaMessage[] = [];
  const events: SimulationEvent[] = [];
  for (const input of inputs) {
    const produced = produceMessage(current, input);
    current = produced.state;
    messages.push(produced.value);
    events.push(...produced.events);
  }
  return result(current, messages, events);
}

export function produceSyntheticBatch(
  state: KafkaSimulationState,
  count: number,
  options: Omit<ProducedMessageInput, "value"> & { valuePrefix?: string } = {},
): SimulationResult<KafkaMessage[]> {
  const inputs = Array.from({ length: Math.max(0, Math.floor(count)) }, (_, index) => ({
    producerId: options.producerId,
    key: options.key,
    partition: options.partition,
    timestamp: options.timestamp,
    value: `${options.valuePrefix ?? "message"}-${index + 1}`,
  }));
  return produceBatch(state, inputs);
}
