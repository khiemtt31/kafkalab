import { createBrokerCluster, setRequiredAcks } from "./cluster";
import { failRecordProcessing, pollConsumer, processRecords } from "./consumer";
import { commitProcessedOffsets } from "./offsets";
import { produceMessage } from "./producer";
import { addConsumer } from "./rebalance";
import { advanceClock, createSimulationState } from "./simulation";
import type { KafkaSimulationState } from "./types";

export const REFERENCE_GROUP = "analytics-group";

/**
 * Static local teaching data, assembled through the same pure domain actions
 * as the playground. Loading it never starts playback or contacts a broker.
 * Explicit partitions demonstrate independent logs; no global topic order is
 * implied by the different business IDs or the fixed example timestamps.
 */
export function createReferenceFlow(): KafkaSimulationState {
  const startedAt = Date.UTC(2026, 0, 1, 9);
  let state = createSimulationState({
    topicName: "orders",
    partitionCount: 4,
    producerIds: ["checkout-api", "payment-service", "shipping-service"],
    startTime: startedAt,
  });
  state = createBrokerCluster(state, 3, 3, 2).state;
  state = setRequiredAcks(state, "all").state;
  for (let index = 0; index < 4; index += 1) {
    state = addConsumer(state, REFERENCE_GROUP).state;
  }

  const orders = [
    { partition: 0, key: "order-7F3K9" },
    { partition: 0, key: "order-N8R4C" },
    { partition: 1, key: "order-Q2M81" },
    { partition: 1, key: "order-A4D92" },
    { partition: 2, key: "order-R8H27" },
    { partition: 2, key: "order-B6T90" },
    { partition: 3, key: "order-V5X14" },
    { partition: 3, key: "order-C9L36" },
  ];
  const phases = [
    { producerId: "checkout-api", value: "order-created" },
    { producerId: "payment-service", value: "order-paid" },
    { producerId: "shipping-service", value: "order-shipped" },
  ];
  for (const phase of phases) {
    for (const order of orders) {
      state = produceMessage(state, { ...order, ...phase, timestamp: state.clock }).state;
      state = advanceClock(state, 100);
    }
  }
  state = pollConsumer(state, REFERENCE_GROUP, "consumer-0", { partition: 0 }).state;
  state = processRecords(state, REFERENCE_GROUP, "consumer-0", { partition: 0, offset: 0 }).state;
  state = commitProcessedOffsets(state, REFERENCE_GROUP, "consumer-0", [0]).state;
  state = advanceClock(state, 100);
  state = pollConsumer(state, REFERENCE_GROUP, "consumer-0", { partition: 0 }).state;
  state = processRecords(state, REFERENCE_GROUP, "consumer-0", { partition: 0, offset: 1 }).state;
  state = advanceClock(state, 100);
  state = pollConsumer(state, REFERENCE_GROUP, "consumer-1", { partition: 1 }).state;
  state = failRecordProcessing(state, REFERENCE_GROUP, "consumer-1", {
    partition: 1, offset: 0, reason: "Inventory service timed out before processing the order.",
  }).state;
  state = advanceClock(state, 100);
  state = pollConsumer(state, REFERENCE_GROUP, "consumer-2", { partition: 2 }).state;
  state = processRecords(state, REFERENCE_GROUP, "consumer-2", { partition: 2, offset: 0 }).state;
  state = commitProcessedOffsets(state, REFERENCE_GROUP, "consumer-2", [2]).state;
  state = advanceClock(state, 100);
  state = pollConsumer(state, REFERENCE_GROUP, "consumer-3", { partition: 3 }).state;
  return state;
}
