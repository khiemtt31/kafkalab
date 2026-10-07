import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useKafkaStore } from "@/store/kafka-store";

describe("Kafka lab store adapter", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    useKafkaStore.getState().actions.reset();
  });

  afterEach(() => {
    useKafkaStore.getState().actions.reset();
    vi.useRealTimers();
  });

  it("produces records and appends a real metric sample", () => {
    const store = useKafkaStore.getState();
    store.actions.produce({ key: "user-42", value: "created" });
    const snapshot = useKafkaStore.getState().snapshot;

    expect(snapshot.messageCount).toBe(1);
    expect(snapshot.metrics.at(-1)?.produced).toBe(1);
    expect(snapshot.events.some((event) => event.type === "message-produced")).toBe(true);
  });

  it("preserves the selected producer for single records and batches", () => {
    const actions = useKafkaStore.getState().actions;
    actions.loadReferenceFlow?.();
    actions.produce({ producerId: "payment-service", key: "custom-order", value: "payment-received" });
    actions.produceBatch?.({ producerId: "shipping-service", count: 3, valuePrefix: "shipment" });
    actions.produce({ key: null, value: "default-producer" });
    const snapshot = useKafkaStore.getState().snapshot;
    expect(snapshot.producers).toEqual([
      { id: "checkout-api", producedCount: 9 },
      { id: "payment-service", producedCount: 9 },
      { id: "shipping-service", producedCount: 11 },
    ]);
    expect(snapshot.events.filter((event) => event.type === "message-produced").slice(-5).map((event) => event.metadata?.producerId)).toEqual([
      "payment-service", "shipping-service", "shipping-service", "shipping-service", "checkout-api",
    ]);
    const records = snapshot.partitions.flatMap((partition) => partition.messages);
    expect(records.find((record) => record.value === "payment-received")?.producerId).toBe("payment-service");
    expect(records.filter((record) => record.value.startsWith("shipment-")).map((record) => record.producerId)).toEqual(Array(3).fill("shipping-service"));
    expect(records.find((record) => record.value === "default-producer")?.producerId).toBe("checkout-api");
  });

  it("shares the total automatic rate among existing producers without inventing a producer", () => {
    const actions = useKafkaStore.getState().actions;
    actions.loadReferenceFlow?.();
    for (const consumer of useKafkaStore.getState().snapshot.consumers) actions.setConsumerRate?.(consumer.id, 0);
    actions.start?.();
    vi.advanceTimersByTime(1000);
    actions.pause?.();
    const snapshot = useKafkaStore.getState().snapshot;
    expect(snapshot.messageCount).toBe(36);
    expect(snapshot.producers).toEqual([
      { id: "checkout-api", producedCount: 12 },
      { id: "payment-service", producedCount: 12 },
      { id: "shipping-service", producedCount: 12 },
    ]);
    expect(snapshot.metrics.reduce((sum, point) => sum + point.produced, 0)).toBe(12);
  });

  it("adds distinct producers up to eight even when generated names already exist", () => {
    const actions = useKafkaStore.getState().actions;
    actions.addProducer?.("producer-0");
    actions.addProducer?.("producer-1");
    for (let index = 0; index < 20; index += 1) actions.addProducer?.();
    const snapshot = useKafkaStore.getState().snapshot;
    expect(snapshot.producers).toHaveLength(8);
    expect(new Set(snapshot.producers?.map((producer) => producer.id)).size).toBe(8);
    expect(snapshot.producers?.every((producer) => producer.producedCount === 0)).toBe(true);
    expect(snapshot.messageCount).toBe(0);
  });

  it("applies the same ISR admission rule to the transaction append control", () => {
    const actions = useKafkaStore.getState().actions;
    actions.loadReferenceFlow?.();
    actions.failBroker?.("broker-2");
    actions.failBroker?.("broker-3");
    actions.beginTransaction?.();
    const transaction = useKafkaStore.getState().simulation.advanced.transactions.at(-1)!;
    const count = useKafkaStore.getState().snapshot.messageCount;
    actions.appendTransactionMessage?.(transaction.id);
    expect(useKafkaStore.getState().snapshot.messageCount).toBe(count);
    expect(useKafkaStore.getState().simulation.advanced.transactions.at(-1)?.messageIds).toEqual([]);
    expect(useKafkaStore.getState().snapshot.events.at(-1)?.type).toBe("produce-rejected");
  });

  it("preserves produced-record identity in timeline events for transit matching", () => {
    const actions = useKafkaStore.getState().actions;
    actions.produce({ key: "same-order", value: "order-created" });
    actions.produce({ key: "same-order", value: "order-paid" });
    const snapshot = useKafkaStore.getState().snapshot;
    const producedEvents = snapshot.events.filter((event) => event.type === "message-produced");
    const records = snapshot.partitions.flatMap((partition) => partition.messages);

    expect(producedEvents).toHaveLength(2);
    expect(producedEvents.map((event) => event.entityId)).toEqual(records.map((record) => record.id));
    for (const event of producedEvents) {
      const record = records.find((candidate) => candidate.id === event.entityId);
      expect(record).toBeDefined();
      expect(event.metadata).toMatchObject({ partition: record!.partition, offset: record!.offset });
    }
  });

  it("loads and runs the domain scenario instead of only changing its label", () => {
    const store = useKafkaStore.getState();
    store.actions.loadScenario?.("keyed-partitioning");
    expect(useKafkaStore.getState().snapshot.messageCount).toBe(0);
    useKafkaStore.getState().actions.runScenario?.();
    const snapshot = useKafkaStore.getState().snapshot;
    const populatedPartitions = snapshot.partitions.filter((partition) => partition.messages.length > 0);

    expect(snapshot.activeScenario?.id).toBe("keyed-partitioning");
    expect(snapshot.messageCount).toBe(3);
    expect(populatedPartitions).toHaveLength(1);
  });

  it("shows one order's lifecycle in one ordered partition", () => {
    const actions = useKafkaStore.getState().actions;
    actions.loadScenario?.("order-lifecycle");
    actions.runScenario?.();
    const records = useKafkaStore.getState().snapshot.partitions.flatMap((partition) => partition.messages);
    expect(records.map((record) => record.value)).toEqual(["order-created", "payment-authorized", "inventory-reserved", "order-shipped"]);
    expect(records.map((record) => record.key)).toEqual(Array(4).fill("order-7F3K9"));
    expect(new Set(records.map((record) => record.partition)).size).toBe(1);
    expect(records.map((record) => record.offset)).toEqual([0, 1, 2, 3]);
  });

  it("caps a group's flow nodes at twelve consumers", () => {
    const actions = useKafkaStore.getState().actions;
    const groupId = useKafkaStore.getState().snapshot.selectedGroupId;
    for (let index = 0; index < 20; index += 1) actions.addConsumer(groupId);
    expect(useKafkaStore.getState().snapshot.consumers).toHaveLength(12);
  });

  it("keeps a crashed consumer dead until the explicit restart action", () => {
    const store = useKafkaStore.getState();
    store.actions.loadScenario?.("duplicate-processing");
    const consumerId = useKafkaStore.getState().snapshot.consumers[0]?.id;
    expect(consumerId).toBeDefined();

    store.actions.crashConsumer(consumerId as string);
    expect(useKafkaStore.getState().snapshot.consumers[0]?.status).toBe("dead");
    store.actions.restartConsumer(consumerId as string);
    expect(useKafkaStore.getState().snapshot.consumers[0]?.status).toBe("running");
  });

  it("runs the commit-too-soon lesson with skipped unprocessed work after restart", () => {
    const actions = useKafkaStore.getState().actions;
    actions.loadScenario?.("loss-before-processing");
    const loaded = useKafkaStore.getState();
    expect(loaded.snapshot.messageCount).toBe(1);
    expect(loaded.snapshot.partitions[0].currentOffset).toBe(1);
    expect(loaded.snapshot.partitions[0].committedOffset).toBe(0);
    expect(loaded.simulation.deliveries[0].status).toBe("read");
    expect(loaded.simulation.deliveries[0].processedAt).toBeUndefined();

    actions.runScenario?.();
    const { simulation, snapshot } = useKafkaStore.getState();
    expect(snapshot.activeScenario?.id).toBe("loss-before-processing");
    expect(snapshot.messageCount).toBe(1);
    expect(snapshot.partitions).toHaveLength(1);
    expect(snapshot.partitions[0].committedOffset).toBe(1);
    expect(snapshot.partitions[0].currentOffset).toBe(1);
    expect(snapshot.totalLag).toBe(0);
    expect(snapshot.running).toBe(false);
    expect(snapshot.events.filter((event) => event.type === "lost-delivery")).toHaveLength(1);
    expect(snapshot.events.filter((event) => event.type === "consumer-polled")).toHaveLength(1);
    expect(snapshot.events.some((event) => event.type === "consumer-crashed")).toBe(true);
    expect(snapshot.events.some((event) => event.type === "consumer-restarted")).toBe(true);
    expect(snapshot.events.some((event) => event.type === "record-processed")).toBe(false);
    expect(simulation.deliveries).toHaveLength(1);
    expect(simulation.deliveries[0].status).toBe("lost");
    expect(simulation.deliveries[0].processedAt).toBeUndefined();
    expect(snapshot.inspector?.title).toBe("Record skipped after restart");
    expect(snapshot.inspector?.state).toBe("Stored records: 1 · saved next offset: 1 · processed deliveries: 0 · records read after restart: 0");
    expect(snapshot.inspector?.relatedCode).toBe("src/core/kafka/offsets.ts");

    actions.runScenario?.();
    expect(useKafkaStore.getState().snapshot.messageCount).toBe(1);
    expect(useKafkaStore.getState().snapshot.events.filter((event) => event.type === "lost-delivery")).toHaveLength(1);
    expect(useKafkaStore.getState().snapshot.events.filter((event) => event.type === "consumer-polled")).toHaveLength(1);
  });

  it("does not invent lost work when committing a record already processed by a tick", () => {
    const actions = useKafkaStore.getState().actions;
    const consumerId = useKafkaStore.getState().snapshot.consumers[0].id;
    actions.setConsumerAutoCommit?.(consumerId, false);
    actions.produce({ key: null, value: "processed-before-manual-save" });
    actions.startReading?.();
    vi.advanceTimersByTime(1000);
    actions.pause?.();
    actions.commitCurrentConsumer(consumerId);

    const { snapshot, simulation } = useKafkaStore.getState();
    expect(snapshot.totalLag).toBe(0);
    expect(simulation.deliveries[0].status).toBe("committed");
    expect(simulation.deliveries[0].processedAt).toBeDefined();
    expect(snapshot.events.some((event) => event.type === "lost-delivery")).toBe(false);
  });

  it.each(["slow-consumer", "consumer-scaling", "rebalance"])("adds consumers safely after loading and running %s", (scenarioId) => {
    const actions = useKafkaStore.getState().actions;
    actions.loadScenario?.(scenarioId);
    expect(() => actions.runScenario?.()).not.toThrow();
    const groupId = useKafkaStore.getState().snapshot.selectedGroupId;
    const initialCount = useKafkaStore.getState().snapshot.consumers.length;
    for (let index = 0; index < 6; index += 1) {
      expect(() => actions.addConsumer(groupId)).not.toThrow();
    }

    const { snapshot, simulation } = useKafkaStore.getState();
    const ids = simulation.groups.flatMap((group) => group.consumers.map((consumer) => consumer.id));
    expect(snapshot.consumers).toHaveLength(initialCount + 6);
    expect(new Set(ids).size).toBe(ids.length);
    const assignedPartitions = snapshot.consumers.flatMap((consumer) => consumer.assignedPartitions);
    expect(new Set(assignedPartitions).size).toBe(snapshot.partitions.length);
    expect(assignedPartitions).toHaveLength(snapshot.partitions.length);
    expect(snapshot.partitions.every((partition) => ids.includes(partition.assignedConsumerId ?? ""))).toBe(true);
  });

  it("exposes the complete local policy state through the adapter", () => {
    const store = useKafkaStore.getState();
    store.actions.configurePolicies?.({ retries: 5, idempotence: true, compaction: true, cooperativeRebalance: true });
    const policies = useKafkaStore.getState().snapshot.advanced.policies;

    expect(policies.retries).toBe(5);
    expect(policies.idempotence).toBe(true);
    expect(policies.compaction).toBe(true);
    expect(policies.cooperativeRebalance).toBe(true);
  });

  it("uses required acknowledgements when a broker loses ISR capacity", () => {
    const store = useKafkaStore.getState();
    store.actions.createCluster?.();
    store.actions.setAcks?.("all");
    store.actions.failBroker?.("broker-1");
    store.actions.produce({ key: null, value: "blocked-by-acks" });
    const snapshot = useKafkaStore.getState().snapshot;

    expect(snapshot.messageCount).toBe(0);
    expect(snapshot.events.some((event) => event.type === "produce-rejected")).toBe(true);
  });

  it("applies the same acknowledgement gate to batch production", () => {
    const store = useKafkaStore.getState();
    store.actions.createCluster?.();
    store.actions.setAcks?.("all");
    store.actions.failBroker?.("broker-1");
    store.actions.produceBatch?.({ count: 3, valuePrefix: "blocked" });

    expect(useKafkaStore.getState().snapshot.messageCount).toBe(0);
    expect(useKafkaStore.getState().snapshot.events.filter((event) => event.type === "produce-rejected")).toHaveLength(3);
  });

  it("reads an example without generating additional records", () => {
    const actions = useKafkaStore.getState().actions;
    actions.produce({ key: "user-42", value: "one-example" });
    actions.startReading?.();
    actions.startReading?.();

    expect(vi.getTimerCount()).toBe(1);
    expect(useKafkaStore.getState().snapshot.running).toBe(true);
    expect(useKafkaStore.getState().snapshot.producerRunning).toBe(false);

    vi.advanceTimersByTime(1000);
    const snapshot = useKafkaStore.getState().snapshot;
    expect(snapshot.messageCount).toBe(1);
    expect(snapshot.totalLag).toBe(0);
    expect(snapshot.partitions.reduce((sum, partition) => sum + (partition.committedOffset ?? 0), 0)).toBe(1);
    expect(snapshot.metrics.reduce((sum, metric) => sum + metric.consumed, 0)).toBe(1);
    expect(snapshot.metrics.filter((metric) => Number(metric.timestamp) > 0).every((metric) => metric.produced === 0)).toBe(true);
  });

  it("keeps continuous production available and stops it when switching to reading", () => {
    const actions = useKafkaStore.getState().actions;
    actions.start?.();
    vi.advanceTimersByTime(1000);
    const messageCount = useKafkaStore.getState().snapshot.messageCount;
    expect(messageCount).toBeGreaterThan(0);
    expect(useKafkaStore.getState().snapshot.producerRunning).toBe(true);

    actions.startReading?.();
    vi.advanceTimersByTime(1000);
    expect(useKafkaStore.getState().snapshot.messageCount).toBe(messageCount);
    expect(useKafkaStore.getState().snapshot.producerRunning).toBe(false);
  });

  it.each([6, 10])("reads exactly %s records per simulated second at the configured rate", (rate) => {
    const actions = useKafkaStore.getState().actions;
    const consumerId = useKafkaStore.getState().snapshot.consumers[0].id;
    actions.produceBatch?.({ count: 30 });
    actions.setConsumerRate?.(consumerId, rate);
    actions.startReading?.();
    vi.advanceTimersByTime(1000);
    const snapshot = useKafkaStore.getState().snapshot;
    expect(snapshot.metrics.reduce((sum, metric) => sum + metric.consumed, 0)).toBe(rate);
    expect(snapshot.totalLag).toBe(30 - rate);
  });

  it("freezes reads at a zero rate without pausing the simulation", () => {
    const actions = useKafkaStore.getState().actions;
    const consumerId = useKafkaStore.getState().snapshot.consumers[0].id;
    actions.produceBatch?.({ count: 20 });
    actions.setConsumerRate?.(consumerId, 6);
    actions.startReading?.();
    vi.advanceTimersByTime(100);
    actions.setConsumerRate?.(consumerId, 0);
    vi.advanceTimersByTime(2000);

    const snapshot = useKafkaStore.getState().snapshot;
    expect(snapshot.running).toBe(true);
    expect(snapshot.consumers[0].rate).toBe(0);
    expect(snapshot.totalLag).toBe(20);
    expect(snapshot.metrics.every((metric) => metric.consumed === 0)).toBe(true);

    actions.setConsumerRate?.(consumerId, 10);
    vi.advanceTimersByTime(1000);
    expect(useKafkaStore.getState().snapshot.totalLag).toBe(10);
  });

  it.each([
    { requested: -10, expected: 1 },
    { requested: 0, expected: 1 },
    { requested: 3.9, expected: 3 },
    { requested: 99, expected: 8 },
    { requested: Number.NaN, expected: 3 },
    { requested: Number.POSITIVE_INFINITY, expected: 3 },
  ])("bounds a requested partition count of $requested to $expected", ({ requested, expected }) => {
    useKafkaStore.getState().actions.configureTopology?.({ topicName: "  payments  ", partitionCount: requested });
    const snapshot = useKafkaStore.getState().snapshot;
    expect(snapshot.topicName).toBe("payments");
    expect(snapshot.partitions).toHaveLength(expected);
    expect(snapshot.consumers).toHaveLength(1);
    expect(snapshot.consumerGroups).toHaveLength(1);
    expect(snapshot.consumers[0].assignedPartitions).toEqual(Array.from({ length: expected }, (_, index) => index));
  });

  it("trims, limits and defaults the configured topic name", () => {
    const actions = useKafkaStore.getState().actions;
    actions.configureTopology?.({ topicName: `  ${"a".repeat(100)}  `, partitionCount: 2 });
    expect(useKafkaStore.getState().snapshot.topicName).toBe("a".repeat(80));

    actions.configureTopology?.({ topicName: "   ", partitionCount: 2 });
    expect(useKafkaStore.getState().snapshot.topicName).toBe("orders");
  });

  it("applies topology as a paused fresh simulation without leaking previous settings", () => {
    const actions = useKafkaStore.getState().actions;
    const previousConsumer = useKafkaStore.getState().snapshot.consumers[0].id;
    actions.setConsumerRate?.(previousConsumer, 88);
    actions.setConsumerAutoCommit?.(previousConsumer, false);
    actions.setProducerRate?.(99);
    actions.setSimulationSpeed?.(4);
    actions.createCluster?.();
    actions.configurePolicies?.({ idempotence: true });
    actions.produce({ key: null, value: "old-record" });
    actions.start?.();
    vi.advanceTimersByTime(100);

    actions.configureTopology?.({ topicName: "payments", partitionCount: 4 });
    const snapshot = useKafkaStore.getState().snapshot;
    expect(snapshot.topicName).toBe("payments");
    expect(snapshot.partitions).toHaveLength(4);
    expect(snapshot.selectedGroupId).toBe("analytics-group");
    expect(snapshot.consumers).toHaveLength(1);
    expect(snapshot.consumers[0].rate).toBe(6);
    expect(snapshot.consumers[0].autoCommit).toBe(true);
    expect(snapshot.running).toBe(false);
    expect(snapshot.producerRunning).toBe(false);
    expect(snapshot.producerRate).toBe(12);
    expect(snapshot.simulationSpeed).toBe(1);
    expect(snapshot.messageCount).toBe(0);
    expect(snapshot.totalLag).toBe(0);
    expect(snapshot.metrics).toEqual([]);
    expect(snapshot.producedPerSecond).toBe(0);
    expect(snapshot.consumedPerSecond).toBe(0);
    expect(snapshot.cluster).toBeUndefined();
    expect(snapshot.activeScenario).toBeUndefined();
    expect(snapshot.advanced.policies.idempotence).toBe(false);
    expect(snapshot.events.some((event) => event.type === "message-produced")).toBe(false);
    expect(vi.getTimerCount()).toBe(0);

    vi.advanceTimersByTime(1000);
    expect(useKafkaStore.getState().snapshot.messageCount).toBe(0);
    expect(useKafkaStore.getState().simulation.clock).toBe(0);
  });

  it("increments the graph revision only when rebuilding the simulation", () => {
    const actions = useKafkaStore.getState().actions;
    const revision = useKafkaStore.getState().snapshot.resetRevision ?? 0;
    actions.produce({ key: null, value: "example" });
    actions.selectGroup?.("analytics-group");
    actions.startReading?.();
    vi.advanceTimersByTime(100);
    expect(useKafkaStore.getState().snapshot.resetRevision).toBe(revision);

    actions.configureTopology?.({ topicName: "custom", partitionCount: 2 });
    expect(useKafkaStore.getState().snapshot.resetRevision).toBe(revision + 1);
    actions.loadScenario?.("keyed-partitioning");
    expect(useKafkaStore.getState().snapshot.resetRevision).toBe(revision + 2);
    actions.loadScenario?.("unknown-scenario");
    expect(useKafkaStore.getState().snapshot.resetRevision).toBe(revision + 2);
    actions.reset();
    expect(useKafkaStore.getState().snapshot.resetRevision).toBe(revision + 3);
  });

  it("restores default consumer rates and commits on reset and scenario loading", () => {
    const actions = useKafkaStore.getState().actions;
    const originalConsumer = useKafkaStore.getState().snapshot.consumers[0].id;
    actions.setConsumerRate?.(originalConsumer, 88);
    actions.setConsumerAutoCommit?.(originalConsumer, false);
    actions.reset();
    expect(useKafkaStore.getState().snapshot.consumers[0].rate).toBe(6);
    expect(useKafkaStore.getState().snapshot.consumers[0].autoCommit).toBe(true);

    actions.loadScenario?.("consumer-scaling");
    actions.setConsumerRate?.("consumer-1", 88);
    actions.setConsumerAutoCommit?.("consumer-1", false);
    actions.start?.();
    actions.loadScenario?.("slow-consumer");
    expect(useKafkaStore.getState().snapshot.consumers[0].id).toBe("consumer-1");
    expect(useKafkaStore.getState().snapshot.consumers[0].rate).toBe(6);
    expect(useKafkaStore.getState().snapshot.consumers[0].autoCommit).toBe(true);
    expect(useKafkaStore.getState().snapshot.running).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["reset", "configureTopology", "loadScenario"] as const)("clears fractional producer work on %s", (resetAction) => {
    const actions = useKafkaStore.getState().actions;
    actions.setProducerRate?.(9);
    actions.start?.();
    vi.advanceTimersByTime(100);
    expect(useKafkaStore.getState().snapshot.messageCount).toBe(0);

    if (resetAction === "reset") actions.reset();
    else if (resetAction === "configureTopology") actions.configureTopology?.({ topicName: "fresh", partitionCount: 1 });
    else actions.loadScenario?.("keyed-partitioning");
    actions.setProducerRate?.(1);
    actions.start?.();
    vi.advanceTimersByTime(100);
    expect(useKafkaStore.getState().snapshot.messageCount).toBe(0);
  });

  it.each(["reset", "configureTopology", "loadScenario"] as const)("clears fractional consumer work on %s", (resetAction) => {
    const actions = useKafkaStore.getState().actions;
    actions.produceBatch?.({ count: 10 });
    actions.startReading?.();
    vi.advanceTimersByTime(100);
    expect(useKafkaStore.getState().snapshot.totalLag).toBe(10);

    if (resetAction === "reset") actions.reset();
    else if (resetAction === "configureTopology") actions.configureTopology?.({ topicName: "fresh", partitionCount: 1 });
    else actions.loadScenario?.("keyed-partitioning");
    actions.produceBatch?.({ count: 10 });
    actions.startReading?.();
    vi.advanceTimersByTime(100);
    expect(useKafkaStore.getState().snapshot.totalLag).toBe(10);
  });
});
