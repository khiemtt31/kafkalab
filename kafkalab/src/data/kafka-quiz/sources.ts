/**
 * Primary Apache Kafka 4.3 references for the question banks. Questions are
 * paraphrased and intentionally avoid copying long passages from the docs.
 * The producer/consumer/topic/broker pages are configuration references;
 * design, consumer concepts, semantics, and transactions use the 4.3 design
 * page because those concepts are explained there in the current site.
 */
export const kafkaQuizSources = {
  overview: "https://kafka.apache.org/43/",
  design: "https://kafka.apache.org/43/design/design/",
  topics: "https://kafka.apache.org/43/getting-started/introduction/",
  consumers: "https://kafka.apache.org/43/design/design/",
  producerConfigs: "https://kafka.apache.org/43/configuration/producer-configs/",
  consumerConfigs: "https://kafka.apache.org/43/configuration/consumer-configs/",
  topicConfigs: "https://kafka.apache.org/43/configuration/topic-configs/",
  brokerConfigs: "https://kafka.apache.org/43/configuration/broker-configs/",
  semantics: "https://kafka.apache.org/43/design/design/",
  transactions: "https://kafka.apache.org/43/design/design/",
  streams: "https://kafka.apache.org/43/streams/introduction/",
  connect: "https://kafka.apache.org/43/kafka-connect/",
  security: "https://kafka.apache.org/43/security/",
} as const;
