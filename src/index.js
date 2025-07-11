/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * Root entry-point: exports Kafka producer and consumer implementations.
 *
 * @module maestro
 * @exports {Class} KafkaProducer - Kafka message producer implementation
 * @exports {Class} KafkaConsumer - Kafka message consumer implementation
 * @exports {Class} DefaultProducer - Alias for KafkaProducer (default producer)
 * @exports {Class} DefaultConsumer - Alias for KafkaConsumer (default consumer)
 *
 */

module.exports = {
  KafkaProducer: require("./implementations/brokers/kafka/kafka-producer"),
  KafkaConsumer: require("./implementations/brokers/kafka/kafka-consumer"),

  DefaultProducer: require("./implementations/brokers/kafka/kafka-producer"),
  DefaultConsumer: require("./implementations/brokers/kafka/kafka-consumer"),
};
