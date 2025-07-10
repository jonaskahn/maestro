/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * Root entry-point: expose Kafka broker only. This keeps the public API
 * surface minimal while we stabilise additional brokers.
 *
 */

module.exports = {
  KafkaProducer: require("./implementations/brokers/kafka/kafka-producer"),
  KafkaConsumer: require("./implementations/brokers/kafka/kafka-consumer"),

  DefaultProducer: require("./implementations/brokers/kafka/kafka-producer"),
  DefaultConsumer: require("./implementations/brokers/kafka/kafka-consumer"),
};
