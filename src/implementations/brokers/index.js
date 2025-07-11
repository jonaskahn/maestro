/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * Central export for broker specific implementations (Kafka, RabbitMQ, etc.).
 *
 * Example:
 *   const { KafkaProducer } = require("@/brokers");
 */

module.exports = {
  KafkaProducer: require("./kafka/kafka-producer"),
  KafkaConsumer: require("./kafka/kafka-consumer"),
  KafkaMonitorService: require("./kafka/kafka-monitor-service"),
  KafkaManager: require("./kafka/kafka-manager"),
};
