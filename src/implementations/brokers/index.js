/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Message Broker Implementation Exports
 *
 * Provides centralized access to concrete message broker implementations
 * that extend the AbstractProducer and AbstractConsumer interfaces.
 * Currently includes Kafka implementations with support for additional
 * broker types planned in the future (RabbitMQ, etc.).
 *
 * @module implementations/brokers
 * @exports {Class} KafkaProducer - Kafka implementation of message producer
 * @exports {Class} KafkaConsumer - Kafka implementation of message consumer
 * @exports {Class} KafkaMonitorService - Backpressure monitoring for Kafka
 * @exports {Object} KafkaManager - Utility for Kafka client management
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
