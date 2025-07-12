/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Kafka Implementation Exports
 *
 * Provides concrete implementations of the abstract messaging interfaces
 * for the Kafka message broker. Includes producer, consumer, monitoring service,
 * and a specialized manager for Kafka-specific operations and configuration.
 *
 * These components implement the messaging abstractions with Kafka-specific
 * functionality including message serialization, offset management, topic
 * administration, and backpressure monitoring.
 *
 * @module implementations/brokers/kafka
 * @exports {Class} KafkaConsumer - Kafka implementation of message consumer
 * @exports {Class} KafkaProducer - Kafka implementation of message producer
 * @exports {Class} KafkaMonitorService - Backpressure monitoring for Kafka
 * @exports {Object} KafkaManager - Utility for Kafka client management and operations
 */

const KafkaConsumer = require("./kafka-consumer");
const KafkaProducer = require("./kafka-producer");
const KafkaMonitorService = require("./kafka-monitor-service");
const KafkaManager = require("./kafka-manager");

module.exports = {
  KafkaConsumer,
  KafkaProducer,
  KafkaMonitorService,
  KafkaManager,
};
