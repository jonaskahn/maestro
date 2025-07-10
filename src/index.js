/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Maestro - Unified Messaging System
 *
 * Main library export that provides consistent interfaces for message
 * brokers and distributed processing. Currently focused on Kafka
 * implementations with a flexible design for future support of additional
 * brokers like RabbitMQ and BullMQ.
 *
 * @module maestro
 * @exports {Class} KafkaProducer - Kafka message producer implementation
 * @exports {Class} KafkaConsumer - Kafka message consumer implementation
 * @exports {Class} DefaultProducer - Alias for KafkaProducer (default producer)
 * @exports {Class} DefaultConsumer - Alias for KafkaConsumer (default consumer)
 */

module.exports = {
  KafkaProducer: require("./implementations/brokers/kafka/kafka-producer"),
  KafkaConsumer: require("./implementations/brokers/kafka/kafka-consumer"),

  DefaultProducer: require("./implementations/brokers/kafka/kafka-producer"),
  DefaultConsumer: require("./implementations/brokers/kafka/kafka-consumer"),
};
