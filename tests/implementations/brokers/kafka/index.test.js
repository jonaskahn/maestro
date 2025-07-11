/**
 * Tests for the Kafka implementation module exports in src/implementations/brokers/kafka/index.js
 */

const kafkaModule = require("../../../../src/implementations/brokers/kafka/index");
const KafkaConsumer = require("../../../../src/implementations/brokers/kafka/kafka-consumer");
const KafkaProducer = require("../../../../src/implementations/brokers/kafka/kafka-producer");
const KafkaMonitorService = require("../../../../src/implementations/brokers/kafka/kafka-monitor-service");
const KafkaManager = require("../../../../src/implementations/brokers/kafka/kafka-manager");

describe("Kafka Implementation Module Exports", () => {
  test("should export KafkaConsumer", () => {
    expect(kafkaModule.KafkaConsumer).toBeDefined();
    expect(kafkaModule.KafkaConsumer).toBe(KafkaConsumer);
  });

  test("should export KafkaProducer", () => {
    expect(kafkaModule.KafkaProducer).toBeDefined();
    expect(kafkaModule.KafkaProducer).toBe(KafkaProducer);
  });

  test("should export KafkaMonitorService", () => {
    expect(kafkaModule.KafkaMonitorService).toBeDefined();
    expect(kafkaModule.KafkaMonitorService).toBe(KafkaMonitorService);
  });

  test("should export KafkaManager", () => {
    expect(kafkaModule.KafkaManager).toBeDefined();
    expect(kafkaModule.KafkaManager).toBe(KafkaManager);
  });

  test("should have exactly 4 exports", () => {
    expect(Object.keys(kafkaModule).length).toBe(4);
  });
});
