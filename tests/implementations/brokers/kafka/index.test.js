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
    // Given: Test setup for should export KafkaConsumer
    // When: Action being tested
    // Then: Expected outcome
    expect(kafkaModule.KafkaConsumer).toBeDefined();
    expect(kafkaModule.KafkaConsumer).toBe(KafkaConsumer);
  });

  test("should export KafkaProducer", () => {
    // Given: Test setup for should export KafkaProducer
    // When: Action being tested
    // Then: Expected outcome
    expect(kafkaModule.KafkaProducer).toBeDefined();
    expect(kafkaModule.KafkaProducer).toBe(KafkaProducer);
  });

  test("should export KafkaMonitorService", () => {
    // Given: Test setup for should export KafkaMonitorService
    // When: Action being tested
    // Then: Expected outcome
    expect(kafkaModule.KafkaMonitorService).toBeDefined();
    expect(kafkaModule.KafkaMonitorService).toBe(KafkaMonitorService);
  });

  test("should export KafkaManager", () => {
    // Given: Test setup for should export KafkaManager
    // When: Action being tested
    // Then: Expected outcome
    expect(kafkaModule.KafkaManager).toBeDefined();
    expect(kafkaModule.KafkaManager).toBe(KafkaManager);
  });

  test("should have exactly 4 exports", () => {
    // Given: Test setup for should have exactly 4 exports
    // When: Action being tested
    // Then: Expected outcome
    expect(Object.keys(kafkaModule).length).toBe(4);
  });
});
