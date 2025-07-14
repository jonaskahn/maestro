/**
 * Tests for the brokers module exports in src/implementations/brokers/index.js
 */

const brokersModule = require("../../../src/implementations/brokers/index");
const KafkaProducer = require("../../../src/implementations/brokers/kafka/kafka-producer");
const KafkaConsumer = require("../../../src/implementations/brokers/kafka/kafka-consumer");
const KafkaMonitorService = require("../../../src/implementations/brokers/kafka/kafka-monitor-service");
const KafkaManager = require("../../../src/implementations/brokers/kafka/kafka-manager");

describe("Brokers Module Exports", () => {
  test("should export KafkaProducer", () => {
    // Given: Test setup for should export KafkaProducer
    // When: Action being tested
    // Then: Expected outcome
    expect(brokersModule.KafkaProducer).toBeDefined();
    expect(brokersModule.KafkaProducer).toBe(KafkaProducer);
  });

  test("should export KafkaConsumer", () => {
    // Given: Test setup for should export KafkaConsumer
    // When: Action being tested
    // Then: Expected outcome
    expect(brokersModule.KafkaConsumer).toBeDefined();
    expect(brokersModule.KafkaConsumer).toBe(KafkaConsumer);
  });

  test("should export KafkaMonitorService", () => {
    // Given: Test setup for should export KafkaMonitorService
    // When: Action being tested
    // Then: Expected outcome
    expect(brokersModule.KafkaMonitorService).toBeDefined();
    expect(brokersModule.KafkaMonitorService).toBe(KafkaMonitorService);
  });

  test("should export KafkaManager", () => {
    // Given: Test setup for should export KafkaManager
    // When: Action being tested
    // Then: Expected outcome
    expect(brokersModule.KafkaManager).toBeDefined();
    expect(brokersModule.KafkaManager).toBe(KafkaManager);
  });

  test("should have exactly 4 exports", () => {
    // Given: Test setup for should have exactly 4 exports
    // When: Action being tested
    // Then: Expected outcome
    expect(Object.keys(brokersModule).length).toBe(4);
  });
});
