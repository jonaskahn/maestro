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
    expect(brokersModule.KafkaProducer).toBeDefined();
    expect(brokersModule.KafkaProducer).toBe(KafkaProducer);
  });

  test("should export KafkaConsumer", () => {
    expect(brokersModule.KafkaConsumer).toBeDefined();
    expect(brokersModule.KafkaConsumer).toBe(KafkaConsumer);
  });

  test("should export KafkaMonitorService", () => {
    expect(brokersModule.KafkaMonitorService).toBeDefined();
    expect(brokersModule.KafkaMonitorService).toBe(KafkaMonitorService);
  });

  test("should export KafkaManager", () => {
    expect(brokersModule.KafkaManager).toBeDefined();
    expect(brokersModule.KafkaManager).toBe(KafkaManager);
  });

  test("should have exactly 4 exports", () => {
    expect(Object.keys(brokersModule).length).toBe(4);
  });
});
