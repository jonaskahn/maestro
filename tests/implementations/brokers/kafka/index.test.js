/**
 * Tests for the Kafka implementation module exports in src/implementations/brokers/kafka/index.js
 */

const kafkaModule = require("../../../../src/implementations/brokers/kafka/index");
const KafkaConsumer = require("../../../../src/implementations/brokers/kafka/kafka-consumer");
const KafkaProducer = require("../../../../src/implementations/brokers/kafka/kafka-producer");
const KafkaMonitorService = require("../../../../src/implementations/brokers/kafka/kafka-monitor-service");
const KafkaManager = require("../../../../src/implementations/brokers/kafka/kafka-manager");

describe("Kafka Implementation Module Exports", () => {
  test("Given the Kafka module When accessing KafkaConsumer export Then it should be defined and match implementation", () => {
    expect(kafkaModule.KafkaConsumer).toBeDefined();
    expect(kafkaModule.KafkaConsumer).toBe(KafkaConsumer);
  });
  test("Given the Kafka module When accessing KafkaProducer export Then it should be defined and match implementation", () => {
    expect(kafkaModule.KafkaProducer).toBeDefined();
    expect(kafkaModule.KafkaProducer).toBe(KafkaProducer);
  });
  test("Given the Kafka module When accessing KafkaMonitorService export Then it should be defined and match implementation", () => {
    expect(kafkaModule.KafkaMonitorService).toBeDefined();
    expect(kafkaModule.KafkaMonitorService).toBe(KafkaMonitorService);
  });
  test("Given the Kafka module When accessing KafkaManager export Then it should be defined and match implementation", () => {
    expect(kafkaModule.KafkaManager).toBeDefined();
    expect(kafkaModule.KafkaManager).toBe(KafkaManager);
  });
  test("Given the Kafka module When checking number of exports Then there should be exactly 4 exports", () => {
    expect(Object.keys(kafkaModule).length).toBe(4);
  });
});
