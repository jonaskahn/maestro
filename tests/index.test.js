/**
 * Tests for the main module exports in src/index.js
 */

const mainModule = require("../src/index");
const KafkaProducer = require("../src/implementations/brokers/kafka/kafka-producer");
const KafkaConsumer = require("../src/implementations/brokers/kafka/kafka-consumer");

describe("Main Module Exports", () => {
  test("should export KafkaProducer", () => {
    expect(mainModule.KafkaProducer).toBeDefined();
    expect(mainModule.KafkaProducer).toBe(KafkaProducer);
  });

  test("should export KafkaConsumer", () => {
    expect(mainModule.KafkaConsumer).toBeDefined();
    expect(mainModule.KafkaConsumer).toBe(KafkaConsumer);
  });

  test("should export DefaultProducer as alias for KafkaProducer", () => {
    expect(mainModule.DefaultProducer).toBeDefined();
    expect(mainModule.DefaultProducer).toBe(KafkaProducer);
    expect(mainModule.DefaultProducer).toBe(mainModule.KafkaProducer);
  });

  test("should export DefaultConsumer as alias for KafkaConsumer", () => {
    expect(mainModule.DefaultConsumer).toBeDefined();
    expect(mainModule.DefaultConsumer).toBe(KafkaConsumer);
    expect(mainModule.DefaultConsumer).toBe(mainModule.KafkaConsumer);
  });

  test("should have exactly 4 exports", () => {
    expect(Object.keys(mainModule).length).toBe(4);
  });
});
