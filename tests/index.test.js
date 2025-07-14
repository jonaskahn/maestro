const mainModule = require("../src/index");
const KafkaProducer = require("../src/implementations/brokers/kafka/kafka-producer");
const KafkaConsumer = require("../src/implementations/brokers/kafka/kafka-consumer");

describe("Main Module Exports", () => {
  test("should export KafkaProducer", () => {
    // Given: The main module is imported
    // When: Accessing the KafkaProducer export
    // Then: It should be defined and match the KafkaProducer implementation
    expect(mainModule.KafkaProducer).toBeDefined();
    expect(mainModule.KafkaProducer).toBe(KafkaProducer);
  });

  test("should export KafkaConsumer", () => {
    // Given: The main module is imported
    // When: Accessing the KafkaConsumer export
    // Then: It should be defined and match the KafkaConsumer implementation
    expect(mainModule.KafkaConsumer).toBeDefined();
    expect(mainModule.KafkaConsumer).toBe(KafkaConsumer);
  });

  test("should export DefaultProducer as alias for KafkaProducer", () => {
    // Given: The main module is imported
    // When: Accessing the DefaultProducer export
    // Then: It should be defined and be an alias for KafkaProducer
    expect(mainModule.DefaultProducer).toBeDefined();
    expect(mainModule.DefaultProducer).toBe(KafkaProducer);
    expect(mainModule.DefaultProducer).toBe(mainModule.KafkaProducer);
  });

  test("should export DefaultConsumer as alias for KafkaConsumer", () => {
    // Given: The main module is imported
    // When: Accessing the DefaultConsumer export
    // Then: It should be defined and be an alias for KafkaConsumer
    expect(mainModule.DefaultConsumer).toBeDefined();
    expect(mainModule.DefaultConsumer).toBe(KafkaConsumer);
    expect(mainModule.DefaultConsumer).toBe(mainModule.KafkaConsumer);
  });

  test("should have exactly 4 exports", () => {
    // Given: The main module is imported
    // When: Checking the number of exports
    // Then: There should be exactly 4 exports
    expect(Object.keys(mainModule).length).toBe(4);
  });
});
