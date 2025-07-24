const mainModule = require("../src/index");
const KafkaProducer = require("../src/implementations/brokers/kafka/kafka-producer");
const KafkaConsumer = require("../src/implementations/brokers/kafka/kafka-consumer");

describe("Main Module Exports", () => {
  test("Given the main module is imported When accessing KafkaProducer export Then it should be defined and match implementation", () => {
    expect(mainModule.KafkaProducer).toBeDefined();
    expect(mainModule.KafkaProducer).toBe(KafkaProducer);
  });
  test("Given the main module is imported When accessing KafkaConsumer export Then it should be defined and match implementation", () => {
    expect(mainModule.KafkaConsumer).toBeDefined();
    expect(mainModule.KafkaConsumer).toBe(KafkaConsumer);
  });
  test("Given the main module is imported When accessing DefaultProducer export Then it should be defined as alias for KafkaProducer", () => {
    expect(mainModule.DefaultProducer).toBeDefined();
    expect(mainModule.DefaultProducer).toBe(KafkaProducer);
    expect(mainModule.DefaultProducer).toBe(mainModule.KafkaProducer);
  });
  test("Given the main module is imported When accessing DefaultConsumer export Then it should be defined as alias for KafkaConsumer", () => {
    expect(mainModule.DefaultConsumer).toBeDefined();
    expect(mainModule.DefaultConsumer).toBe(KafkaConsumer);
    expect(mainModule.DefaultConsumer).toBe(mainModule.KafkaConsumer);
  });
  test("Given the main module is imported When checking number of exports Then there should be exactly 4 exports", () => {
    expect(Object.keys(mainModule).length).toBe(4);
  });
});
