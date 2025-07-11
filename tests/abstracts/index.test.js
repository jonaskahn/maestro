/**
 * Tests for the abstracts module exports in src/abstracts/index.js
 */

const abstractsModule = require("../../src/abstracts/index");
const AbstractProducer = require("../../src/abstracts/abstract-producer");
const AbstractConsumer = require("../../src/abstracts/abstract-consumer");
const AbstractCache = require("../../src/abstracts/abstract-cache");
const AbstractMonitorService = require("../../src/abstracts/abstract-monitor-service");

describe("Abstracts Module Exports", () => {
  test("should export AbstractProducer", () => {
    expect(abstractsModule.AbstractProducer).toBeDefined();
    expect(abstractsModule.AbstractProducer).toBe(AbstractProducer);
  });

  test("should export AbstractConsumer", () => {
    expect(abstractsModule.AbstractConsumer).toBeDefined();
    expect(abstractsModule.AbstractConsumer).toBe(AbstractConsumer);
  });

  test("should export AbstractCache", () => {
    expect(abstractsModule.AbstractCache).toBeDefined();
    expect(abstractsModule.AbstractCache).toBe(AbstractCache);
  });

  test("should export AbstractMonitorService", () => {
    expect(abstractsModule.AbstractMonitorService).toBeDefined();
    expect(abstractsModule.AbstractMonitorService).toBe(AbstractMonitorService);
  });

  test("should have exactly 4 exports", () => {
    expect(Object.keys(abstractsModule).length).toBe(4);
  });
});
