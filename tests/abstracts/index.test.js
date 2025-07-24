/**
 * Tests for the abstracts module exports in src/abstracts/index.js
 */

const abstractsModule = require("../../src/abstracts/index");
const AbstractProducer = require("../../src/abstracts/abstract-producer");
const AbstractConsumer = require("../../src/abstracts/abstract-consumer");
const AbstractCache = require("../../src/abstracts/abstract-cache");
const AbstractMonitorService = require("../../src/abstracts/abstract-monitor-service");

describe("Abstracts Module Exports", () => {
  test("Given the abstracts module When accessing AbstractProducer export Then it should be defined and match implementation", () => {
    expect(abstractsModule.AbstractProducer).toBeDefined();
    expect(abstractsModule.AbstractProducer).toBe(AbstractProducer);
  });
  test("Given the abstracts module When accessing AbstractConsumer export Then it should be defined and match implementation", () => {
    expect(abstractsModule.AbstractConsumer).toBeDefined();
    expect(abstractsModule.AbstractConsumer).toBe(AbstractConsumer);
  });
  test("Given the abstracts module When accessing AbstractCache export Then it should be defined and match implementation", () => {
    expect(abstractsModule.AbstractCache).toBeDefined();
    expect(abstractsModule.AbstractCache).toBe(AbstractCache);
  });
  test("Given the abstracts module When accessing AbstractMonitorService export Then it should be defined and match implementation", () => {
    expect(abstractsModule.AbstractMonitorService).toBeDefined();
    expect(abstractsModule.AbstractMonitorService).toBe(AbstractMonitorService);
  });
  test("Given the abstracts module When checking number of exports Then there should be exactly 4 exports", () => {
    expect(Object.keys(abstractsModule).length).toBe(4);
  });
});
