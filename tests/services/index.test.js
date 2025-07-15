/**
 * Tests for the services module exports in src/services/index.js
 */

const servicesModule = require("../../src/services/index");
const DistributedLockService = require("../../src/services/distributed-lock-service");
const LoggerService = require("../../src/services/logger-service");

describe("Services Module Exports", () => {
  test("Given The services module is imported When Accessing the DistributedLockService export Then It should be defined and match the DistributedLockService implementation", () => {
    expect(servicesModule.DistributedLockService).toBeDefined();
    expect(servicesModule.DistributedLockService).toBe(DistributedLockService);
  });

  test("Given The services module is imported When Accessing the LoggerService export Then It should be defined and match the LoggerService implementation", () => {
    expect(servicesModule.LoggerService).toBeDefined();
    expect(servicesModule.LoggerService).toBe(LoggerService);
  });

  test("Given The services module is imported When Checking the number of exports Then There should be exactly 2 exports", () => {
    expect(Object.keys(servicesModule).length).toBe(2);
  });
});
