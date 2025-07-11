/**
 * Tests for the services module exports in src/services/index.js
 */

const servicesModule = require("../../src/services/index");
const DistributedLockService = require("../../src/services/distributed-lock-service");
const LoggerService = require("../../src/services/logger-service");

describe("Services Module Exports", () => {
  test("should export DistributedLockService", () => {
    expect(servicesModule.DistributedLockService).toBeDefined();
    expect(servicesModule.DistributedLockService).toBe(DistributedLockService);
  });

  test("should export LoggerService", () => {
    expect(servicesModule.LoggerService).toBeDefined();
    expect(servicesModule.LoggerService).toBe(LoggerService);
  });

  test("should have exactly 2 exports", () => {
    expect(Object.keys(servicesModule).length).toBe(2);
  });
});
