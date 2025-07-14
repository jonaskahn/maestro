/**
 * Tests for the cache module exports in src/implementations/cache/index.js
 */

const cacheModule = require("../../../src/implementations/cache/index");
const CacheClientFactory = require("../../../src/implementations/cache/cache-client-factory");
const RedisCacheClient = require("../../../src/implementations/cache/redis-cache-client");

describe("Cache Module Exports", () => {
  test("should export CacheClientFactory", () => {
    // Given: Test setup for should export CacheClientFactory
    // When: Action being tested
    // Then: Expected outcome
    expect(cacheModule.CacheClientFactory).toBeDefined();
    expect(cacheModule.CacheClientFactory).toBe(CacheClientFactory);
  });

  test("should export RedisCacheClient", () => {
    // Given: Test setup for should export RedisCacheClient
    // When: Action being tested
    // Then: Expected outcome
    expect(cacheModule.RedisCacheClient).toBeDefined();
    expect(cacheModule.RedisCacheClient).toBe(RedisCacheClient);
  });

  test("should have exactly 2 exports", () => {
    // Given: Test setup for should have exactly 2 exports
    // When: Action being tested
    // Then: Expected outcome
    expect(Object.keys(cacheModule).length).toBe(2);
  });
});
