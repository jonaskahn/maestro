/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * Central export for cache related implementations.
 * Usage:
 *   const { CacheClientFactory, RedisCacheClient } = require("@/cache");
 */

module.exports = {
  CacheClientFactory: require("./cache-client-factory"),
  RedisCacheClient: require("./redis-cache-client"),
};
