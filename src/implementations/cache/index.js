/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Central export for cache related implementations.
 *
 * @module implementations/cache
 * @exports {Object} CacheClientFactory - Factory for creating appropriate cache clients
 * @exports {Class} RedisCacheClient - Redis implementation of abstract cache interface
 */

module.exports = {
  CacheClientFactory: require("./cache-client-factory"),
  RedisCacheClient: require("./redis-cache-client"),
};
