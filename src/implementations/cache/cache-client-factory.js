/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Cache Factory for creating cache layer implementations
 *
 * Supports Redis, Memcached, in-memory cache with fallback mechanisms and configuration validation.
 */
const logger = require("../../services/logger-service");

class CacheClientFactory {
  /**
   * Creates a cache _client instance based on configuration
   * @param {Object} config - Cache configuration object
   * @param {string} config.implementation - Cache implementation type ('redis', 'memcached', 'memory')
   * @param {string} config.keyPrefix - Key prefix for organization
   * @param {number} config.processingTtl - Processing key TTL in milliseconds
   * @param {number} config.sentTtl - Sent message TTL in milliseconds
   * @param {Object} config.connectionOptions - Implementation-specific connection options
   * @returns {AbstractCache} Cache layer instance
   * @throws {Error} When cache creation fails or configuration is invalid
   */
  static createClient(config = {}) {
    try {
      this.validateConfiguration(config);

      const implementation = this._resolveImplementation(config);

      switch (implementation.toLowerCase()) {
        case "redis":
          return this._createRedisClient(config);
        default:
          logger.logWarning(`Unsupported cache implementation: ${implementation}. Falling back to Redis.`);
          return this._createRedisClient({
            ...config,
            implementation: "redis",
          });
      }
    } catch (error) {
      logger.logError("Failed to create cache _client", error);

      if (config.implementation !== "redis") {
        logger.logWarning("Attempting fallback to Redis cache...");
        try {
          return this._createRedisClient({
            ...config,
            implementation: "redis",
          });
        } catch (fallbackError) {
          logger.logError("Redis fallback also failed", fallbackError);
          throw new Error(
            `Cache client creation failed: ${error.message}. Fallback to Redis also failed: ${fallbackError.message}`
          );
        }
      }

      throw error;
    }
  }

  /**
   * Resolve cache implementation from _config and environment
   * @param {Object} config - Configuration object
   * @returns {string} Implementation type
   * @private
   */
  static _resolveImplementation(config) {
    return config.implementation || process.env.MO_CACHE_IMPLEMENTATION || "redis";
  }

  /**
   * Create Redis cache _client
   * @param {Object} config - Configuration object
   * @returns {RedisCacheClient} Redis cache layer instance
   * @private
   */
  static _createRedisClient(config) {
    const RedisCacheClient = require("./redis-cache-client");

    try {
      return new RedisCacheClient({
        ...config,
        implementation: "redis",
      });
    } catch (error) {
      logger.logError("Failed to create Redis cache _client", error);
      throw new Error(`Redis cache client creation failed: ${error.message}`);
    }
  }

  /**
   * Create Memcached _client
   * @param {Object} config - Configuration object
   * @returns {MemcachedCacheClient} Memcached cache layer instance
   * @private
   */
  static _createMemcachedClient(config) {
    const MemcachedCacheClient = require("./memcached-cache-_client");

    try {
      return new MemcachedCacheClient({
        ...config,
        implementation: "memcached",
      });
    } catch (error) {
      logger.logError("Failed to create Memcached cache _client", error);
      throw new Error(`Memcached cache client creation failed: ${error.message}`);
    }
  }

  /**
   * Create Memory cache _client
   * @param {Object} config - Configuration object
   * @returns {MemoryCacheClient} Memory cache layer instance
   * @private
   */
  static _createMemoryClient(config) {
    const MemoryCacheClient = require("./memory-cache-_client");

    try {
      logger.logWarning(
        "Memory cache is for development/testing only. Not suitable for production multi-instance deployments."
      );

      return new MemoryCacheClient({
        ...config,
        implementation: "memory",
      });
    } catch (error) {
      logger.logError("Failed to create Memory cache _client", error);
      throw new Error(`Memory cache client creation failed: ${error.message}`);
    }
  }

  /**
   * Validate cache configuration against required fields and constraints
   * @param {Object} config Configuration to validate
   * @throws {Error} If configuration is invalid
   */
  static validateConfiguration(config) {
    if (!config || typeof config !== "object") {
      throw new Error("Cache configuration must be an object");
    }

    if (!config.keyPrefix || typeof config.keyPrefix !== "string") {
      throw new Error("Cache configuration must include a keyPrefix string");
    }

    if (config.keyPrefix.trim().length === 0) {
      throw new Error("Cache keyPrefix cannot be empty");
    }

    const implementation = this._resolveImplementation(config);
    const supportedImplementations = this.getSupportedImplementations();

    if (!supportedImplementations.includes(implementation.toLowerCase())) {
      logger.logWarning(
        `Unsupported cache implementation: ${implementation}. ` +
          `Supported implementations: ${supportedImplementations.join(", ")}`
      );
    }

    if (config.processingTtl !== undefined) {
      this._validateTtl(config.processingTtl, "processingTtl");
    }

    if (config.sentTtl !== undefined) {
      this._validateTtl(config.sentTtl, "sentTtl");
    }

    if (config.connectionOptions && typeof config.connectionOptions !== "object") {
      throw new Error("Cache connectionOptions must be an object");
    }
  }

  /**
   * Validate TTL value
   * @param {any} ttl - TTL value to validate
   * @param {string} fieldName - Name of the field for error messages
   * @private
   */
  static _validateTtl(ttl, fieldName) {
    if (typeof ttl !== "number" || ttl <= 0 || !Number.isInteger(ttl)) {
      throw new Error(`Cache ${fieldName} must be a positive integer (milliseconds)`);
    }

    if (ttl > 86400 * 30 * 1000) {
      logger.logWarning(
        `Cache ${fieldName} is quite high (${ttl} milliseconds = ${Math.round(ttl / (86400 * 1000))} days). Consider if this is intentional.`
      );
    }
  }

  /**
   * Get list of supported cache implementations
   * @returns {Array<string>} Array of supported implementation names
   */
  static getSupportedImplementations() {
    return ["redis", "memcached", "memory"];
  }

  /**
   * Check if implementation is supported
   * @param {string} implementation Implementation name to check
   * @returns {boolean} True if implementation is supported
   */
  static isImplementationSupported(implementation) {
    if (typeof implementation !== "string") {
      return false;
    }

    return this.getSupportedImplementations().includes(implementation.toLowerCase());
  }

  /**
   * Get default configuration for implementation
   * @param {string} implementation Implementation type
   * @returns {Object} Default configuration object
   */
  static getDefaultConfig(implementation = "redis") {
    const baseConfig = {
      keyPrefix: "MO_DEFAULT",
      processingTtl: 10 * 1000,
      sentTtl: 20 * 1000,
      retryOptions: {
        retries: 3,
        retryDelay: 1000,
      },
    };

    const implementationDefaults = {
      redis: {
        implementation: "redis",
        connectionOptions: {
          url: "redis://localhost:6379",
          db: 0,
          maxRetriesPerRequest: 3,
          lazyConnect: true,
        },
      },
      memcached: {
        implementation: "memcached",
        connectionOptions: {
          servers: "localhost:11211",
          options: {
            timeout: 3000,
            retries: 2,
          },
        },
      },
      memory: {
        implementation: "memory",
        connectionOptions: {
          maxSize: 1000,
          defaultTtl: 3600 * 1000,
        },
      },
    };

    return {
      ...baseConfig,
      ...implementationDefaults[implementation.toLowerCase()],
    };
  }
}

module.exports = CacheClientFactory;
