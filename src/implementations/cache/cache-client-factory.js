/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Cache Client Factory
 *
 * Factory pattern implementation for creating and configuring cache clients.
 * Supports different cache implementations with automatic fallback mechanisms,
 * configuration validation, and standardized interfaces. Currently supports
 * Redis with planned support for Memcached and in-memory cache options.
 */
const logger = require("../../services/logger-service");
const RedisCacheClient = require("./redis-cache-client");

class CacheClientFactory {
  /**
   * Creates a cache client instance based on configuration
   * @param {Object} config Cache configuration object
   * @param {string} config.implementation Cache implementation type ('redis', 'memcached', 'memory')
   * @param {string} config.keyPrefix Key prefix for organization
   * @param {number} config.processingTtl Processing key TTL in milliseconds
   * @param {number} config.sentTtl Sent message TTL in milliseconds
   * @param {Object} config.connectionOptions Implementation-specific connection options
   * @returns {RedisCacheClient} Cache layer instance
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
      logger.logError("Failed to create cache client", error);

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

  static _resolveImplementation(config) {
    return config.implementation || process.env.MO_CACHE_IMPLEMENTATION || "redis";
  }

  static _createRedisClient(config) {
    try {
      return new RedisCacheClient({
        ...config,
        implementation: "redis",
      });
    } catch (error) {
      logger.logError("Failed to create Redis cache client", error);
      throw new Error(`Redis cache client creation failed: ${error.message}`);
    }
  }

  static _createMemcachedClient(config) {
    try {
      return new MemcachedCacheClient({
        ...config,
        implementation: "memcached",
      });
    } catch (error) {
      logger.logError("Failed to create Memcached cache client", error);
      throw new Error(`Memcached cache client creation failed: ${error.message}`);
    }
  }

  static _createMemoryClient(config) {
    const MemoryCacheClient = require("./memory-cache-client");

    try {
      logger.logWarning(
        "Memory cache is for development/testing only. Not suitable for production multi-instance deployments."
      );

      return new MemoryCacheClient({
        ...config,
        implementation: "memory",
      });
    } catch (error) {
      logger.logError("Failed to create Memory cache client", error);
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
          maxItems: 1000,
        },
      },
    };

    return {
      ...baseConfig,
      ...(implementationDefaults[implementation.toLowerCase()] || implementationDefaults.redis),
    };
  }
}

module.exports = CacheClientFactory;
