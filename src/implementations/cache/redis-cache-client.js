/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Redis Cache Implementation
 *
 * Concrete implementation of AbstractCache using Redis as the backend.
 * Provides Redis-specific methods for key-value operations, TTL management,
 * and pattern-based key scanning with standardized error handling and logging.
 * Supports Redis connection pooling, retry strategies, and reconnection logic.
 */
const AbstractCache = require("../../abstracts/abstract-cache");
const redis = require("redis");
const logger = require("../../services/logger-service");

/**
 * Redis Cache Client
 *
 * Implements the AbstractCache interface using Redis as the backing store.
 * Handles connection management, retry strategies, and provides Redis-specific
 * implementations of the cache operations defined in the abstract class.
 */
class RedisCacheClient extends AbstractCache {
  /**
   * Creates a new Redis cache client instance
   *
   * @param {Object} config - Cache configuration object
   * @param {string} config.keyPrefix - Required prefix for all cache keys
   * @param {Object} [config.connectionOptions] - Redis-specific connection options
   * @param {string} [config.connectionOptions.url] - Redis connection URL
   * @param {string} [config.connectionOptions.password] - Redis password
   * @param {number} [config.processingTtl] - TTL for processing state keys
   * @param {number} [config.suppressionTtl] - TTL for suppression keys
   */
  constructor(config) {
    super(config);
    this._client = this.#createRedisClient();
  }

  /**
   * Creates a Redis client with configured options
   * @returns {Object} Redis client instance
   */
  #createRedisClient() {
    const connectionOptions = this.config.connectionOptions || {};
    let redisUrl = "redis://localhost:6379";
    if (process.env.MO_REDIS_URL) {
      redisUrl = `redis://${process.env.REDIS_URL.trim().replace("redis://")}`;
    } else {
      redisUrl = `redis://${process.env.MO_REDIS_HOST?.trim() || "127.0.0.1"}:${process.env.MO_REDIS_PORT || 6379}`;
    }
    const clientConfig = {
      url: connectionOptions.url || process.env.MO_REDIS_URL || redisUrl,
      password: connectionOptions.password || process.env.MO_REDIS_PASSWORD,
      retry_strategy: this.#createRetryStrategy(),
      socket: {
        reconnectStrategy: this.#createReconnectionStrategy(),
      },
      ...connectionOptions,
    };

    const client = redis.createClient(clientConfig);
    this.#attachEventHandlers(client);

    return client;
  }

  /**
   * Creates retry strategy for Redis client
   * @returns {Function} Retry strategy callback
   */
  #createRetryStrategy() {
    const maxRetryAttempts = parseInt(process.env.MO_REDIS_MAX_RETRY_ATTEMPTS) || 5;
    const retryDelayMs = parseInt(process.env.MO_REDIS_DELAY_MS) || 1000;
    const maxDelayMs = parseInt(process.env.MO_REDIS_MAX_DELAY_MS) || 30000;

    return attemptNumber => {
      if (attemptNumber > maxRetryAttempts) {
        logger.logError(`Redis retry limit exceeded (${attemptNumber} attempts)`);
        return null;
      }

      const delayMs = Math.min(attemptNumber * retryDelayMs, maxDelayMs);
      logger.logInfo(`Redis retry attempt ${attemptNumber} in ${delayMs}ms`);
      return delayMs;
    };
  }

  /**
   * Creates reconnection strategy for Redis socket
   * @returns {Function} Reconnection strategy callback
   */
  #createReconnectionStrategy() {
    const maxRetryAttempts = parseInt(process.env.MO_REDIS_MAX_RETRY_ATTEMPTS) || 5;
    const retryDelayMs = parseInt(process.env.MO_REDIS_DELAY_MS) || 1000;
    const maxDelayMs = parseInt(process.env.MO_REDIS_MAX_DELAY_MS) || 30000;

    return retryAttemptNumber => {
      if (retryAttemptNumber > maxRetryAttempts) {
        logger.logError(`Redis reconnection limit exceeded (${retryAttemptNumber} attempts)`);
        return new Error("Redis connection failed permanently");
      }

      const delayMs = Math.min(retryAttemptNumber * retryDelayMs, maxDelayMs);
      logger.logInfo(`Redis reconnecting in ${delayMs}ms (attempt ${retryAttemptNumber})`);
      return delayMs;
    };
  }

  /**
   * Attaches event handlers to Redis client
   * @param {Object} client - Redis client instance
   */
  #attachEventHandlers(client) {
    client.on("error", error => {
      logger.logError("Redis client error occurred", error);
    });

    client.on("connect", () => {
      logger.logConnectionEvent("Redis", "client connecting to server");
    });

    client.on("ready", () => {
      logger.logConnectionEvent("Redis", "client ready for operations");
    });

    client.on("end", () => {
      logger.logConnectionEvent("Redis", "client disconnected from server");
    });

    client.on("reconnecting", () => {
      logger.logConnectionEvent("Redis", "client reconnecting to server");
    });
  }

  /**
   * Checks if a Redis connection already exists
   * @returns {boolean} True if connection exists
   */
  _checkExistingConnection() {
    return this._client && this._client.isOpen;
  }

  /**
   * Connects to Redis server
   * @returns {Promise<void>}
   */
  async _connectTo() {
    if (!this._client.isOpen) {
      await this._client.connect();
    }
  }

  /**
   * Disconnects from Redis server
   * @returns {Promise<void>}
   */
  async _disconnectFrom() {
    if (this._client.isOpen) {
      await this._client.disconnect();
    }
  }

  /**
   * Sets a value in Redis with optional TTL
   * @param {string} key - Cache key
   * @param {any} value - Value to store
   * @param {number} ttlMs - Time-to-live in milliseconds
   * @returns {Promise<boolean>} Success indicator
   */
  async _setKeyValue(key, value, ttlMs) {
    if (ttlMs) {
      const ttlSeconds = Math.ceil(ttlMs / 1000);
      await this._client.set(key, value, {
        EX: ttlSeconds,
      });
    } else {
      await this._client.set(key, value);
    }
    return true;
  }

  /**
   * Gets a value from Redis
   * @param {string} key - Cache key
   * @returns {Promise<any>} Retrieved value or null
   */
  async _getKeyValue(key) {
    return await this._client.get(key);
  }

  /**
   * Deletes a key from Redis
   * @param {string} key - Cache key
   * @returns {Promise<boolean>} True if key was deleted
   */
  async _deleteKey(key) {
    return await this._client.del(key);
  }

  /**
   * Checks if key exists in Redis
   * @param {string} key - Cache key
   * @returns {Promise<boolean>} True if key exists
   */
  async _checkKeyExists(key) {
    return await this._client.exists(key);
  }

  /**
   * Sets expiry for existing Redis key (converts from milliseconds to seconds)
   * @param {string} key - Cache key
   * @param {number} ttlMs - Time-to-live in milliseconds
   * @returns {Promise<boolean>} True if expiry was set
   */
  async _setKeyExpiry(key, ttlMs) {
    const ttlSeconds = Math.ceil(ttlMs / 1000);
    return await this._client.expire(key, ttlSeconds);
  }

  /**
   * Finds keys matching a pattern using Redis scan
   * @param {string} pattern - Pattern to match
   * @returns {Promise<string[]>} Array of matching keys
   */
  async _findKeysByPattern(pattern) {
    try {
      const sentKeys = [];
      let scanCount = 0;
      for await (const keys of this._client.scanIterator({
        MATCH: pattern,
        COUNT: 1000,
      })) {
        sentKeys.push(...keys);
        scanCount++;
        if (scanCount % 100 === 0) {
          logger.logDebug(`Found ${scanCount} keys in Redis scan operation`);
        }
      }
      return sentKeys;
    } catch (error) {
      logger.logError(`Error scanning Redis keys with pattern '${pattern}'`, error);
      throw error;
    }
  }

  /**
   * Sets a key with TTL only if it doesn't exist (atomic NX operation)
   * @param {string} key - Cache key
   * @param {any} value - Value to store
   * @param {number} ttlMs - Time-to-live in milliseconds
   * @returns {Promise<boolean>} True if key was set
   */
  async _setKeyIfNotExists(key, value, ttlMs) {
    let result;
    if (ttlMs) {
      const ttlSeconds = Math.ceil(ttlMs / 1000);
      result = await this._client.set(key, value, {
        EX: ttlSeconds,
        NX: true,
      });
    } else {
      result = await this._client.set(key, value, {
        NX: true,
      });
    }
    return result === "OK";
  }
}

module.exports = RedisCacheClient;
