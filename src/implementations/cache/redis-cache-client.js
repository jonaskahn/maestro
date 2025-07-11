/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Redis Cache Layer Implementation
 *
 * Concrete implementation of AbstractCache using Redis as the backend
 */
const AbstractCache = require("../../abstracts/abstract-cache");
const redis = require("redis");
const logger = require("../../services/logger-service");

class RedisCacheClient extends AbstractCache {
  constructor(config) {
    super(config);
    this._client = this.#createRedisClient();
  }

  #createRedisClient() {
    const connectionOptions = this.config.connectionOptions || {};

    const clientConfig = {
      url: connectionOptions.url || process.env.MO_REDIS_URL || "redis://localhost:6379",
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

  #createRetryStrategy() {
    const maxRetryAttempts = parseInt(process.env.MO_REDIS_MAX_RETRY_ATTEMPTS) || 5;
    const retryDelayMs = parseInt(process.env.MO_REDIS_DELAY_MS) || 1000;
    const maxDelayMs = parseInt(process.env.MO_REDIS_MAX_DELAY_MS) || 30000;

    return attemptNumber => {
      if (attemptNumber > maxRetryAttempts) {
        logger.logError(`❌ Redis retry limit exceeded (${attemptNumber} attempts)`);
        return null;
      }

      const delayMs = Math.min(attemptNumber * retryDelayMs, maxDelayMs);
      logger.logInfo(`📡 Redis retry attempt ${attemptNumber} in ${delayMs}ms`);
      return delayMs;
    };
  }

  #createReconnectionStrategy() {
    const maxRetryAttempts = parseInt(process.env.MO_REDIS_MAX_RETRY_ATTEMPTS) || 5;
    const retryDelayMs = parseInt(process.env.MO_REDIS_DELAY_MS) || 1000;
    const maxDelayMs = parseInt(process.env.MO_REDIS_MAX_DELAY_MS) || 30000;

    return retryAttemptNumber => {
      if (retryAttemptNumber > maxRetryAttempts) {
        logger.logError(`❌ Redis reconnection limit exceeded (${retryAttemptNumber} attempts)`);
        return new Error("Redis connection failed permanently");
      }

      const delayMs = Math.min(retryAttemptNumber * retryDelayMs, maxDelayMs);
      logger.logInfo(`🔄 Redis reconnecting in ${delayMs}ms (attempt ${retryAttemptNumber})`);
      return delayMs;
    };
  }

  #attachEventHandlers(client) {
    client.on("error", error => {
      logger.logError("❌ Redis _client error", error);
    });

    client.on("connect", () => {
      logger.logConnectionEvent("ℹ️ Redis", "_client connected");
    });

    client.on("ready", () => {
      logger.logConnectionEvent("ℹ️ Redis", "_client ready");
    });

    client.on("end", () => {
      logger.logConnectionEvent("Redis", "🔚 _client disconnected");
    });

    client.on("reconnecting", () => {
      logger.logConnectionEvent("Redis", "🔄 _client reconnecting");
    });
  }

  _checkExistingConnection() {
    return this._client && this._client.isOpen;
  }

  async _connectTo() {
    if (!this._client.isOpen) {
      await this._client.connect();
    }
  }

  async _disconnectFrom() {
    if (this._client.isOpen) {
      await this._client.disconnect();
    }
  }

  /**
   * Set a value in the cache with optional TTL
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

  async _getKeyValue(key) {
    return await this._client.get(key);
  }

  async _deleteKey(key) {
    return await this._client.del(key);
  }

  async _checkKeyExists(key) {
    return await this._client.exists(key);
  }

  /**
   * Set expiry for existing key (Redis expects seconds, we convert from milliseconds)
   * @param {string} key - Cache key
   * @param {number} ttlMs - Time-to-live in milliseconds (converted to seconds for Redis)
   * @returns {Promise<boolean>} True if expiry was set
   */
  async _setKeyExpiry(key, ttlMs) {
    const ttlSeconds = Math.ceil(ttlMs / 1000);
    return await this._client.expire(key, ttlSeconds);
  }

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
          logger.logDebug(`📈 Found ${scanCount} keys so far...`);
        }
      }
      return sentKeys;
    } catch (error) {
      logger.logError(`❌ Error scanning Redis keys with pattern '${pattern}':`, error);
      throw error;
    }
  }

  /**
   * Set key with TTL if not exists (Redis expects seconds, we convert from milliseconds)
   * @param {string} key - Cache key
   * @param {any} value - Value to store
   * @param {number} ttlMs - Time-to-live in milliseconds (converted to seconds for Redis)
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
