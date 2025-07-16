/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Distributed Lock Service
 *
 * Provides coordination of access to shared resources across multiple processes
 * using cache-based distributed locking with automatic refresh capabilities.
 * Supports any cache implementation (Redis, Memcached, etc.) and implements
 * retry logic with exponential backoff to handle contention.
 */
const logger = require("./logger-service");
const TTLConfig = require("../config/ttl-config");

const DEFAULT_VALUES = {
  KEY_PREFIX: "DISTRIBUTED_LOCKS",
  BACKOFF_MULTIPLIER: 2,
  BACKOFF_RANDOMNESS: 5,
};

const TtlConfig = TTLConfig.getLockConfig();

const LOCK_STATES = {
  LOCKED: true,
  UNLOCKED: false,
};

/**
 * Distributed Lock Service
 *
 * Implements distributed locking pattern to ensure exclusive access to resources
 * across multiple processes or servers. Uses cache layer for lock storage with
 * automatic TTL-based expiration and refresh to prevent stale locks. Provides
 * advanced features like retry with exponential backoff and jitter.
 */
class DistributedLockService {
  /**
   * Creates a new distributed lock instance
   *
   * @param {string} lockKey - Cache key for the lock (should be unique per resource)
   * @param {number} ttlMs - Lock TTL in milliseconds (defaults to TTLConfig value)
   * @param {Object} cacheInstance - Cache instance for lock storage that implements required methods
   * @param {Function} cacheInstance.connect - Method to connect to cache
   * @param {Function} cacheInstance.setIfNotExists - Method to set value only if key doesn't exist
   * @param {Function} cacheInstance.get - Method to get value from cache
   * @param {Function} cacheInstance.del - Method to delete key from cache
   * @param {Function} cacheInstance.expire - Method to update key expiration
   */
  constructor(lockKey, ttlMs = TtlConfig.ttlMs, cacheInstance = null) {
    if (!lockKey || typeof lockKey !== "string") {
      throw new Error("Lock key must be a non-empty string");
    }

    if (typeof ttlMs !== "number" || ttlMs <= 0) {
      throw new Error("TTL must be a positive number");
    }

    this.lockKey = lockKey;
    this.ttl = ttlMs;
    this.lockValue = `${Date.now()}-${Math.random().toString(36).substring(2, 15)}`;
    this.refreshInterval = null;
    this.isLocked = LOCK_STATES.UNLOCKED;
    this.cacheLayer = cacheInstance;
  }

  /**
   * Attempts to acquire the distributed lock with retry logic
   *
   * Uses exponential backoff with jitter for retries to reduce contention.
   * Automatically starts background refresh to maintain the lock if acquired.
   *
   * @param {number} lockTtl - Expect time to lock
   * @param {number} maxWaitTime - Maximum time to wait for lock acquisition in milliseconds
   * @returns {Promise<boolean>} True if lock was successfully acquired
   * @throws {Error} When lock acquisition fails due to cache errors
   */
  async acquire(lockTtl, maxWaitTime) {
    await this.#ensureCacheInitialized();
    const acquisitionResult = await this.#attemptLockAcquisition(lockTtl, maxWaitTime);
    if (acquisitionResult.success) {
      this.isLocked = LOCK_STATES.LOCKED;
      this.#startAutoRefresh();
      logger.logDebug(`Lock acquired successfully: ${this.lockKey}`);
      return true;
    }
    logger.logWarning(`Failed to acquire lock: ${this.lockKey} (timeout)`);
    return false;
  }

  async #ensureCacheInitialized() {
    if (!this.cacheLayer) {
      logger.logWarning("No cache instance provided, distributed locking disabled");
      return;
    }
    if (!this.cacheLayer.isConnected && this.cacheLayer.connect) {
      await this.cacheLayer.connect();
      logger.logInfo(`Distributed lock initialized with cache: ${this.lockKey}`);
    }
  }

  async #attemptLockAcquisition(lockTtl, maxWaitTime) {
    const realLockTime = lockTtl ?? this.ttl;
    const realMaxWaitTime = maxWaitTime ?? realLockTime * 3;
    const startTime = Date.now();
    let attemptCount = 0;
    while (Date.now() - startTime < realMaxWaitTime) {
      attemptCount++;
      try {
        if (!this.cacheLayer) {
          throw new Error("No cache layer available for lock operations");
        }
        const result = await this.cacheLayer.setIfNotExists(this.lockKey, this.lockValue, realLockTime);
        if (result) {
          logger.logDebug(`Lock acquired: ${this.lockKey} (attempt ${attemptCount})`);
          return { success: true };
        }
        logger.logDebug(`Lock acquisition attempt ${attemptCount} failed, retrying...`);
        const baseDelay = TtlConfig.retryDelayMs;
        const maxDelay = Math.min(
          baseDelay * Math.pow(DEFAULT_VALUES.BACKOFF_MULTIPLIER, attemptCount),
          TtlConfig.maxBackoffMs
        );
        const randomFactor = 1 + (Math.random() * DEFAULT_VALUES.BACKOFF_RANDOMNESS) / 100;
        const delay = Math.floor(maxDelay * randomFactor);
        await new Promise(resolve => setTimeout(resolve, delay));
      } catch (error) {
        logger.logError(`Lock acquisition error for ${this.lockKey}`, error);
        return { success: false, error };
      }
    }
    return { success: false, reason: "timeout" };
  }

  #startAutoRefresh() {
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
    }
    const refreshIntervalMs = Math.floor(this.ttl / 3);
    this.refreshInterval = setInterval(() => this.#performRefresh(), refreshIntervalMs);
  }

  async #performRefresh() {
    try {
      if (this.isLocked !== LOCK_STATES.LOCKED) {
        this.#stopAutoRefresh();
        return;
      }
      if (!this.cacheLayer) {
        return;
      }
      const currentValue = await this.cacheLayer.get(this.lockKey);
      if (currentValue === this.lockValue) {
        await this.cacheLayer.expire(this.lockKey, this.ttl);
        logger.logDebug(`Lock refreshed: ${this.lockKey}`);
      } else {
        logger.logWarning(`Lock lost: ${this.lockKey}`);
        this.isLocked = LOCK_STATES.UNLOCKED;
        this.#stopAutoRefresh();
      }
    } catch (error) {
      logger.logError(`Lock refresh error for ${this.lockKey}`, error);
    }
  }

  #stopAutoRefresh() {
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
      this.refreshInterval = null;
    }
  }

  /**
   * Releases the distributed lock if currently held
   *
   * Verifies owner identity before releasing to prevent accidental releases
   * by other processes. Stops the auto-refresh mechanism regardless of outcome.
   *
   * @returns {Promise<boolean>} True if lock was successfully released
   * @throws {Error} When lock release fails due to cache errors
   */
  async release() {
    if (this.isLocked !== LOCK_STATES.LOCKED) {
      return true;
    }
    try {
      await this.#ensureCacheInitialized();
      const releaseResult = await this.#attemptLockRelease();
      this.#stopAutoRefresh();
      this.isLocked = LOCK_STATES.UNLOCKED;
      if (releaseResult.success) {
        logger.logDebug(`Lock released: ${this.lockKey}`);
      } else {
        logger.logWarning(`Failed to release lock: ${this.lockKey} (not lock owner)`);
      }
      return releaseResult.success;
    } catch (error) {
      logger.logError(`Lock release error for ${this.lockKey}`, error);
      this.#stopAutoRefresh();
      this.isLocked = LOCK_STATES.UNLOCKED;
      return false;
    }
  }

  async #attemptLockRelease() {
    try {
      if (!this.cacheLayer) {
        return { success: true, reason: "no_cache" };
      }
      if (this.isLocked !== LOCK_STATES.LOCKED) {
        return { success: true, reason: "not_locked" };
      }
      const currentValue = await this.cacheLayer.get(this.lockKey);
      if (currentValue === this.lockValue) {
        await this.cacheLayer.del(this.lockKey);
        return { success: true };
      } else if (!currentValue) {
        return { success: true, reason: "already_released" };
      } else {
        return { success: false, reason: "not_lock_owner" };
      }
    } catch (error) {
      logger.logError(`Lock release error for ${this.lockKey}`, error);
      return { success: false, error };
    }
  }

  /**
   * Disconnects from the cache layer and cleans up resources
   *
   * Releases any held locks and stops all background processes.
   *
   * @returns {Promise<void>}
   * @throws {Error} When disconnection fails
   */
  async disconnect() {
    await this.release().catch(error => {
      logger.logWarning(`Error releasing lock during disconnect: ${this.lockKey}`, error);
    });
    this.#stopAutoRefresh();
    this.isLocked = LOCK_STATES.UNLOCKED;
    if (this.cacheLayer && typeof this.cacheLayer.disconnect === "function") {
      await this.cacheLayer.disconnect();
    }
    logger.logInfo(`Distributed lock disconnected: ${this.lockKey}`);
  }

  /**
   * Returns comprehensive lock status information
   *
   * @returns {Object} Status object with lock details and cache information
   */
  getStatus() {
    return {
      lockKey: this.lockKey,
      lockValue: this.lockValue,
      ttl: this.ttl,
      isLocked: this.isLocked,
      hasAutoRefresh: Boolean(this.refreshInterval),
      cacheConnected: this.cacheLayer && this.cacheLayer.isConnected,
      hasCacheLayer: Boolean(this.cacheLayer),
    };
  }

  /**
   * Gets the lock key used by this instance
   *
   * @returns {string} Lock key
   */
  getLockKey() {
    return this.lockKey;
  }

  /**
   * Gets the lock TTL in milliseconds
   *
   * @returns {number} Lock TTL in milliseconds
   */
  getLockTtl() {
    return this.ttl;
  }
}

module.exports = DistributedLockService;
