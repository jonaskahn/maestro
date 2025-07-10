/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Distributed Lock Service
 *
 * Provides cache-based distributed locking with automatic refresh capabilities
 * to ensure exclusive access to shared resources across multiple processes.
 * Works with any compatible cache implementation and includes exponential
 * backoff retry strategies.
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

class DistributedLockService {
  /**
   * Creates a new distributed lock
   * @param {string} lockKey Cache key for the lock
   * @param {number} ttlMs Lock TTL in milliseconds (default: from TTLConfig)
   * @param {Object} cacheInstance Cache instance for lock storage (optional)
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

  async #attemptLockAcquisition(maxWaitTime) {
    const startTime = Date.now();
    let attemptCount = 0;

    while (Date.now() - startTime < maxWaitTime) {
      attemptCount++;
      try {
        if (!this.cacheLayer) {
          logger.logWarning("Cache layer is not enabled, distributed locking is not working.");
          return { success: true };
        }
        const result = await this.cacheLayer.setIfNotExists(this.lockKey, this.lockValue, this.ttl);
        if (result) {
          logger.logInfo(`Lock acquired: ${this.lockKey} (attempt ${attemptCount})`);
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

  #stopAutoRefresh() {
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
      this.refreshInterval = null;
    }
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

  #startAutoRefresh() {
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
    }

    const refreshIntervalMs = Math.floor(this.ttl / 3);
    this.refreshInterval = setInterval(() => this.#performRefresh(), refreshIntervalMs);
  }

  /**
   * Attempts to acquire the distributed lock with retry logic
   * @param {number} maxWaitTime Maximum time to wait for lock acquisition in milliseconds
   * @returns {Promise<boolean>} True if lock was successfully acquired
   * @throws {Error} When lock acquisition fails after all retries
   */
  async acquire(maxWaitTime = TtlConfig.maxWaitTimeMs) {
    await this.#ensureCacheInitialized();

    const acquisitionResult = await this.#attemptLockAcquisition(maxWaitTime);

    if (acquisitionResult.success) {
      this.isLocked = LOCK_STATES.LOCKED;
      this.#startAutoRefresh();
      logger.logInfo(`Lock acquired successfully: ${this.lockKey}`);
      return true;
    }

    logger.logWarning(`Failed to acquire lock: ${this.lockKey} (timeout)`);
    return false;
  }

  /**
   * Releases the distributed lock if currently held
   * @returns {Promise<boolean>} True if lock was successfully released
   * @throws {Error} When lock release fails
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
        logger.logInfo(`Lock released: ${this.lockKey}`);
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

  /**
   * Disconnects and cleans up the distributed lock
   * @returns {Promise<boolean>} True if cleanup was successful
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
   * Gets the current status of the lock
   * @returns {Object} Lock status object
   */
  getStatus() {
    return {
      key: this.lockKey,
      isLocked: this.isLocked,
      ttl: this.ttl,
      autoRefresh: this.refreshInterval !== null,
      cacheAvailable: this.cacheLayer !== null,
    };
  }

  /**
   * Gets the lock key
   * @returns {string} Lock key
   */
  getLockKey() {
    return this.lockKey;
  }

  /**
   * Gets the lock TTL in milliseconds
   * @returns {number} Lock TTL in milliseconds
   */
  getLockTtl() {
    return this.ttl;
  }
}

module.exports = DistributedLockService;
