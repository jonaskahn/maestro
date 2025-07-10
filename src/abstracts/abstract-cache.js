/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Abstract Cache Layer Base Class
 *
 * Provides unified interface for key-value operations across various cache implementations
 * such as Redis, Memcached, and in-memory cache. Supports job orchestration features including
 * processing state management and message tracking. This class must be extended by specific
 * cache implementations.
 */
const logger = require("../services/logger-service");
const TTLConfig = require("../config/ttl-config");

const CacheConfig = TTLConfig.getCacheConfig();

const KEY_SUFFIXES = {
  PROCESSING: "_PROCESSING:",
  FREEZING: "_FREEZING:",
};

const CACHE_OPERATIONS = {
  SET: "Set key",
  GET: "Get key",
  DELETE: "Delete key",
  SET_IF_NOT_EXISTS: "Set key if not exists",
  SET_EXPIRY: "Set expiry",
  SET_PROCESSING: "Set processing key",
  REMOVE_PROCESSING: "Removed processing key",
  SET_FREEZING: "Set freezing key",
};

const CONNECTION_STATES = {
  CONNECTED: true,
  DISCONNECTED: false,
};

const ENV_KEYS = {
  CACHE_KEY_PREFIX: ["MO_CACHE_KEY_PREFIX"],
  PROCESSING_SUFFIX: ["MO_CACHE_KEY_SUFFIXES_PROCESSING"],
  FREEZING_SUFFIX: ["MO_CACHE_KEY_SUFFIXES_FREEZING"],
};

/**
 * Abstract Cache Layer Base Class
 *
 * Base class for implementing cache providers that provides a unified interface
 * for key-value operations with job orchestration capabilities.
 */
class AbstractCache {
  _isConnected;

  /**
   * Create cache instance with configuration validation and initialization
   * @param {Object} config Cache configuration with keyPrefix, TTL settings, and connection options
   */
  constructor(config) {
    if (this.constructor === AbstractCache) {
      throw new Error("AbstractCache cannot be instantiated directly");
    }

    if (!config || typeof config !== "object") {
      throw new Error("Configuration must be an object");
    }

    if (!this.#isNonEmptyString(config.keyPrefix)) {
      throw new Error("keyPrefix is required and must be a non-empty string");
    }

    this.implementation = config.implementation;

    const processingKeySuffix = this.#getEnvironmentValue(ENV_KEYS.PROCESSING_SUFFIX) || KEY_SUFFIXES.PROCESSING;
    const freezingKeySuffix = this.#getEnvironmentValue(ENV_KEYS.FREEZING_SUFFIX) || KEY_SUFFIXES.FREEZING;

    this.config = {
      processingPrefix: `${config.keyPrefix}${processingKeySuffix}`,
      suppressionPrefix: `${config.keyPrefix}${freezingKeySuffix}`,
      processingTtl: config.processingTtl || CacheConfig.processingTtl,
      suppressionTtl: config.suppressionTtl || config.processingTtl * 3 || CacheConfig.freezingTtl,
      connectionOptions: config.connectionOptions || {},
      retryOptions: config.retryOptions || {},
    };

    logger.logDebug("Cache configuration", {
      implementation: this.implementation,
      processingTtl: this.config.processingTtl,
      suppressionTtl: this.config.suppressionTtl,
    });

    this._isConnected = CONNECTION_STATES.DISCONNECTED;
  }

  #isNonEmptyString(value) {
    return typeof value === "string" && value.trim().length > 0;
  }

  #getEnvironmentValue(keys) {
    for (const key of keys) {
      const value = process.env[key];
      if (value !== undefined) {
        return value;
      }
    }
    return null;
  }

  /**
   * Establish connection to cache implementation
   * @returns {Promise<void>}
   */
  async connect() {
    if (await this._checkExistingConnection()) {
      this._isConnected = CONNECTION_STATES.CONNECTED;
      logger.logDebug(`Using existing connection to ${this.implementation} cache`);
      return;
    }

    try {
      await this._connectTo();
      this._isConnected = CONNECTION_STATES.CONNECTED;
    } catch (error) {
      this._isConnected = CONNECTION_STATES.DISCONNECTED;
      logger.logError(`Failed to connect to ${this.implementation} cache`, error);
      throw error;
    }
  }

  /**
   * Check if cache already has active connection
   * @abstract
   * @returns {Promise<boolean>} True if connection already exists
   */
  async _checkExistingConnection() {
    throw new Error("_checkExistingConnection method must be implemented by subclass");
  }

  /**
   * Connect to the cache system
   * @abstract
   * @returns {Promise<void>}
   */
  async _connectTo() {
    throw new Error("_connectTo method must be implemented by subclass");
  }

  /**
   * Disconnect from cache implementation
   * @returns {Promise<void>}
   */
  async disconnect() {
    if (this._isConnected === CONNECTION_STATES.CONNECTED) {
      try {
        await this._disconnectFrom();
        this._isConnected = CONNECTION_STATES.DISCONNECTED;
      } catch (error) {
        logger.logWarning(`Error disconnecting from ${this.implementation} cache`, error);
        this._isConnected = CONNECTION_STATES.DISCONNECTED;
      }
    }
  }

  /**
   * Disconnect from the cache system
   * @abstract
   * @returns {Promise<void>}
   */
  async _disconnectFrom() {
    throw new Error("_disconnectFrom method must be implemented by subclass");
  }

  #validateKeyValue(key, value) {
    if (!this.#isNonEmptyString(key)) {
      throw new Error("Key must be a non-empty string");
    }

    if (value === undefined || value === null) {
      throw new Error("Value cannot be undefined or null");
    }
  }

  #ensureConnected() {
    if (this._isConnected === CONNECTION_STATES.DISCONNECTED) {
      throw new Error("Cache is not connected");
    }
  }

  #logKeyOperation(operation, key, ttl = null) {
    const logData = { key, operation };
    if (ttl) {
      logData.ttl = ttl;
    }
    logger.logDebug(`${operation}: ${key}`, logData);
  }

  /**
   * Set a value in the cache
   * @param {string} key Key to set
   * @param {string|Object} value Value to store
   * @param {Object} options Options including TTL
   * @returns {Promise<boolean>} Success indicator
   */
  async set(key, value, options = {}) {
    const ttl = options?.ttl || CacheConfig.defaultTtl;
    this.#validateKeyValue(key, value);
    this.#ensureConnected();
    this.#logKeyOperation(CACHE_OPERATIONS.SET, key, ttl);
    return await this._setKeyValue(key, value, ttl);
  }

  /**
   * Set a value in the cache implementation
   * @abstract
   * @param {string} key Key to set
   * @param {string|Object} value Value to store
   * @param {number} ttlMs TTL in milliseconds
   * @returns {Promise<boolean>} Success indicator
   */
  async _setKeyValue(key, value, ttlMs) {
    throw new Error("_setKeyValue method must be implemented by subclass");
  }

  /**
   * Get a value from the cache
   * @param {string} key Key to retrieve
   * @returns {Promise<any>} Retrieved value or null
   */
  async get(key) {
    if (!this.#isNonEmptyString(key)) {
      throw new Error("Key must be a non-empty string");
    }
    this.#ensureConnected();
    this.#logKeyOperation(CACHE_OPERATIONS.GET, key);
    return await this._getKeyValue(key);
  }

  /**
   * Get a value from the cache implementation
   * @abstract
   * @param {string} key Key to retrieve
   * @returns {Promise<any>} Retrieved value or null
   */
  async _getKeyValue(key) {
    throw new Error("_getKeyValue method must be implemented by subclass");
  }

  /**
   * Delete a key from the cache
   * @param {string} key Key to delete
   * @returns {Promise<boolean>} Success indicator
   */
  async del(key) {
    if (!this.#isNonEmptyString(key)) {
      throw new Error("Key must be a non-empty string");
    }
    this.#ensureConnected();
    this.#logKeyOperation(CACHE_OPERATIONS.DELETE, key);
    return await this._deleteKey(key);
  }

  /**
   * Delete a key from the cache implementation
   * @abstract
   * @param {string} key Key to delete
   * @returns {Promise<boolean>} Success indicator
   */
  async _deleteKey(key) {
    throw new Error("_deleteKey method must be implemented by subclass");
  }

  /**
   * Set a value only if the key doesn't exist
   * @param {string} key Key to set
   * @param {string|Object} value Value to store
   * @param {Object} options Options including TTL
   * @returns {Promise<boolean>} True if set, false if key exists
   */
  async setIfNotExists(key, value, options = {}) {
    const ttl = options?.ttl || CacheConfig.defaultTtl;
    this.#validateKeyValue(key, value);
    this.#ensureConnected();
    this.#logKeyOperation(CACHE_OPERATIONS.SET_IF_NOT_EXISTS, key, ttl);
    return await this._setKeyIfNotExists(key, value, ttl);
  }

  /**
   * Set a value only if key doesn't exist in cache implementation
   * @abstract
   * @param {string} key Key to set
   * @param {string|Object} value Value to store
   * @param {number} ttlMs TTL in milliseconds
   * @returns {Promise<boolean>} True if set, false if key exists
   */
  async _setKeyIfNotExists(key, value, ttlMs) {
    throw new Error("_setKeyIfNotExists method must be implemented by subclass");
  }

  /**
   * Check if a key exists in the cache
   * @param {string} key Key to check
   * @returns {Promise<boolean>} True if key exists
   */
  async exists(key) {
    if (!this.#isNonEmptyString(key)) {
      throw new Error("Key must be a non-empty string");
    }
    this.#ensureConnected();
    return await this._checkKeyExists(key);
  }

  /**
   * Check if a key exists in the cache implementation
   * @abstract
   * @param {string} key Key to check
   * @returns {Promise<boolean>} True if key exists
   */
  async _checkKeyExists(key) {
    throw new Error("_checkKeyExists method must be implemented by subclass");
  }

  /**
   * Update expiration time for a key
   * @param {string} key Key to update
   * @param {number} ttlMs New TTL in milliseconds
   * @returns {Promise<boolean>} Success indicator
   */
  async expire(key, ttlMs) {
    if (!this.#isNonEmptyString(key)) {
      throw new Error("Key must be a non-empty string");
    }
    if (!Number.isInteger(ttlMs) || ttlMs <= 0) {
      throw new Error("TTL must be a positive integer");
    }
    this.#ensureConnected();
    this.#logKeyOperation(CACHE_OPERATIONS.SET_EXPIRY, key, ttlMs);
    return await this._setKeyExpiry(key, ttlMs);
  }

  /**
   * Update expiration time for a key in cache implementation
   * @abstract
   * @param {string} key Key to update
   * @param {number} ttlMs New TTL in milliseconds
   * @returns {Promise<boolean>} Success indicator
   */
  async _setKeyExpiry(key, ttlMs) {
    throw new Error("_setKeyExpiry method must be implemented by subclass");
  }

  /**
   * Find keys matching a pattern
   * @param {string} pattern Pattern to match
   * @returns {Promise<Array<string>>} Matched keys
   */
  async keys(pattern) {
    if (!this.#isNonEmptyString(pattern)) {
      throw new Error("Pattern must be a non-empty string");
    }
    this.#ensureConnected();
    return await this._findKeysByPattern(pattern);
  }

  /**
   * Find keys matching a pattern in cache implementation
   * @abstract
   * @param {string} pattern Pattern to match
   * @returns {Promise<Array<string>>} Matched keys
   */
  async _findKeysByPattern(pattern) {
    throw new Error("_findKeysByPattern method must be implemented by subclass");
  }

  /**
   * Mark an item as being processed
   * @param {string} itemId Item identifier
   * @returns {Promise<boolean>} Success indicator
   */
  async markAsProcessing(itemId) {
    if (!this.#isNonEmptyString(itemId)) {
      throw new Error("itemId must be a non-empty string");
    }
    this.#ensureConnected();

    const key = `${this.config.processingPrefix}${itemId}`;
    this.#logKeyOperation(CACHE_OPERATIONS.SET_PROCESSING, key, this.config.processingTtl);

    return await this._setKeyIfNotExists(key, "1", this.config.processingTtl);
  }

  /**
   * Mark an item as completed processing
   * @param {string} itemId Item identifier
   * @returns {Promise<boolean>} Success indicator
   */
  async markAsCompletedProcessing(itemId) {
    if (!this.#isNonEmptyString(itemId)) {
      throw new Error("itemId must be a non-empty string");
    }
    this.#ensureConnected();

    const key = `${this.config.processingPrefix}${itemId}`;
    this.#logKeyOperation(CACHE_OPERATIONS.REMOVE_PROCESSING, key);

    return await this._deleteKey(key);
  }

  /**
   * Mark an item as suppressed for deduplication
   * @param {string} itemId Item identifier
   * @returns {Promise<boolean>} Success indicator
   */
  async markAsSuppressed(itemId) {
    if (!this.#isNonEmptyString(itemId)) {
      throw new Error("itemId must be a non-empty string");
    }
    this.#ensureConnected();

    const key = `${this.config.suppressionPrefix}${itemId}`;
    this.#logKeyOperation(CACHE_OPERATIONS.SET_FREEZING, key, this.config.suppressionTtl);

    return await this._setKeyValue(key, "1", this.config.suppressionTtl);
  }

  /**
   * Check if an item has been suppressed recently
   * @param {string} itemId Item identifier
   * @returns {Promise<boolean>} True if item is suppressed
   */
  async isSuppressedRecently(itemId) {
    if (!this.#isNonEmptyString(itemId)) {
      throw new Error("itemId must be a non-empty string");
    }
    this.#ensureConnected();

    const key = `${this.config.suppressionPrefix}${itemId}`;
    return await this._checkKeyExists(key);
  }

  /**
   * Get all currently processing item IDs
   * @returns {Promise<Array<string>>} Processing item IDs
   */
  async getProcessingIds() {
    this.#ensureConnected();

    const pattern = `${this.config.processingPrefix}*`;
    const keys = await this._findKeysByPattern(pattern);

    return keys.map(key => {
      const prefixLength = this.config.processingPrefix.length;
      return key.substring(prefixLength);
    });
  }

  /**
   * Get all currently suppressed item IDs
   * @returns {Promise<Array<string>>} Suppressed item IDs
   */
  async getSuppressedIds() {
    this.#ensureConnected();

    const pattern = `${this.config.suppressionPrefix}*`;
    const keys = await this._findKeysByPattern(pattern);

    return keys.map(key => {
      const prefixLength = this.config.suppressionPrefix.length;
      return key.substring(prefixLength);
    });
  }

  /**
   * Check if cache is connected
   * @returns {boolean} True if connected
   */
  isConnected() {
    return this._isConnected === CONNECTION_STATES.CONNECTED;
  }
}

module.exports = AbstractCache;
