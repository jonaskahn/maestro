/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Abstract Cache Layer Base Class for cache implementation providers
 *
 * Provides unified interface for key-value operations across Redis, Memcached, in-memory cache
 * with job orchestration features including processing state management and message tracking.
 * This class must be extended by specific cache implementations.
 */
const logger = require("../services/logger-service");
const TTLConfig = require("../config/ttl-config");

const CacheConfig = TTLConfig.getCacheConfig();

const KEY_SUFFIXES = {
  PROCESSING: "_PROCESSING:",
  SUPPRESSION: "_SUPPRESSION:",
};

const CACHE_OPERATIONS = {
  SET: "Set key",
  GET: "Get key",
  DELETE: "Delete key",
  SET_IF_NOT_EXISTS: "Set key if not exists",
  SET_EXPIRY: "Set expiry",
  SET_PROCESSING: "Set processing key",
  REMOVE_PROCESSING: "Removed processing key",
  SET_SUPPRESSION: "Set suppression key",
};

const CONNECTION_STATES = {
  CONNECTED: true,
  DISCONNECTED: false,
};

const ENV_KEYS = {
  CACHE_KEY_PREFIX: ["MO_CACHE_KEY_PREFIX"],
  PROCESSING_SUFFIX: ["MO_CACHE_KEY_SUFFIXES_PROCESSING"],
  SUPPRESSION_SUFFIX: ["MO_CACHE_KEY_SUFFIXES_SUPPRESSION"],
};

/**
 * Abstract Cache Layer Base Class for cache implementation providers
 * Provides unified interface for key-value operations across Redis, Memcached, in-memory cache
 * with job orchestration features including processing state management and message tracking
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

    if (!this.isNonEmptyString(config.keyPrefix)) {
      throw new Error("keyPrefix is required and must be a non-empty string");
    }

    this.implementation = config.implementation;

    const processingKeySuffix = this.getEnvironmentValue(ENV_KEYS.PROCESSING_SUFFIX) || KEY_SUFFIXES.PROCESSING;
    const freezingKeySuffix = this.getEnvironmentValue(ENV_KEYS.SUPPRESSION_SUFFIX) || KEY_SUFFIXES.SUPPRESSION;

    this.config = {
      processingPrefix: `${config.keyPrefix}${processingKeySuffix}`,
      suppressionPrefix: `${config.keyPrefix}${freezingKeySuffix}`,
      processingTtl: config.processingTtl || CacheConfig.processingTtl,
      suppressionTtl: config.suppressionTtl || config.processingTtl * 3 || CacheConfig.freezingTtl,
      connectionOptions: config.connectionOptions || {},
      retryOptions: config.retryOptions || {},
    };

    logger.logDebug("ℹ️ Cache configuration", {
      implementation: this.implementation,
      processingTtl: this.config.processingTtl,
      suppressionTtl: this.config.suppressionTtl,
    });

    this._isConnected = CONNECTION_STATES.DISCONNECTED;
  }

  isNonEmptyString(value) {
    return typeof value === "string" && value.trim().length > 0;
  }

  getEnvironmentValue(keys) {
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

  async _checkExistingConnection() {
    throw new Error("_checkExistingConnection method must be implemented by subclass");
  }

  async _connectTo() {
    throw new Error("_connectTo method must be implemented by subclass");
  }

  /**
   * Disconnect from cache implementation
   * @returns {Promise<void>}
   */
  async disconnect() {
    if (this.isConnected()) {
      try {
        await this._disconnectFrom();
        this._isConnected = CONNECTION_STATES.DISCONNECTED;
      } catch (error) {
        logger.logWarning(`Error disconnecting from ${this.implementation} cache`, error);
        this._isConnected = CONNECTION_STATES.DISCONNECTED;
      }
    }
  }

  isConnected() {
    return this._isConnected === CONNECTION_STATES.CONNECTED;
  }

  isDisconnected() {
    return this._isConnected === CONNECTION_STATES.DISCONNECTED;
  }

  async _disconnectFrom() {
    throw new Error("_disconnectFrom method must be implemented by subclass");
  }

  validateKeyValue(key, value) {
    if (!this.isNonEmptyString(key)) {
      throw new Error("Key must be a non-empty string");
    }

    if (value === undefined || value === null) {
      throw new Error("Value cannot be undefined or null");
    }
  }

  ensureConnected() {
    if (this.isDisconnected()) {
      throw new Error("Cache is not connected");
    }
  }

  logKeyOperation(operation, key, ttl = null) {
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
   * @param {number} ttlMs TTL in milliseconds
   * @returns {Promise<boolean>} Success indicator
   */
  async set(key, value, ttlMs) {
    this.validateKeyValue(key, value);
    this.ensureConnected();
    this.logKeyOperation(CACHE_OPERATIONS.SET, key, ttlMs);
    return await this._setKeyValue(key, value, ttlMs);
  }

  async _setKeyValue(key, value, ttlMs) {
    throw new Error("_setKeyValue method must be implemented by subclass");
  }

  /**
   * Get a value from the cache
   * @param {string} key Key to retrieve
   * @returns {Promise<any>} Retrieved value or null
   */
  async get(key) {
    if (!this.isNonEmptyString(key)) {
      throw new Error("Key must be a non-empty string");
    }
    this.ensureConnected();
    this.logKeyOperation(CACHE_OPERATIONS.GET, key);
    return await this._getKeyValue(key);
  }

  async _getKeyValue(_key) {
    throw new Error("_getKeyValue method must be implemented by subclass");
  }

  /**
   * Delete a key from the cache
   * @param {string} key Key to delete
   * @returns {Promise<boolean>} Success indicator
   */
  async del(key) {
    if (!this.isNonEmptyString(key)) {
      throw new Error("Key must be a non-empty string");
    }
    this.ensureConnected();
    this.logKeyOperation(CACHE_OPERATIONS.DELETE, key);
    return await this._deleteKey(key);
  }

  async _deleteKey(_key) {
    throw new Error("_deleteKey method must be implemented by subclass");
  }

  /**
   * Set a value only if the key doesn't exist
   * @param {string} key Key to set
   * @param {string|Object} value Value to store
   * @param {number} ttlMs TTL in milliseconds
   * @returns {Promise<boolean>} True if set, false if key exists
   */
  async setIfNotExists(key, value, ttlMs) {
    this.validateKeyValue(key, value);
    this.ensureConnected();
    this.logKeyOperation(CACHE_OPERATIONS.SET_IF_NOT_EXISTS, key, ttlMs);
    return await this._setKeyIfNotExists(key, value, ttlMs);
  }

  async _setKeyIfNotExists(_key, _value, _ttlMs) {
    throw new Error("_setKeyIfNotExists method must be implemented by subclass");
  }

  /**
   * Check if a key exists in the cache
   * @param {string} key Key to check
   * @returns {Promise<boolean>} True if exists
   */
  async exists(key) {
    if (!this.isNonEmptyString(key)) {
      throw new Error("Key must be a non-empty string");
    }
    this.ensureConnected();
    return await this._checkKeyExists(key);
  }

  async _checkKeyExists(_key) {
    throw new Error("_checkKeyExists method must be implemented by subclass");
  }

  /**
   * Set or update the expiration for a key
   * @param {string} key Key to update
   * @param {number} ttlMs New TTL in milliseconds
   * @returns {Promise<boolean>} Success indicator
   */
  async expire(key, ttlMs) {
    if (!this.isNonEmptyString(key)) {
      throw new Error("Key must be a non-empty string");
    }

    if (typeof ttlMs !== "number" || ttlMs <= 0) {
      throw new Error("TTL must be a positive number");
    }

    this.ensureConnected();
    this.logKeyOperation(CACHE_OPERATIONS.SET_EXPIRY, key, ttlMs);
    return await this._setKeyExpiry(key, ttlMs);
  }

  async _setKeyExpiry(_key, _ttlMs) {
    throw new Error("_setKeyExpiry method must be implemented by subclass");
  }

  /**
   * Find keys matching a pattern
   * @param {string} pattern Pattern to match
   * @returns {Promise<string[]>} Matching keys
   */
  async keys(pattern) {
    if (!this.isNonEmptyString(pattern)) {
      throw new Error("Pattern must be a non-empty string");
    }
    this.ensureConnected();

    const keys = await this._findKeysByPattern(pattern);
    logger.logDebug(`Found ${keys.length} keys matching pattern: ${pattern}`);
    return keys;
  }

  async _findKeysByPattern(_pattern) {
    throw new Error("_findKeysByPattern method must be implemented by subclass");
  }

  /**
   * Mark an item as being processed
   * @param {string} itemId Item identifier
   * @returns {Promise<boolean>} Success indicator
   */
  async markAsProcessing(itemId) {
    if (!this.isNonEmptyString(itemId)) {
      throw new Error("Item ID must be a non-empty string");
    }
    this.ensureConnected();
    const key = `${this.config.processingPrefix}${itemId}`;
    return await this.setIfNotExists(key, itemId, this.config.processingTtl);
  }

  /**
   * Mark an item as being processed
   * @param {string} itemId Item identifier
   * @returns {Promise<boolean>} Success indicator
   */
  async markAsCompletedProcessing(itemId) {
    if (!this.isNonEmptyString(itemId)) {
      throw new Error("Item ID must be a non-empty string");
    }
    this.ensureConnected();
    const key = `${this.config.processingPrefix}${itemId}`;
    return await this.del(key);
  }

  async markAsSuppressed(itemId) {
    if (!this.isNonEmptyString(itemId)) {
      throw new Error("Item ID must be a non-empty string");
    }
    this.ensureConnected();
    const key = `${this.config.suppressionPrefix}${itemId}`;
    return await this.setIfNotExists(key, itemId, this.config.suppressionTtl);
  }

  /**
   * Check if a message was sent recently
   * @param {string} itemId Item identifier
   * @returns {Promise<boolean>} True if sent recently
   */
  async isSuppressedRecently(itemId) {
    if (!this.isNonEmptyString(itemId)) {
      throw new Error("Item ID must be a non-empty string");
    }
    this.ensureConnected();

    const suppressedKey = `${this.config.suppressionPrefix}${itemId}`;
    return await this.exists(suppressedKey);
  }

  /**
   * Get IDs of all items currently being processed
   * @returns {Promise<string[]>} Processing item IDs
   */
  async getProcessingIds() {
    return await this.getIdsByPrefix("processing", this.config.processingPrefix);
  }

  async getIdsByPrefix(typeName, keyPrefix) {
    try {
      const pattern = `${keyPrefix}*`;
      const keys = await this.keys(pattern);

      const ids = keys.map(key => key.substring(keyPrefix.length));
      logger.logDebug(`Found ${ids.length} ${typeName} IDs`);

      return ids;
    } catch (error) {
      logger.logWarning(`Failed to retrieve ${typeName} IDs`, error);
      return [];
    }
  }

  /**
   * Get IDs of all items in freezing state
   * @returns {Promise<string[]>} Freezing item IDs
   */
  async getSuppressedIds() {
    return await this.getIdsByPrefix("suppressed", this.config.suppressionPrefix);
  }
}

module.exports = AbstractCache;
