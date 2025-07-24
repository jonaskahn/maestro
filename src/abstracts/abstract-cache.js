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
const TtlConfig = require("../config/ttl-config");
const TopicConfig = TtlConfig.getTopicConfig();

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
 * Abstract Cache Layer Base Class
 *
 * Provides unified interface for key-value operations across different cache providers
 * with job orchestration features including processing state management and message tracking.
 */
class AbstractCache {
  _isConnected;

  /**
   * Creates a cache instance with configuration validation and initialization
   *
   * @param {Object} config - Cache configuration object
   * @param {string} config.keyPrefix - Required prefix for all cache keys
   * @param {number} [config.processingTtl] - TTL for processing state keys (defaults to CacheConfig value)
   * @param {number} [config.suppressionTtl] - TTL for suppression keys (defaults to 3x processing TTL)
   * @param {Object} [config.connectionOptions] - Cache provider-specific connection options
   * @param {Object} [config.retryOptions] - Options for connection retry behavior
   * @param {string} [config.implementation] - Cache implementation type identifier
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
      processingTtl: config.processingTtl || TopicConfig.processingTtl,
      suppressionTtl: config.suppressionTtl || config.processingTtl * 3 || TopicConfig.suppressionTtl,
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
   * Establishes connection to the cache implementation
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
   * Checks if a connection already exists
   * @returns {Promise<boolean>}
   */
  async _checkExistingConnection() {
    throw new Error("_checkExistingConnection method must be implemented by subclass");
  }

  /**
   * Establishes connection to the specific cache implementation
   * @returns {Promise<void>}
   */
  async _connectTo() {
    throw new Error("_connectTo method must be implemented by subclass");
  }

  /**
   * Disconnects from the cache implementation
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

  /**
   * Checks if the cache is currently connected
   * @returns {boolean}
   */
  isConnected() {
    return this._isConnected === CONNECTION_STATES.CONNECTED;
  }

  /**
   * Checks if the cache is currently disconnected
   * @returns {boolean}
   */
  isDisconnected() {
    return this._isConnected === CONNECTION_STATES.DISCONNECTED;
  }

  /**
   * Disconnects from the specific cache implementation
   * @returns {Promise<void>}
   */
  async _disconnectFrom() {
    throw new Error("_disconnectFrom method must be implemented by subclass");
  }

  /**
   * Validates key and value for storage operations
   * @param {string} key - Key to validate
   * @param {any} value - Value to validate
   * @throws {Error} When validation fails
   */
  validateKeyValue(key, value) {
    if (!this.isNonEmptyString(key)) {
      throw new Error("Key must be a non-empty string");
    }

    if (value === undefined || value === null) {
      throw new Error("Value cannot be undefined or null");
    }
  }

  /**
   * Ensures the cache is connected before operations
   * @throws {Error} When cache is not connected
   */
  ensureConnected() {
    if (this.isDisconnected()) {
      throw new Error("Cache is not connected");
    }
  }

  /**
   * Logs key operations for debugging
   * @param {string} operation - Operation being performed
   * @param {string} key - Key being operated on
   * @param {number} ttl - Time-to-live value in milliseconds
   */
  logKeyOperation(operation, key, ttl = null) {
    const logData = { key, operation };
    if (ttl) {
      logData.ttl = ttl;
    }
    logger.logDebug(`${operation}: ${key}`, logData);
  }

  /**
   * Sets a value in the cache
   * @param {string} key - Key to set
   * @param {string|Object} value - Value to store
   * @param {number} ttlMs - TTL in milliseconds
   * @returns {Promise<boolean>} Success indicator
   */
  async set(key, value, ttlMs) {
    this.validateKeyValue(key, value);
    this.ensureConnected();
    this.logKeyOperation(CACHE_OPERATIONS.SET, key, ttlMs);
    return await this._setKeyValue(key, value, ttlMs);
  }

  /**
   * Implementation-specific method to set a value
   * @param {string} key - Key to set
   * @param {string|Object} value - Value to store
   * @param {number} ttlMs - TTL in milliseconds
   * @returns {Promise<boolean>} Success indicator
   */
  async _setKeyValue(key, value, ttlMs) {
    throw new Error("_setKeyValue method must be implemented by subclass");
  }

  /**
   * Gets a value from the cache
   * @param {string} key - Key to retrieve
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

  /**
   * Implementation-specific method to get a value
   * @param {string} _key - Key to retrieve
   * @returns {Promise<any>} Retrieved value or null
   */
  async _getKeyValue(_key) {
    throw new Error("_getKeyValue method must be implemented by subclass");
  }

  /**
   * Deletes a key from the cache
   * @param {string} key - Key to delete
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

  /**
   * Implementation-specific method to delete a key
   * @param {string} _key - Key to delete
   * @returns {Promise<boolean>} Success indicator
   */
  async _deleteKey(_key) {
    throw new Error("_deleteKey method must be implemented by subclass");
  }

  /**
   * Sets a value only if the key doesn't exist
   * @param {string} key - Key to set
   * @param {string|Object} value - Value to store
   * @param {number} ttlMs - TTL in milliseconds
   * @returns {Promise<boolean>} True if set, false if key exists
   */
  async setIfNotExists(key, value, ttlMs) {
    this.validateKeyValue(key, value);
    this.ensureConnected();
    this.logKeyOperation(CACHE_OPERATIONS.SET_IF_NOT_EXISTS, key, ttlMs);
    return await this._setKeyIfNotExists(key, value, ttlMs);
  }

  /**
   * Implementation-specific method to set a value if key doesn't exist
   * @param {string} _key - Key to set
   * @param {string|Object} _value - Value to store
   * @param {number} _ttlMs - TTL in milliseconds
   * @returns {Promise<boolean>} True if set, false if key exists
   */
  async _setKeyIfNotExists(_key, _value, _ttlMs) {
    throw new Error("_setKeyIfNotExists method must be implemented by subclass");
  }

  /**
   * Checks if a key exists in the cache
   * @param {string} key - Key to check
   * @returns {Promise<boolean>} True if exists
   */
  async exists(key) {
    if (!this.isNonEmptyString(key)) {
      throw new Error("Key must be a non-empty string");
    }
    this.ensureConnected();
    return await this._checkKeyExists(key);
  }

  /**
   * Implementation-specific method to check if a key exists
   * @param {string} _key - Key to check
   * @returns {Promise<boolean>} True if exists
   */
  async _checkKeyExists(_key) {
    throw new Error("_checkKeyExists method must be implemented by subclass");
  }

  /**
   * Sets or updates the expiration for a key
   * @param {string} key - Key to update
   * @param {number} ttlMs - New TTL in milliseconds
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

  /**
   * Implementation-specific method to set key expiry
   * @param {string} _key - Key to update
   * @param {number} _ttlMs - TTL in milliseconds
   * @returns {Promise<boolean>} Success indicator
   */
  async _setKeyExpiry(_key, _ttlMs) {
    throw new Error("_setKeyExpiry method must be implemented by subclass");
  }

  /**
   * Finds keys matching a pattern
   * @param {string} pattern - Pattern to match
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

  /**
   * Implementation-specific method to find keys by pattern
   * @param {string} _pattern - Pattern to match
   * @returns {Promise<string[]>} Matching keys
   */
  async _findKeysByPattern(_pattern) {
    throw new Error("_findKeysByPattern method must be implemented by subclass");
  }

  /**
   * Marks an item as being processed
   * @param {string} itemId - Item identifier
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
   * Marks an item as completed processing
   * @param {string} itemId - Item identifier
   * @returns {Promise<boolean>} Success indicator
   */
  async clearProcessingState(itemId) {
    if (!this.isNonEmptyString(itemId)) {
      throw new Error("Item ID must be a non-empty string");
    }
    this.ensureConnected();
    const key = `${this.config.processingPrefix}${itemId}`;
    this.logKeyOperation(CACHE_OPERATIONS.DELETE, key);
    return await this.del(key);
  }

  /**
   * Marks an item as suppressed to prevent duplicate processing
   * @param {string} itemId - Item identifier
   * @returns {Promise<boolean>} Success indicator
   */
  async markAsSuppressed(itemId) {
    if (!this.isNonEmptyString(itemId)) {
      throw new Error("Item ID must be a non-empty string");
    }
    this.ensureConnected();
    const key = `${this.config.suppressionPrefix}${itemId}`;
    return await this.setIfNotExists(key, itemId, this.config.suppressionTtl);
  }

  async clearSuppressionState(itemId) {
    if (!this.isNonEmptyString(itemId)) {
      throw new Error("Item ID must be a non-empty string");
    }
    this.ensureConnected();
    const key = `${this.config.suppressionPrefix}${itemId}`;
    this.logKeyOperation(CACHE_OPERATIONS.DELETE, key);
    return await this.del(key);
  }

  /**
   * Checks if an item was processed recently
   * @param {string} itemId - Item identifier
   * @returns {Promise<boolean>} True if suppressed recently
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
   * Gets IDs of all items currently being processed
   * @returns {Promise<string[]>} Processing item IDs
   */
  async getProcessingIds() {
    return await this.getIdsByPrefix("processing", this.config.processingPrefix);
  }

  /**
   * Gets IDs by key prefix
   * @param {string} typeName - Type name for logging
   * @param {string} keyPrefix - Key prefix to search
   * @returns {Promise<string[]>} Matching IDs
   */
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
   * Gets IDs of all suppressed items
   * @returns {Promise<string[]>} Suppressed item IDs
   */
  async getSuppressedIds() {
    return await this.getIdsByPrefix("suppressed", this.config.suppressionPrefix);
  }

  /**
   * Extends TTL for a processing item
   * @param {string} itemId - Item identifier
   * @param {number} ttlMs - New TTL in milliseconds
   * @returns {Promise<boolean>} Success indicator
   */
  async extendProcessingTtl(itemId, ttlMs) {
    if (!this.isNonEmptyString(itemId)) {
      throw new Error("Item ID must be a non-empty string");
    }

    if (typeof ttlMs !== "number" || ttlMs <= 0) {
      throw new Error("TTL must be a positive number");
    }

    this.ensureConnected();
    const key = `${this.config.processingPrefix}${itemId}`;

    if (!(await this.exists(key))) {
      logger.logWarning(`Cannot extend TTL for non-existent processing key: ${key}`);
      return false;
    }

    logger.logDebug(`Extending TTL for processing key: ${key} to ${ttlMs}ms`);
    return await this.expire(key, ttlMs);
  }
}

module.exports = AbstractCache;
