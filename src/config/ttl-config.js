/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the Apache License 2.0 found in the
 * LICENSE file in the root directory of this source tree.
 *
 * TTL Configuration Module
 *
 * Centralized TTL configuration to coordinate timing across components
 * Manages TTL values according to defined relationships to prevent race conditions
 */
const logger = require("../services/logger-service");

/**
 * Primary TTL Values (in milliseconds)
 * These are the base values from which other TTL values are derived
 */
const PRIMARY_TTL_VALUES = {
  // Base time for task processing operations
  TASK_PROCESSING_BASE_TTL: 30_000, // 30 seconds

  // Base timeout for network operations
  NETWORK_OPERATION_BASE_TIMEOUT: 5_000, // 5 seconds

  // Base interval for backoff/retry operations
  RETRY_BASE_INTERVAL: 3_000, // 1 second
};

/**
 * Environment variable names for primary TTL values
 */
const PRIMARY_ENV_VARS = {
  TASK_PROCESSING_BASE_TTL: ["JO_TASK_PROCESSING_BASE_TTL_MS"],
  NETWORK_OPERATION_BASE_TIMEOUT: ["JO_NETWORK_OPERATION_BASE_TIMEOUT_MS"],
  RETRY_BASE_INTERVAL: ["JO_RETRY_BASE_INTERVAL_MS"],
};

/**
 * Environment variable names for derived TTL values
 */
const DERIVED_ENV_VARS = {
  // Lock-related environment variables
  DISTRIBUTED_LOCK_TTL: ["JO_DISTRIBUTED_LOCK_TTL_MS"],
  DISTRIBUTED_LOCK_REFRESH_INTERVAL: [
    "JO_DISTRIBUTED_LOCK_REFRESH_INTERVAL_MS",
  ],
  DISTRIBUTED_LOCK_MAX_WAIT_TIME: ["JO_DISTRIBUTED_LOCK_MAX_WAIT_TIME_MS"],
  DISTRIBUTED_LOCK_RETRY_DELAY: ["JO_DISTRIBUTED_LOCK_RETRY_DELAY_MS"],

  // Cache-related environment variables
  TASK_PROCESSING_STATE_TTL: ["JO_TASK_PROCESSING_STATE_TTL_MS"],
  TASK_FREEZING_STATE_TTL: ["JO_TASK_FREEZING_STATE_TTL_MS"],

  // Backpressure-related environment variables
  BACKPRESSURE_CHECK_INTERVAL: ["JO_BACKPRESSURE_CHECK_INTERVAL_MS"],
  BACKPRESSURE_CACHE_TTL: ["JO_BACKPRESSURE_CACHE_TTL_MS"],

  // Kafka-related environment variables
  KAFKA_CONNECTION_TIMEOUT: ["JO_KAFKA_CONNECTION_TIMEOUT_MS"],
  KAFKA_REQUEST_TIMEOUT: ["JO_KAFKA_REQUEST_TIMEOUT_MS"],

  // Backoff-related environment variables
  BACKOFF_MIN_DELAY: ["JO_BACKOFF_MIN_DELAY_MS"],
  BACKOFF_MAX_DELAY: ["JO_BACKOFF_MAX_DELAY_MS"],
};

class TTLConfig {
  /**
   * Get all TTL values according to the relationship model
   * @param {Object} overrides - Optional overrides for specific values
   * @returns {Object} All TTL values
   */
  static getAllTTLValues(overrides = {}) {
    // Get primary TTL values first
    const base = {
      TASK_PROCESSING_BASE_TTL: this.getPrimaryTTL("TASK_PROCESSING_BASE_TTL"),
      NETWORK_OPERATION_BASE_TIMEOUT: this.getPrimaryTTL(
        "NETWORK_OPERATION_BASE_TIMEOUT",
      ),
      RETRY_BASE_INTERVAL: this.getPrimaryTTL("RETRY_BASE_INTERVAL"),
    };

    // Calculate derived values based on primaries
    const derived = {
      // Lock-related values
      DISTRIBUTED_LOCK_TTL: this.getDerivedTTL(
        "DISTRIBUTED_LOCK_TTL",
        () => base.TASK_PROCESSING_BASE_TTL * 3,
      ),
      DISTRIBUTED_LOCK_MAX_WAIT_TIME: this.getDerivedTTL(
        "DISTRIBUTED_LOCK_MAX_WAIT_TIME",
        () => base.TASK_PROCESSING_BASE_TTL * 3,
      ),
      DISTRIBUTED_LOCK_RETRY_DELAY: this.getDerivedTTL(
        "DISTRIBUTED_LOCK_RETRY_DELAY",
        () => base.TASK_PROCESSING_BASE_TTL,
      ),

      // Cache-related values
      TASK_PROCESSING_STATE_TTL: this.getDerivedTTL(
        "TASK_PROCESSING_STATE_TTL",
        () => base.TASK_PROCESSING_BASE_TTL,
      ),
      TASK_FREEZING_STATE_TTL: this.getDerivedTTL(
        "TASK_FREEZING_STATE_TTL",
        () => base.TASK_PROCESSING_BASE_TTL * 5,
      ),

      // Backpressure-related values
      BACKPRESSURE_CHECK_INTERVAL: this.getDerivedTTL(
        "BACKPRESSURE_CHECK_INTERVAL",
        () => base.NETWORK_OPERATION_BASE_TIMEOUT * 3,
      ),
      BACKPRESSURE_CACHE_TTL: this.getDerivedTTL(
        "BACKPRESSURE_CACHE_TTL",
        () => base.NETWORK_OPERATION_BASE_TIMEOUT * 3 * 20,
      ),

      // Kafka-related values
      KAFKA_CONNECTION_TIMEOUT: this.getDerivedTTL(
        "KAFKA_CONNECTION_TIMEOUT",
        () => base.NETWORK_OPERATION_BASE_TIMEOUT / 5,
      ),
      KAFKA_REQUEST_TIMEOUT: this.getDerivedTTL(
        "KAFKA_REQUEST_TIMEOUT",
        () => base.NETWORK_OPERATION_BASE_TIMEOUT * 6,
      ),

      // Backoff/retry-related values
      BACKOFF_MIN_DELAY: this.getDerivedTTL(
        "BACKOFF_MIN_DELAY",
        () => base.RETRY_BASE_INTERVAL * 3,
      ),
      BACKOFF_MAX_DELAY: this.getDerivedTTL(
        "BACKOFF_MAX_DELAY",
        () => base.RETRY_BASE_INTERVAL * 6,
      ),
    };

    // Calculate additional dependent values
    derived.DISTRIBUTED_LOCK_REFRESH_INTERVAL = this.getDerivedTTL(
      "DISTRIBUTED_LOCK_REFRESH_INTERVAL",
      () => Math.floor(derived.DISTRIBUTED_LOCK_TTL / 5),
    );

    // Apply any explicit overrides
    const ttlValues = { ...base, ...derived, ...overrides };

    // Validate relationships
    this.validateTTLRelationships(ttlValues);

    return ttlValues;
  }

  /**
   * Get configuration specifically for distributed locks
   * @returns {Object} Lock configuration
   */
  static getLockConfig() {
    const ttlValues = this.getAllTTLValues();

    return {
      ttlMs: ttlValues.DISTRIBUTED_LOCK_TTL,
      maxWaitTimeMs: ttlValues.DISTRIBUTED_LOCK_MAX_WAIT_TIME,
      retryDelayMs: ttlValues.DISTRIBUTED_LOCK_RETRY_DELAY,
      refreshInterval: ttlValues.DISTRIBUTED_LOCK_REFRESH_INTERVAL,
      minBackoffMs: ttlValues.BACKOFF_MIN_DELAY,
      maxBackoffMs: ttlValues.BACKOFF_MAX_DELAY,
    };
  }

  /**
   * Get configuration specifically for cache
   * @returns {Object} Cache configuration
   */
  static getCacheConfig() {
    const ttlValues = this.getAllTTLValues();

    return {
      processingTtl: ttlValues.TASK_PROCESSING_STATE_TTL,
      freezingTtl: ttlValues.TASK_FREEZING_STATE_TTL,
    };
  }

  /**
   * Get configuration specifically for backpressure
   * @returns {Object} Backpressure configuration
   */
  static getBackpressureConfig() {
    const ttlValues = this.getAllTTLValues();

    return {
      checkInterval: ttlValues.BACKPRESSURE_CHECK_INTERVAL,
      cacheTTL: ttlValues.BACKPRESSURE_CACHE_TTL,
      backoffMinDelay: ttlValues.BACKOFF_MIN_DELAY,
      backoffMaxDelay: ttlValues.BACKOFF_MAX_DELAY,
    };
  }

  /**
   * Get configuration specifically for Kafka
   * @returns {Object} Kafka configuration
   */
  static getKafkaConfig() {
    const ttlValues = this.getAllTTLValues();

    return {
      connectionTimeout: ttlValues.KAFKA_CONNECTION_TIMEOUT,
      requestTimeout: ttlValues.KAFKA_REQUEST_TIMEOUT,
    };
  }

  /**
   * Get a primary TTL value with environment variable override
   * @param {string} key - Primary TTL key
   * @returns {number} TTL value in milliseconds
   */
  static getPrimaryTTL(key) {
    if (!PRIMARY_TTL_VALUES[key]) {
      throw new Error(`Unknown primary TTL key: ${key}`);
    }

    const envValue = this._getEnvValue(PRIMARY_ENV_VARS[key]);
    if (envValue) {
      const parsed = parseInt(envValue, 10);
      if (isNaN(parsed) || parsed < 0) {
        logger.logWarning(
          `Invalid environment value for ${key}: ${envValue}. Using default.`,
        );
        return PRIMARY_TTL_VALUES[key];
      }
      return parsed;
    }

    return PRIMARY_TTL_VALUES[key];
  }

  /**
   * Get a derived TTL value with environment variable override
   * @param {string} key - Derived TTL key
   * @param {Function} derivationFunction - Function to derive value from primary TTLs
   * @returns {number} TTL value in milliseconds
   */
  static getDerivedTTL(key, derivationFunction) {
    // Check for direct environment variable override
    const envValue = this._getEnvValue(DERIVED_ENV_VARS[key]);
    if (envValue) {
      const parsed = parseInt(envValue, 10);
      if (isNaN(parsed) || parsed < 0) {
        logger.logWarning(
          `Invalid environment value for ${key}: ${envValue}. Using calculated value.`,
        );
      } else {
        return parsed;
      }
    }

    // Calculate derived value
    return derivationFunction();
  }

  /**
   * Validate TTL relationships to catch potential race conditions
   * @param {Object} ttlValues - Object with TTL values to validate
   * @returns {boolean} True if valid, false if potential issues exist
   */
  static validateTTLRelationships(ttlValues) {
    let isValid = true;

    // Check lock TTL vs processing TTL
    if (
      ttlValues.DISTRIBUTED_LOCK_TTL <
      ttlValues.TASK_PROCESSING_STATE_TTL * 2
    ) {
      logger.logWarning(
        `TTL Validation: Distributed Lock TTL (${ttlValues.DISTRIBUTED_LOCK_TTL}ms) is less than 2x task processing TTL (${ttlValues.TASK_PROCESSING_STATE_TTL}ms), which may cause race conditions`,
      );
      isValid = false;
    }

    // Check lock refresh interval vs processing TTL
    const lockRefreshInterval = Math.floor(ttlValues.DISTRIBUTED_LOCK_TTL / 5);
    if (
      Math.abs(lockRefreshInterval - ttlValues.TASK_PROCESSING_STATE_TTL) < 1000
    ) {
      logger.logWarning(
        `TTL Validation: Distributed Lock refresh interval (${lockRefreshInterval}ms) is dangerously close to task processing TTL (${ttlValues.TASK_PROCESSING_STATE_TTL}ms), which may cause race conditions`,
      );
      isValid = false;
    }

    // Check backpressure check interval vs kafka request timeout
    if (
      ttlValues.BACKPRESSURE_CHECK_INTERVAL > ttlValues.KAFKA_REQUEST_TIMEOUT
    ) {
      logger.logWarning(
        `TTL Validation: Backpressure check interval (${ttlValues.BACKPRESSURE_CHECK_INTERVAL}ms) is greater than Kafka request timeout (${ttlValues.KAFKA_REQUEST_TIMEOUT}ms), which may cause race conditions`,
      );
      isValid = false;
    }

    return isValid;
  }

  /**
   * Get environment variable value by checking all provided key names
   * @param {string[]} keys - List of environment variable names to check
   * @returns {string|null} First found environment variable value or null
   */
  static _getEnvValue(keys) {
    for (const key of keys) {
      const value = process.env[key];
      if (value !== undefined) {
        return value;
      }
    }
    return null;
  }
}

module.exports = TTLConfig;
