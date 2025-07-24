/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * TTL Configuration Module
 *
 * Provides centralized time-to-live (TTL) configuration for various system components.
 * Manages both primary TTL values and derived values calculated from the primaries.
 * Handles environment variable overrides, validation of TTL relationships,
 * and specialized configuration objects for different system components.
 */
const logger = require("../services/logger-service");

const PRIMARY_TTL_VALUES = {
  DELAY_BASE_TIMEOUT_MS: 1_000,
  TASK_PROCESSING_BASE_TTL: 30_000,
  NETWORK_OPERATION_BASE_TIMEOUT: 5_000,
  RETRY_BASE_INTERVAL: 3_000,
};

const PRIMARY_ENV_VARS = {
  DELAY_BASE_TIMEOUT_MS: ["MO_DELAY_BASE_TIMEOUT_MS"],
  TASK_PROCESSING_BASE_TTL: ["MO_TASK_PROCESSING_BASE_TTL_MS"],
  NETWORK_OPERATION_BASE_TIMEOUT: ["MO_NETWORK_OPERATION_BASE_TIMEOUT_MS"],
  RETRY_BASE_INTERVAL: ["MO_RETRY_BASE_INTERVAL_MS"],
};

const DERIVED_ENV_VARS = {
  DISTRIBUTED_LOCK_TTL: ["MO_DISTRIBUTED_LOCK_TTL_MS"],
  DISTRIBUTED_LOCK_REFRESH_INTERVAL: ["MO_DISTRIBUTED_LOCK_REFRESH_INTERVAL_MS"],
  DISTRIBUTED_LOCK_MAX_WAIT_TIME: ["MO_DISTRIBUTED_LOCK_MAX_WAIT_TIME_MS"],
  DISTRIBUTED_LOCK_RETRY_DELAY: ["MO_DISTRIBUTED_LOCK_RETRY_DELAY_MS"],

  TASK_PROCESSING_STATE_TTL: ["MO_TASK_PROCESSING_STATE_TTL_MS"],
  TASK_SUPPRESSION_STATE_TTL: ["MO_TASK_SUPPRESSION_STATE_TTL_MS"],

  BACKPRESSURE_CHECK_INTERVAL: ["MO_BACKPRESSURE_CHECK_INTERVAL_MS"],
  BACKPRESSURE_CACHE_TTL: ["MO_BACKPRESSURE_CACHE_TTL_MS"],

  KAFKA_CONNECTION_TIMEOUT: ["MO_KAFKA_CONNECTION_TIMEOUT_MS"],
  KAFKA_REQUEST_TIMEOUT: ["MO_KAFKA_REQUEST_TIMEOUT_MS"],

  BACKOFF_MIN_DELAY: ["MO_BACKOFF_MIN_DELAY_MS"],
  BACKOFF_MAX_DELAY: ["MO_BACKOFF_MAX_DELAY_MS"],
};

/**
 * TTL Configuration Class
 *
 * Static utility class providing time-to-live configurations for the system.
 * Retrieves values from environment variables when available, otherwise
 * calculates derived values from primary TTLs and validates their relationships.
 */
class TtlConfig {
  /**
   * Gets all TTL values with optional overrides
   *
   * @param {Object} overrides - Optional overrides for specific values
   * @returns {Object} Complete set of TTL values for the system
   */
  static getAllTtlValues(overrides = {}) {
    const base = {
      DELAY_BASE_TIMEOUT_MS: this.getPrimaryTTL("DELAY_BASE_TIMEOUT_MS"),
      TASK_PROCESSING_BASE_TTL: this.getPrimaryTTL("TASK_PROCESSING_BASE_TTL"),
      NETWORK_OPERATION_BASE_TIMEOUT: this.getPrimaryTTL("NETWORK_OPERATION_BASE_TIMEOUT"),
      RETRY_BASE_INTERVAL: this.getPrimaryTTL("RETRY_BASE_INTERVAL"),
    };

    const derived = {
      DISTRIBUTED_LOCK_TTL: this.getDerivedTTL("DISTRIBUTED_LOCK_TTL", () => base.TASK_PROCESSING_BASE_TTL * 2),
      DISTRIBUTED_LOCK_MAX_WAIT_TIME: this.getDerivedTTL(
        "DISTRIBUTED_LOCK_MAX_WAIT_TIME",
        () => base.TASK_PROCESSING_BASE_TTL * 5
      ),
      DISTRIBUTED_LOCK_RETRY_DELAY: this.getDerivedTTL(
        "DISTRIBUTED_LOCK_RETRY_DELAY",
        () => base.TASK_PROCESSING_BASE_TTL
      ),

      TASK_PROCESSING_STATE_TTL: this.getDerivedTTL("TASK_PROCESSING_STATE_TTL", () => base.TASK_PROCESSING_BASE_TTL),
      TASK_SUPPRESSION_STATE_TTL: this.getDerivedTTL(
        "TASK_SUPPRESSION_STATE_TTL",
        () => base.TASK_PROCESSING_BASE_TTL * 3
      ),

      BACKPRESSURE_CHECK_INTERVAL: this.getDerivedTTL(
        "BACKPRESSURE_CHECK_INTERVAL",
        () => base.NETWORK_OPERATION_BASE_TIMEOUT * 3
      ),
      BACKPRESSURE_CACHE_TTL: this.getDerivedTTL(
        "BACKPRESSURE_CACHE_TTL",
        () => base.NETWORK_OPERATION_BASE_TIMEOUT * 3 * 20
      ),

      KAFKA_CONNECTION_TIMEOUT: this.getDerivedTTL(
        "KAFKA_CONNECTION_TIMEOUT",
        () => base.NETWORK_OPERATION_BASE_TIMEOUT / 5
      ),
      KAFKA_REQUEST_TIMEOUT: this.getDerivedTTL("KAFKA_REQUEST_TIMEOUT", () => base.NETWORK_OPERATION_BASE_TIMEOUT * 6),

      BACKOFF_MIN_DELAY: this.getDerivedTTL("BACKOFF_MIN_DELAY", () => base.RETRY_BASE_INTERVAL * 3),
      BACKOFF_MAX_DELAY: this.getDerivedTTL("BACKOFF_MAX_DELAY", () => base.RETRY_BASE_INTERVAL * 6),
    };

    derived.DISTRIBUTED_LOCK_REFRESH_INTERVAL = this.getDerivedTTL("DISTRIBUTED_LOCK_REFRESH_INTERVAL", () =>
      Math.floor(derived.DISTRIBUTED_LOCK_TTL / 5)
    );

    const ttlValues = { ...base, ...derived, ...overrides };
    this.validateTTLRelationships(ttlValues);

    return ttlValues;
  }

  /**
   * Gets configuration specific to distributed locks
   *
   * @returns {Object} Lock configuration with TTL, wait times, and refresh intervals
   */
  static getLockConfig() {
    const ttlValues = this.getAllTtlValues();

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
   * Gets configuration specific to caching operations
   *
   * @returns {Object} Cache configuration with processing and suppression TTL
   */
  static getTopicConfig() {
    const ttlValues = this.getAllTtlValues();

    return {
      processingTtl: ttlValues.TASK_PROCESSING_STATE_TTL,
      suppressionTtl: ttlValues.TASK_SUPPRESSION_STATE_TTL,
    };
  }

  /**
   * Gets configuration specific to backpressure monitoring
   *
   * @returns {Object} Backpressure configuration with intervals and delays
   */
  static getBackpressureConfig() {
    const ttlValues = this.getAllTtlValues();

    return {
      checkInterval: ttlValues.BACKPRESSURE_CHECK_INTERVAL,
      cacheTTL: ttlValues.BACKPRESSURE_CACHE_TTL,
      backoffMinDelay: ttlValues.BACKOFF_MIN_DELAY,
      backoffMaxDelay: ttlValues.BACKOFF_MAX_DELAY,
    };
  }

  /**
   * Gets configuration specific to Kafka operations
   *
   * @returns {Object} Kafka configuration with timeouts
   */
  static getKafkaConfig() {
    const ttlValues = this.getAllTtlValues();

    return {
      connectionTimeout: ttlValues.KAFKA_CONNECTION_TIMEOUT,
      requestTimeout: ttlValues.KAFKA_REQUEST_TIMEOUT,
    };
  }

  /**
   * Gets a primary TTL value with environment variable override support
   *
   * @param {string} key - Primary TTL key
   * @returns {number} TTL value in milliseconds
   * @throws {Error} If key is unknown
   */
  static getPrimaryTTL(key) {
    if (!PRIMARY_TTL_VALUES[key]) {
      throw new Error(`Unknown primary TTL key: ${key}`);
    }

    const envValue = this._getEnvValue(PRIMARY_ENV_VARS[key]);
    if (envValue) {
      const parsed = parseInt(envValue, 10);
      if (isNaN(parsed) || parsed < 0) {
        logger.logWarning(`Invalid environment value for ${key}: ${envValue}. Using default.`);
        return PRIMARY_TTL_VALUES[key];
      }
      return parsed;
    }

    return PRIMARY_TTL_VALUES[key];
  }

  /**
   * Gets a derived TTL value with environment variable override support
   *
   * @param {string} key - Derived TTL key
   * @param {Function} derivationFunction - Function to derive value from primary TTLs
   * @returns {number} TTL value in milliseconds
   */
  static getDerivedTTL(key, derivationFunction) {
    const envValue = this._getEnvValue(DERIVED_ENV_VARS[key]);
    if (envValue) {
      const parsed = parseInt(envValue, 10);
      if (isNaN(parsed) || parsed < 0) {
        logger.logWarning(`Invalid environment value for ${key}: ${envValue}. Using calculated value.`);
      } else {
        return parsed;
      }
    }

    return derivationFunction();
  }

  /**
   * Validates relationships between TTL values to prevent race conditions
   *
   * @param {Object} ttlValues - Object containing all TTL values
   * @returns {boolean} True if all relationships are valid
   * @throws {Error} If TTL value relationships are invalid and could cause race conditions
   */
  static validateTTLRelationships(ttlValues) {
    let isValid = true;

    if (ttlValues.DISTRIBUTED_LOCK_TTL < ttlValues.TASK_PROCESSING_STATE_TTL * 2) {
      logger.logWarning(
        `TTL Validation: Distributed Lock TTL (${ttlValues.DISTRIBUTED_LOCK_TTL}ms) is less than 2x task processing TTL (${ttlValues.TASK_PROCESSING_STATE_TTL}ms), which may cause race conditions`
      );
      isValid = false;
    }

    const lockRefreshInterval = Math.floor(ttlValues.DISTRIBUTED_LOCK_TTL / 5);
    if (Math.abs(lockRefreshInterval - ttlValues.TASK_PROCESSING_STATE_TTL) < 1000) {
      logger.logWarning(
        `TTL Validation: Distributed Lock refresh interval (${lockRefreshInterval}ms) is dangerously close to task processing TTL (${ttlValues.TASK_PROCESSING_STATE_TTL}ms), which may cause race conditions`
      );
      isValid = false;
    }

    if (ttlValues.BACKPRESSURE_CHECK_INTERVAL > ttlValues.KAFKA_REQUEST_TIMEOUT) {
      logger.logWarning(
        `TTL Validation: Backpressure check interval (${ttlValues.BACKPRESSURE_CHECK_INTERVAL}ms) is greater than Kafka request timeout (${ttlValues.KAFKA_REQUEST_TIMEOUT}ms), which may cause race conditions`
      );
      isValid = false;
    }

    return isValid;
  }

  /**
   * Gets the first environment variable value found from a list of keys
   *
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

module.exports = TtlConfig;
