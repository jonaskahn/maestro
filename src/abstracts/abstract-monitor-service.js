/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Abstract Backpressure Monitor Service
 *
 * Provides intelligent rate limiting and consumer lag monitoring capabilities across
 * various message broker systems. Implements adaptive backoff strategies to prevent
 * system overload while maintaining optimal throughput. This class must be extended
 * by specific broker implementations.
 */
const logger = require("../services/logger-service");
const TtlConfig = require("../config/ttl-config");

const DEFAULT_VALUES = {
  MAX_LAG_THRESHOLD: 100,
  ENABLED_RESOURCE_LAG: false,
  RATE_LIMIT_THRESHOLD: 100,
  EXPONENTIAL_FACTOR: 2,
};

const BackpressureMonitorConfig = TtlConfig.getBackpressureConfig();

const MONITORING_STATES = {
  ENABLED: true,
  DISABLED: false,
  MONITORING_ACTIVE: "active",
  MONITORING_PAUSED: "paused",
};

const BACKPRESSURE_LEVELS = {
  NONE: "NONE",
  LOW: "LOW",
  MEDIUM: "MEDIUM",
  HIGH: "HIGH",
  CRITICAL: "CRITICAL",
};

const ENV_KEYS = {
  MAX_LAG: ["MO_BACKPRESSURE_MAX_LAG"],
  ENABLED_RESOURCE_LAG: ["MO_BACKPRESSURE_ENABLED_RESOURCE_LAG"],
};

class AbstractMonitorService {
  /**
   * Create backpressure monitor with configuration validation and initialization
   * @param {Object} config Monitor configuration with lag thresholds, intervals, and caching options
   */
  constructor(config) {
    if (this.constructor === AbstractMonitorService) {
      throw new Error("AbstractMonitorService cannot be instantiated directly");
    }

    if (!config || typeof config !== "object") {
      throw new Error("Backpressure monitor configuration must be an object");
    }

    this.config = {
      maxLag: config.maxLag || this.getEnvironmentValueOrDefault(ENV_KEYS.MAX_LAG, DEFAULT_VALUES.MAX_LAG_THRESHOLD),
      enabledResourceLag:
        config.enabledResourceLag ||
        this.getEnvironmentValueOrDefault(ENV_KEYS.ENABLED_RESOURCE_LAG, DEFAULT_VALUES.ENABLED_RESOURCE_LAG),
      checkInterval: config.checkInterval || BackpressureMonitorConfig.checkInterval,
      rateLimitThreshold: config.rateLimitThreshold || DEFAULT_VALUES.RATE_LIMIT_THRESHOLD,
      cacheTTL: config.cacheTTL || BackpressureMonitorConfig.cacheTTL,
      initialDelay: config.initialDelay || TtlConfig.getAllTTLValues().BACKOFF_MIN_DELAY,
      maxDelay: config.maxDelay || TtlConfig.getAllTTLValues().BACKOFF_MAX_DELAY,
      exponentialFactor: config.exponentialFactor || DEFAULT_VALUES.EXPONENTIAL_FACTOR,
    };

    this.isMonitoring = MONITORING_STATES.DISABLED;
    this.monitoringInterval = null;

    logger.logInfo(`Backpressure monitor configured for ${this.getBrokerType()}`);
    logger.logDebug("Backpressure monitor configuration", {
      maxLag: this.config.maxLag,
      checkInterval: this.config.checkInterval,
      rateLimitThreshold: this.config.rateLimitThreshold,
    });
  }

  getEnvironmentValueOrDefault(keys, defaultValue) {
    for (const key of keys) {
      const value = process.env[key];
      if (value !== undefined) {
        return parseInt(value);
      }
    }
    return defaultValue;
  }

  /**
   * Returns the broker type identifier
   * @returns {string} Broker type ('kafka', 'rabbitmq', 'bullmq')
   * @throws {Error} When method is not implemented by subclass
   */
  getBrokerType() {
    throw new Error("getBrokerType method must be implemented by subclass");
  }

  /**
   * Establishes connection and starts monitoring
   * @returns {Promise<void>}
   */
  async connect() {
    await this.startMonitoring();
  }

  /**
   * Stops monitoring and disconnects
   * @returns {Promise<void>}
   */
  async disconnect() {
    await this.stopMonitoring();
  }

  /**
   * Starts monitoring for backpressure conditions
   * @returns {Promise<void>}
   * @throws {Error} When monitoring initialization fails
   */
  startMonitoring() {
    if (this.isMonitoring === MONITORING_STATES.ENABLED) {
      logger.logWarning(`Backpressure monitor for ${this.getBrokerType()} is already active`);
      return Promise.resolve();
    }

    try {
      this.monitoringInterval = setInterval(async () => {
        await this.performMonitoringCheck();
      }, this.config.checkInterval);

      this.isMonitoring = MONITORING_STATES.ENABLED;
      logger.logInfo(`Backpressure monitoring started for ${this.getBrokerType()}`);
      return Promise.resolve();
    } catch (error) {
      logger.logError(`Failed to start backpressure monitoring for ${this.getBrokerType()}`, error);
      throw error;
    }
  }

  async performMonitoringCheck() {
    try {
      const metrics = await this.collectCurrentMetrics();
      this.lastMetrics = metrics;
      this.lastMetricsTime = Date.now();

      const backpressureLevel = this.calculateBackpressureLevel(metrics);

      if (backpressureLevel !== BACKPRESSURE_LEVELS.NONE) {
        logger.logWarning(
          `Backpressure detected (${backpressureLevel}): lag=${metrics.totalLag}, ` +
            `memory=${metrics.memoryUsage}%, cpu=${metrics.cpuUsage}%`
        );
      }
    } catch (error) {
      logger.logWarning("Error during backpressure monitoring check", error);
    }
  }

  async collectCurrentMetrics() {
    const lagMetrics = await this.collectLagMetrics();
    const resourceMetrics = await this.collectResourceMetrics();

    return {
      ...lagMetrics,
      ...resourceMetrics,
      timestamp: Date.now(),
      brokerType: this.getBrokerType(),
    };
  }

  async collectLagMetrics() {
    try {
      return await this.getConsumerLag();
    } catch (error) {
      logger.logWarning("Failed to collect consumer lag metrics", error);
      return {
        totalLag: 0,
        maxPartitionLag: 0,
        avgLag: 0,
        lagThreshold: this.config.maxLag,
      };
    }
  }

  async collectResourceMetrics() {
    try {
      return await this.getResourceMetrics();
    } catch (error) {
      logger.logWarning("Failed to collect resource metrics", error);
      return {
        memoryUsage: 0,
        cpuUsage: 0,
        networkLatency: 0,
      };
    }
  }

  /**
   * Gets consumer lag metrics from the message broker
   * @abstract
   * @returns {Promise<Object>} Lag metrics object with totalLag, maxPartitionLag, avgLag, and lagThreshold
   * @throws {Error} When method is not implemented by subclass
   */
  async getConsumerLag() {
    throw new Error("getConsumerLag method must be implemented by subclass");
  }

  /**
   * Gets system resource metrics for backpressure calculation
   * @abstract
   * @returns {Promise<Object>} Resource metrics including CPU, memory, and network metrics
   * @throws {Error} When method is not implemented by subclass
   */
  async getResourceMetrics() {
    throw new Error("getResourceMetrics method must be implemented by subclass");
  }

  /**
   * Calculates the current backpressure level based on metrics
   * @param {Object} metrics Collected metrics object
   * @param {number} metrics.totalLag Total message lag across all partitions
   * @param {number} metrics.cpuUsage Current CPU usage percentage
   * @param {number} metrics.memoryUsage Current memory usage percentage
   * @returns {string} Backpressure level from BACKPRESSURE_LEVELS enum
   */
  calculateBackpressureLevel(metrics) {
    const lagLevel = this.getLagBackpressureLevel(metrics);
    const resourceLevel = this.config.enabledResourceLag
      ? this.getResourceBackpressureLevel(metrics)
      : BACKPRESSURE_LEVELS.NONE;

    return this.getHighestBackpressureLevel(lagLevel, resourceLevel);
  }

  /**
   * Determines backpressure level based on consumer lag
   * @param {Object} metrics Metrics object with lag information
   * @param {number} metrics.totalLag Total consumer lag across all partitions
   * @param {number} metrics.lagThreshold Maximum acceptable lag threshold
   * @returns {string} Lag-based backpressure level from BACKPRESSURE_LEVELS enum
   */
  getLagBackpressureLevel(metrics) {
    if (!metrics || !metrics.totalLag) {
      return BACKPRESSURE_LEVELS.NONE;
    }
    const lagRatio = metrics.totalLag / this.config.maxLag;

    if (lagRatio >= 1.0) {
      return BACKPRESSURE_LEVELS.CRITICAL;
    } else if (lagRatio >= 0.75) {
      return BACKPRESSURE_LEVELS.HIGH;
    } else if (lagRatio >= 0.5) {
      return BACKPRESSURE_LEVELS.MEDIUM;
    } else if (lagRatio >= 0.25) {
      return BACKPRESSURE_LEVELS.LOW;
    }
    return BACKPRESSURE_LEVELS.NONE;
  }

  /**
   * Determines backpressure level based on system resources
   * @param {Object} metrics Metrics object with resource utilization information
   * @param {number} metrics.cpuUsage Current CPU usage percentage (0-100)
   * @param {number} metrics.memoryUsage Current memory usage percentage (0-100)
   * @returns {string} Resource-based backpressure level from BACKPRESSURE_LEVELS enum
   */
  getResourceBackpressureLevel(metrics) {
    if (!metrics) {
      return BACKPRESSURE_LEVELS.NONE;
    }

    const cpuThreshold = Math.max(metrics.cpuUsage || 0, metrics.memoryUsage || 0);

    if (cpuThreshold >= 95) {
      return BACKPRESSURE_LEVELS.CRITICAL;
    } else if (cpuThreshold >= 85) {
      return BACKPRESSURE_LEVELS.HIGH;
    } else if (cpuThreshold >= 75) {
      return BACKPRESSURE_LEVELS.MEDIUM;
    } else if (cpuThreshold >= 60) {
      return BACKPRESSURE_LEVELS.LOW;
    }
    return BACKPRESSURE_LEVELS.NONE;
  }

  /**
   * Determines highest backpressure level between two levels
   * @param {string} level1 First backpressure level
   * @param {string} level2 Second backpressure level
   * @returns {string} Highest backpressure level from BACKPRESSURE_LEVELS enum
   */
  getHighestBackpressureLevel(level1, level2) {
    const levels = [
      BACKPRESSURE_LEVELS.NONE,
      BACKPRESSURE_LEVELS.LOW,
      BACKPRESSURE_LEVELS.MEDIUM,
      BACKPRESSURE_LEVELS.HIGH,
      BACKPRESSURE_LEVELS.CRITICAL,
    ];

    const level1Index = levels.indexOf(level1);
    const level2Index = levels.indexOf(level2);

    if (level1Index === -1 || level2Index === -1) {
      logger.logWarning(`Invalid backpressure level: ${level1} or ${level2}`);
      return BACKPRESSURE_LEVELS.NONE;
    }

    return level1Index >= level2Index ? level1 : level2;
  }

  /**
   * Stops the backpressure monitoring
   * @returns {Promise<void>}
   */
  async stopMonitoring() {
    if (this.isMonitoring !== MONITORING_STATES.ENABLED) {
      logger.logWarning(`Backpressure monitor for ${this.getBrokerType()} is not active`);
      return Promise.resolve();
    }

    try {
      if (this.monitoringInterval) {
        clearInterval(this.monitoringInterval);
        this.monitoringInterval = null;
      }

      this.isMonitoring = MONITORING_STATES.DISABLED;
      logger.logInfo(`Backpressure monitoring stopped for ${this.getBrokerType()}`);
      return Promise.resolve();
    } catch (error) {
      logger.logError(`Failed to stop backpressure monitoring for ${this.getBrokerType()}`, error);
      throw error;
    }
  }

  /**
   * Gets the current backpressure status and metrics
   * @returns {Promise<Object>} Backpressure status with metrics, level, and recommendations
   */
  async getBackpressureStatus() {
    try {
      const metrics = this.lastMetrics || (await this.collectCurrentMetrics());
      const backpressureLevel = this.calculateBackpressureLevel(metrics);
      const shouldPause = await this.shouldPauseProcessing();

      let recommendedDelay = 0;
      if (shouldPause) {
        recommendedDelay = await this.getRecommendedDelay();
      }

      const statusSummary = {
        backpressureLevel,
        shouldPauseProcessing: shouldPause,
        recommendedDelay,
        timestamp: Date.now(),
        metrics,
      };

      if (backpressureLevel !== BACKPRESSURE_LEVELS.NONE) {
        logger.logInfo(`Current backpressure level: ${backpressureLevel}, pause recommended: ${shouldPause}`);
      }

      return statusSummary;
    } catch (error) {
      logger.logError("Failed to get backpressure status", error);
      return {
        backpressureLevel: BACKPRESSURE_LEVELS.NONE,
        shouldPauseProcessing: false,
        recommendedDelay: 0,
        timestamp: Date.now(),
        metrics: {
          totalLag: 0,
          cpuUsage: 0,
          memoryUsage: 0,
        },
        error: error.message,
      };
    }
  }

  /**
   * Determines if processing should be paused due to backpressure
   * @returns {Promise<boolean>} True if processing should be paused
   */
  async shouldPauseProcessing() {
    const metrics = this.lastMetrics || (await this.collectCurrentMetrics());
    const backpressureLevel = this.calculateBackpressureLevel(metrics);
    return backpressureLevel === BACKPRESSURE_LEVELS.HIGH || backpressureLevel === BACKPRESSURE_LEVELS.CRITICAL;
  }

  /**
   * Get recommended delay duration based on current backpressure level
   * @returns {Promise<number>} Recommended delay in milliseconds
   */
  async getRecommendedDelay() {
    const metrics = this.lastMetrics || (await this.collectCurrentMetrics());
    const backpressureLevel = this.calculateBackpressureLevel(metrics);

    switch (backpressureLevel) {
      case BACKPRESSURE_LEVELS.CRITICAL:
        return Math.min(this.config.initialDelay * Math.pow(this.config.exponentialFactor, 4), this.config.maxDelay);
      case BACKPRESSURE_LEVELS.HIGH:
        return Math.min(this.config.initialDelay * Math.pow(this.config.exponentialFactor, 3), this.config.maxDelay);
      case BACKPRESSURE_LEVELS.MEDIUM:
        return Math.min(this.config.initialDelay * Math.pow(this.config.exponentialFactor, 2), this.config.maxDelay);
      case BACKPRESSURE_LEVELS.LOW:
        return Math.min(this.config.initialDelay * this.config.exponentialFactor, this.config.maxDelay);
      default:
        return this.config.initialDelay;
    }
  }

  /**
   * Determines if production should be throttled due to backpressure
   * @returns {Promise<Object>} Throttling result object with decision and reason
   */
  async shouldThrottleProduction() {
    const metrics = this.lastMetrics || (await this.collectCurrentMetrics());
    const backpressureLevel = this.calculateBackpressureLevel(metrics);

    const shouldThrottle =
      backpressureLevel === BACKPRESSURE_LEVELS.HIGH || backpressureLevel === BACKPRESSURE_LEVELS.CRITICAL;

    return {
      shouldThrottle,
      reason: shouldThrottle ? `Backpressure level ${backpressureLevel} exceeds threshold` : null,
      backpressureLevel,
      lagDetails: {
        current: metrics.totalLag,
        threshold: this.config.maxLag,
        ratio: metrics.totalLag / this.config.maxLag,
      },
    };
  }
}

module.exports = AbstractMonitorService;
