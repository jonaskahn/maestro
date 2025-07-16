/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Abstract Backpressure Monitor for intelligent rate limiting and consumer lag monitoring
 *
 * Implements adaptive backoff strategies for message brokers to prevent system overload
 * while maintaining optimal throughput across Kafka, RabbitMQ, BullMQ implementations.
 * This class must be extended by specific broker implementations.
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

/**
 * Abstract Backpressure Monitor Service
 *
 * Implements adaptive backoff strategies for message brokers to prevent system overload
 * while maintaining optimal throughput. Monitors consumer lag and system resources
 * to recommend processing delays and pause conditions.
 */
class AbstractMonitorService {
  /**
   * Creates a backpressure monitor with configuration validation and initialization
   *
   * @param {Object} config - Monitor configuration object
   * @param {number} [config.lagThreshold] - Maximum acceptable message lag
   * @param {boolean} [config.enabledResourceLag] - Whether to monitor system resources
   * @param {number} [config.checkInterval] - Interval for checking metrics in ms
   * @param {number} [config.rateLimitThreshold] - Threshold for rate limiting
   * @param {number} [config.cacheTTL] - TTL for cached metrics in ms
   * @param {number} [config.initialDelay] - Initial delay for backoff in ms
   * @param {number} [config.maxDelay] - Maximum delay for backoff in ms
   * @param {number} [config.exponentialFactor] - Factor for exponential backoff
   */
  constructor(config) {
    if (this.constructor === AbstractMonitorService) {
      throw new Error("AbstractMonitorService cannot be instantiated directly");
    }

    if (!config || typeof config !== "object") {
      throw new Error("Backpressure monitor configuration must be an object");
    }

    this.config = {
      lagThreshold:
        config.lagThreshold || this.getEnvironmentValueOrDefault(ENV_KEYS.MAX_LAG, DEFAULT_VALUES.MAX_LAG_THRESHOLD),
      enabledResourceLag:
        config.enabledResourceLag ||
        this.getEnvironmentValueOrDefault(ENV_KEYS.ENABLED_RESOURCE_LAG, DEFAULT_VALUES.ENABLED_RESOURCE_LAG),
      checkInterval: config.checkInterval || BackpressureMonitorConfig.checkInterval,
      rateLimitThreshold: config.rateLimitThreshold || DEFAULT_VALUES.RATE_LIMIT_THRESHOLD,
      cacheTTL: config.cacheTTL || BackpressureMonitorConfig.cacheTTL,
      initialDelay: config.initialDelay || TtlConfig.getAllTtlValues().BACKOFF_MIN_DELAY,
      maxDelay: config.maxDelay || TtlConfig.getAllTtlValues().BACKOFF_MAX_DELAY,
      exponentialFactor: config.exponentialFactor || DEFAULT_VALUES.EXPONENTIAL_FACTOR,
    };
    this._topic = config.topic;
    this.isMonitoring = MONITORING_STATES.DISABLED;
    this.monitoringInterval = null;

    logger.logDebug(`Backpressure monitor configured for broker ${this.getBrokerType()} on topic [ ${this._topic} ]`);
    logger.logDebug(
      `Backpressure monitor configuration for broker ${this.getBrokerType()} on topic [ ${this._topic} ]`,
      {
        lagThreshold: this.config.lagThreshold,
        checkInterval: this.config.lagMonitorInterval,
        rateLimitThreshold: this.config.rateLimitThreshold,
      }
    );
  }

  // --- Public API ---

  /**
   * Returns the broker type identifier
   * @returns {string} Broker type ('kafka', 'rabbitmq', 'bullmq')
   * @throws {Error} When method is not implemented by subclass
   */
  getBrokerType() {
    throw new Error("getBrokerType method must be implemented by subclass");
  }

  /**
   * Connects and starts the monitoring service
   * @returns {Promise<void>}
   */
  async connect() {
    await this.startMonitoring();
  }

  /**
   * Disconnects and stops the monitoring service
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
      logger.logDebug(`Backpressure monitoring started for ${this.getBrokerType()}`);
      return Promise.resolve();
    } catch (error) {
      logger.logError(`Failed to start backpressure monitoring for ${this.getBrokerType()}`, error);
      throw error;
    }
  }

  /**
   * Performs a single monitoring check cycle
   * @returns {Promise<void>}
   */
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

  /**
   * Collects current metrics from all sources
   * @returns {Promise<Object>} Combined metrics
   */
  async collectCurrentMetrics() {
    const lagMetrics = await this.collectLagMetrics();
    const resourceMetrics = this.collectResourceMetrics();

    return {
      ...lagMetrics,
      ...resourceMetrics,
      timestamp: Date.now(),
      brokerType: this.getBrokerType(),
    };
  }

  /**
   * Collects lag metrics from message broker
   * @returns {Promise<Object>} Lag metrics
   */
  async collectLagMetrics() {
    try {
      return await this.getConsumerLag();
    } catch (error) {
      logger.logWarning("Failed to collect consumer lag metrics", error);
      return {
        totalLag: 0,
        maxPartitionLag: 0,
        avgLag: 0,
        lagThreshold: this.config.lagThreshold,
      };
    }
  }

  /**
   * Gets consumer lag metrics from the message broker
   * @returns {Promise<Object>} Lag metrics object with totalLag, maxPartitionLag, avgLag, and lagThreshold
   * @throws {Error} When method is not implemented by subclass
   */
  async getConsumerLag() {
    throw new Error("getConsumerLag method must be implemented by subclass");
  }

  /**
   * Collects resource metrics from system
   * @returns {Promise<Object>} Resource metrics
   */
  collectResourceMetrics() {
    try {
      return this.getResourceMetrics();
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
   * Gets system resource metrics like CPU and memory usage
   * @returns {Promise<Object>} Resource metrics with cpuUsage and memoryUsage percentages
   * @throws {Error} When method is not implemented by subclass
   */
  getResourceMetrics() {
    throw new Error("getResourceMetrics method must be implemented by subclass");
  }

  /**
   * Calculates the current backpressure level based on metrics
   * @param {Object} metrics - Collected metrics object
   * @param {number} metrics.totalLag - Total message lag across all partitions
   * @param {number} metrics.cpuUsage - Current CPU usage percentage
   * @param {number} metrics.memoryUsage - Current memory usage percentage
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
   * @param {Object} metrics - Metrics object with lag information
   * @param {number} metrics.totalLag - Total consumer lag across all partitions
   * @param {number} metrics.lagThreshold - Maximum acceptable lag threshold
   * @returns {string} Lag-based backpressure level from BACKPRESSURE_LEVELS enum
   */
  getLagBackpressureLevel(metrics) {
    if (!metrics || !metrics.totalLag) {
      return BACKPRESSURE_LEVELS.NONE;
    }
    const lagRatio = metrics.totalLag / this.config.lagThreshold;

    if (lagRatio > 1.0) return BACKPRESSURE_LEVELS.CRITICAL;
    if (lagRatio > 0.8) return BACKPRESSURE_LEVELS.HIGH;
    if (lagRatio > 0.6) return BACKPRESSURE_LEVELS.MEDIUM;
    if (lagRatio > 0.3) return BACKPRESSURE_LEVELS.LOW;

    return BACKPRESSURE_LEVELS.NONE;
  }

  /**
   * Determines backpressure level based on system resource usage
   * @param {Object} metrics - Metrics object with resource information
   * @param {number} metrics.cpuUsage - Current CPU usage percentage (0-100)
   * @param {number} metrics.memoryUsage - Current memory usage percentage (0-100)
   * @returns {string} Resource-based backpressure level from BACKPRESSURE_LEVELS enum
   */
  getResourceBackpressureLevel(metrics) {
    if (!this.config.enabledResourceLag || !metrics) {
      return BACKPRESSURE_LEVELS.NONE;
    }
    const maxResourceUsage = Math.max(metrics.memoryUsage || 0, metrics.cpuUsage || 0);

    if (maxResourceUsage > 90) return BACKPRESSURE_LEVELS.CRITICAL;
    if (maxResourceUsage > 80) return BACKPRESSURE_LEVELS.HIGH;
    if (maxResourceUsage > 70) return BACKPRESSURE_LEVELS.MEDIUM;
    if (maxResourceUsage > 50) return BACKPRESSURE_LEVELS.LOW;

    return BACKPRESSURE_LEVELS.NONE;
  }

  /**
   * Determines the highest backpressure level between two levels
   * @param {string} level1 - First backpressure level from BACKPRESSURE_LEVELS enum
   * @param {string} level2 - Second backpressure level from BACKPRESSURE_LEVELS enum
   * @returns {string} Highest backpressure level from BACKPRESSURE_LEVELS enum
   */
  getHighestBackpressureLevel(level1, level2) {
    const levelOrder = [
      BACKPRESSURE_LEVELS.NONE,
      BACKPRESSURE_LEVELS.LOW,
      BACKPRESSURE_LEVELS.MEDIUM,
      BACKPRESSURE_LEVELS.HIGH,
      BACKPRESSURE_LEVELS.CRITICAL,
    ];

    const index1 = levelOrder.indexOf(level1);
    const index2 = levelOrder.indexOf(level2);

    return levelOrder[Math.max(index1, index2)];
  }

  /**
   * Stops monitoring for backpressure
   * @returns {Promise<void>}
   * @throws {Error} When monitoring cleanup fails
   */
  async stopMonitoring() {
    if (this.isMonitoring !== MONITORING_STATES.ENABLED) {
      logger.logWarning(`Backpressure monitor for ${this.getBrokerType()} is not active`);
      return await Promise.resolve();
    }

    try {
      if (this.monitoringInterval) {
        clearInterval(this.monitoringInterval);
        this.monitoringInterval = null;
      }

      this.lastMetrics = null;
      this.lastMetricsTime = null;
      this.isMonitoring = MONITORING_STATES.DISABLED;

      logger.logDebug(`Backpressure monitoring stopped for ${this.getBrokerType()}`);
    } catch (error) {
      logger.logError(`Error stopping backpressure monitoring for ${this.getBrokerType()}`, error);
      throw error;
    }
  }

  /**
   * Gets current backpressure monitoring status
   * @returns {Promise<Object>} Status object with monitoring state, metrics, and configuration
   */
  async getBackpressureStatus() {
    try {
      const metrics = await this.collectCurrentMetrics();
      const backpressureLevel = this.calculateBackpressureLevel(metrics);
      const shouldPause =
        backpressureLevel === BACKPRESSURE_LEVELS.CRITICAL || backpressureLevel === BACKPRESSURE_LEVELS.HIGH;

      const delayMultipliers = {
        [BACKPRESSURE_LEVELS.NONE]: 0,
        [BACKPRESSURE_LEVELS.LOW]: 1,
        [BACKPRESSURE_LEVELS.MEDIUM]: 2,
        [BACKPRESSURE_LEVELS.HIGH]: 4,
        [BACKPRESSURE_LEVELS.CRITICAL]: 8,
      };

      const multiplier = delayMultipliers[backpressureLevel] || 0;
      const delay = Math.min(this.config.initialDelay * multiplier, this.config.maxDelay);

      return {
        backpressureLevel,
        shouldPause,
        recommendedDelay: delay,
        metrics: {
          lag: {
            total: metrics.totalLag,
            max: metrics.maxPartitionLag,
            average: metrics.avgLag,
            threshold: this.config.lagThreshold,
          },
          resources: {
            memory: metrics.memoryUsage,
            cpu: metrics.cpuUsage,
            network: metrics.networkLatency,
          },
        },
        timestamp: metrics.timestamp,
        brokerType: metrics.brokerType,
      };
    } catch (error) {
      logger.logError("Error getting backpressure status", error);
      return {
        backpressureLevel: BACKPRESSURE_LEVELS.NONE,
        shouldPause: false,
        recommendedDelay: 0,
        error: error.message,
        timestamp: Date.now(),
        brokerType: this.getBrokerType(),
      };
    }
  }

  /**
   * Checks if system should pause processing
   * @returns {Promise<boolean>} True if should pause
   * @throws {Error} When status check fails
   */
  async shouldPauseProcessing() {
    const status = await this.getBackpressureStatus();
    return status.shouldPause === true;
  }

  /**
   * Gets the recommended delay before next operation
   * @returns {Promise<number>} Delay in milliseconds
   * @throws {Error} When status check fails
   */
  async getRecommendedDelay() {
    const status = await this.getBackpressureStatus();
    return status.recommendedDelay || 0;
  }

  /**
   * Gets environment value with fallback to default
   * @param {Array<string>} keys - Environment variable keys to check
   * @param {*} defaultValue - Default value if no environment variable found
   * @returns {*} Environment value or default
   */
  getEnvironmentValueOrDefault(keys, defaultValue) {
    for (const key of keys) {
      const value = process.env[key];
      if (value !== undefined) {
        return parseInt(value);
      }
    }
    return defaultValue;
  }
}

module.exports = AbstractMonitorService;
