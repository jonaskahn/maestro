/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the Apache License 2.0 found in the
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
  MAX_LAG: ["JO_BACKPRESSURE_MAX_LAG"],
  ENABLED_RESOURCE_LAG: ["JO_BACKPRESSURE_ENABLED_RESOURCE_LAG"],
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
      maxLag:
        config.maxLag ||
        this.getEnvironmentValueOrDefault(
          ENV_KEYS.MAX_LAG,
          DEFAULT_VALUES.MAX_LAG_THRESHOLD,
        ),
      enabledResourceLag:
        config.enabledResourceLag ||
        this.getEnvironmentValueOrDefault(
          ENV_KEYS.ENABLED_RESOURCE_LAG,
          DEFAULT_VALUES.ENABLED_RESOURCE_LAG,
        ),
      checkInterval:
        config.checkInterval || BackpressureMonitorConfig.checkInterval,
      rateLimitThreshold:
        config.rateLimitThreshold || DEFAULT_VALUES.RATE_LIMIT_THRESHOLD,
      cacheTTL: config.cacheTTL || BackpressureMonitorConfig.cacheTTL,
      initialDelay:
        config.initialDelay || TtlConfig.getAllTTLValues().BACKOFF_MIN_DELAY,
      maxDelay:
        config.maxDelay || TtlConfig.getAllTTLValues().BACKOFF_MAX_DELAY,
      exponentialFactor:
        config.exponentialFactor || DEFAULT_VALUES.EXPONENTIAL_FACTOR,
    };

    this.isMonitoring = MONITORING_STATES.DISABLED;
    this.monitoringInterval = null;

    logger.logInfo(
      `ℹ️ Backpressure monitor configured for ${this.getBrokerType()}`,
    );
    logger.logDebug("ℹ️ Backpressure monitor configuration", {
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

  async connect() {
    await this.startMonitoring();
  }

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
      logger.logWarning(
        `Backpressure monitor for ${this.getBrokerType()} is already active`,
      );
      return Promise.resolve();
    }

    try {
      this.monitoringInterval = setInterval(async () => {
        await this.performMonitoringCheck();
      }, this.config.checkInterval);

      this.isMonitoring = MONITORING_STATES.ENABLED;
      logger.logInfo(
        `⚡ Backpressure monitoring started for ${this.getBrokerType()}`,
      );
      return Promise.resolve();
    } catch (error) {
      logger.logError(
        `Failed to start backpressure monitoring for ${this.getBrokerType()}`,
        error,
      );
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
          `⚡ Backpressure detected (${backpressureLevel}): lag=${metrics.totalLag}, ` +
            `memory=${metrics.memoryUsage}%, cpu=${metrics.cpuUsage}%`,
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
   * Get consumer lag metrics (must be implemented by subclasses)
   * @returns {Promise<Object>} Lag metrics
   */
  async getConsumerLag() {
    throw new Error("getConsumerLag method must be implemented by subclass");
  }

  /**
   * Get system resource metrics (must be implemented by subclasses)
   * @returns {Promise<Object>} Resource metrics
   */
  async getResourceMetrics() {
    throw new Error(
      "getResourceMetrics method must be implemented by subclass",
    );
  }

  calculateBackpressureLevel(metrics) {
    const lagLevel = this.getLagBackpressureLevel(metrics);
    const resourceLevel = this.config.enabledResourceLag
      ? this.getResourceBackpressureLevel(metrics)
      : BACKPRESSURE_LEVELS.NONE;

    return this.getHighestBackpressureLevel(lagLevel, resourceLevel);
  }

  getLagBackpressureLevel(metrics) {
    const lagRatio = metrics.totalLag / this.config.maxLag;

    if (lagRatio > 1.0) return BACKPRESSURE_LEVELS.CRITICAL;
    if (lagRatio > 0.8) return BACKPRESSURE_LEVELS.HIGH;
    if (lagRatio > 0.6) return BACKPRESSURE_LEVELS.MEDIUM;
    if (lagRatio > 0.3) return BACKPRESSURE_LEVELS.LOW;

    return BACKPRESSURE_LEVELS.NONE;
  }

  getResourceBackpressureLevel(metrics) {
    const maxResourceUsage = Math.max(
      metrics.memoryUsage || 0,
      metrics.cpuUsage || 0,
    );

    if (maxResourceUsage > 90) return BACKPRESSURE_LEVELS.CRITICAL;
    if (maxResourceUsage > 80) return BACKPRESSURE_LEVELS.HIGH;
    if (maxResourceUsage > 70) return BACKPRESSURE_LEVELS.MEDIUM;
    if (maxResourceUsage > 50) return BACKPRESSURE_LEVELS.LOW;

    return BACKPRESSURE_LEVELS.NONE;
  }

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
      logger.logWarning(
        `Backpressure monitor for ${this.getBrokerType()} is not active`,
      );
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

      logger.logInfo(
        `⚡ Backpressure monitoring stopped for ${this.getBrokerType()}`,
      );
    } catch (error) {
      logger.logError(
        `Error stopping backpressure monitoring for ${this.getBrokerType()}`,
        error,
      );
      throw error;
    }
  }

  /**
   * Returns the current backpressure status and metrics
   * @returns {Promise<Object>} Status object with backpressure level and metrics
   * @throws {Error} When status collection fails
   */
  async getBackpressureStatus() {
    try {
      const metrics = await this.collectCurrentMetrics();
      const backpressureLevel = this.calculateBackpressureLevel(metrics);
      const shouldPause =
        backpressureLevel === BACKPRESSURE_LEVELS.CRITICAL ||
        backpressureLevel === BACKPRESSURE_LEVELS.HIGH;

      const delayMultipliers = {
        [BACKPRESSURE_LEVELS.NONE]: 0,
        [BACKPRESSURE_LEVELS.LOW]: 1,
        [BACKPRESSURE_LEVELS.MEDIUM]: 2,
        [BACKPRESSURE_LEVELS.HIGH]: 4,
        [BACKPRESSURE_LEVELS.CRITICAL]: 8,
      };

      const multiplier = delayMultipliers[backpressureLevel] || 0;
      const delay = Math.min(
        this.config.initialDelay * multiplier,
        this.config.maxDelay,
      );

      return {
        backpressureLevel,
        shouldPause,
        recommendedDelay: delay,
        metrics: {
          lag: {
            total: metrics.totalLag,
            max: metrics.maxPartitionLag,
            average: metrics.avgLag,
            threshold: this.config.maxLag,
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
}

module.exports = AbstractMonitorService;
