/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Kafka Monitor Service
 *
 * Monitors Kafka topics and consumer groups for performance metrics and backpressure.
 * Used to implement automatic flow control based on consumer lag and other indicators.
 */
const AbstractMonitorService = require("../../../abstracts/abstract-monitor-service");
const logger = require("../../../services/logger-service");

// Using the new KafkaManager that combines utilities and client factory
const KafkaManager = require("./kafka-manager");

class KafkaMonitorService extends AbstractMonitorService {
  #topic;
  #groupId;
  #clientOptions;
  #admin;

  constructor(config) {
    super(config);
    this.#topic = config.topic;
    this.#groupId = config.groupId;
    this.#clientOptions = config.clientOptions;
  }

  async connect() {
    await super.connect();
    this.#admin = await KafkaManager.createAdmin(null, this.#clientOptions);
    await this.#admin.connect();
  }

  /**
   * Get the broker type
   * @returns {string} Broker type
   */
  getBrokerType() {
    return "kafka";
  }

  /**
   * Get consumer lag metrics (required by abstract class)
   * @returns {Promise<Object>} Lag metrics
   */
  async getConsumerLag() {
    try {
      logger.logDebug(`✨ Start monitoring current consumer for topic ${this.#topic} lag`);
      const totalLag = await this._fetchCurrentLag();

      return {
        totalLag,
        maxPartitionLag: totalLag,
        avgLag: totalLag,
        lagThreshold: this.config.maxLag,
      };
    } catch (error) {
      logger.logError(`‼️ Failed monitoring current consumer for topic ${this.#topic} lag:`, error);
      throw error;
    } finally {
      logger.logDebug(`☑️ Finish monitoring current consumer for topic ${this.#topic} lag`);
    }
  }

  /**
   * Get system resource metrics (required by abstract class)
   * @returns {Promise<Object>} Resource metrics
   */
  async getResourceMetrics() {
    return {
      memoryUsage: (process.memoryUsage().heapUsed / process.memoryUsage().heapTotal) * 100,
      cpuUsage: 0,
      networkLatency: 0,
    };
  }

  async _fetchCurrentLag() {
    if (!this.#groupId || !this.#topic) {
      logger.logWarning("Consumer group or topic not configured, returning 0 lag");
      return 0;
    }

    return await KafkaManager.calculateConsumerLag(this.#groupId, this.#topic, this.#admin);
  }

  async disconnect() {
    try {
      await super.disconnect();
      await this.#admin.disconnect();
      this.#admin = null;
    } catch (error) {
      logger.logWarning(`⚠️ Errors happened when Kafka Monitor Service disconnect its services`, error);
    }
  }
}

module.exports = KafkaMonitorService;
