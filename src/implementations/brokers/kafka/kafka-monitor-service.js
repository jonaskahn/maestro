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
const KafkaManager = require("./kafka-manager");

class KafkaMonitorService extends AbstractMonitorService {
  _groupId;
  _clientOptions;
  _admin;

  constructor(config) {
    super(config);
    this._groupId = config.groupId;
    this._clientOptions = config.clientOptions;
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
      logger.logDebug(`Start monitoring current consumer for topic ${this._topic} lag`);
      const totalLag = await this._fetchCurrentLag();

      return {
        totalLag,
        maxPartitionLag: totalLag,
        avgLag: totalLag,
        lagThreshold: this.config.lagThreshold,
      };
    } catch (error) {
      logger.logError(`Failed monitoring current consumer for topic ${this._topic} lag:`, error);
      throw error;
    } finally {
      logger.logDebug(`Finish monitoring current consumer for topic ${this._topic} lag`);
    }
  }

  /**
   * Get system resource metrics (required by abstract class)
   * @returns {Promise<Object>} Resource metrics
   */
  getResourceMetrics() {
    return {
      memoryUsage: (process.memoryUsage().heapUsed / process.memoryUsage().heapTotal) * 100,
      cpuUsage: 0,
      networkLatency: 0,
    };
  }

  async connect() {
    await super.connect();
    this._admin = await KafkaManager.createAdmin(null, this._clientOptions);
    await this._admin.connect();
  }

  async _fetchCurrentLag() {
    if (!this._groupId || !this._topic) {
      logger.logWarning("Consumer group or topic not configured, returning 0 lag");
      return 0;
    }
    if (await KafkaManager.isTopicExisted(this._admin, this._topic)) {
      return await KafkaManager.calculateConsumerLag(this._groupId, this._topic, this._admin);
    }
    return 0;
  }

  async disconnect() {
    try {
      await super.disconnect();
      await this._admin.disconnect();
      this._admin = null;
    } catch (error) {
      logger.logWarning(`⚠️ Errors happened when Kafka Monitor Service disconnect its services`, error);
    }
  }
}

module.exports = KafkaMonitorService;
