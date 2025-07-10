/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Kafka Monitor Service
 *
 * Concrete implementation of AbstractMonitorService for Kafka.
 * Monitors Kafka topics and consumer groups to collect performance metrics
 * and detect backpressure conditions. Used to implement automatic flow control
 * based on consumer lag and system resource utilization.
 */
const AbstractMonitorService = require("../../../abstracts/abstract-monitor-service");
const KafkaManager = require("./kafka-manager");
const logger = require("../../../services/logger-service");

class KafkaMonitorService extends AbstractMonitorService {
  _topic;
  _groupId;
  _clientOptions;
  _admin;

  /**
   * Create a new Kafka monitor service
   * @param {Object} config Monitor service configuration
   */
  constructor(config) {
    super(config);
    this._topic = config.topic;
    this._groupId = config.groupId;
    this._clientOptions = config.clientOptions;
  }

  /**
   * Connect to Kafka and start monitoring
   * @returns {Promise<void>}
   */
  async connect() {
    await super.connect();
    this._admin = await KafkaManager.createAdmin(null, this._clientOptions);
    await this._admin.connect();
  }

  /**
   * Get the broker type
   * @returns {string} Broker type
   */
  getBrokerType() {
    return "kafka";
  }

  /**
   * Get consumer lag metrics from Kafka
   * @returns {Promise<Object>} Lag metrics
   */
  async getConsumerLag() {
    try {
      logger.logDebug(`✨ Start monitoring current consumer for topic ${this._topic} lag`);
      const totalLag = await this._fetchCurrentLag();

      return {
        totalLag,
        maxPartitionLag: totalLag,
        avgLag: totalLag,
        lagThreshold: this.config.maxLag,
      };
    } catch (error) {
      logger.logError(`‼️ Failed monitoring current consumer for topic ${this._topic} lag:`, error);
      throw error;
    } finally {
      logger.logDebug(`☑️ Finish monitoring current consumer for topic ${this._topic} lag`);
    }
  }

  /**
   * Get system resource metrics
   * @returns {Promise<Object>} Resource metrics
   */
  async getResourceMetrics() {
    return {
      memoryUsage: (process.memoryUsage().heapUsed / process.memoryUsage().heapTotal) * 100,
      cpuUsage: 0,
      networkLatency: 0,
    };
  }

  /**
   * Fetch current consumer lag from Kafka
   * @returns {Promise<number>} Consumer lag value
   */
  async _fetchCurrentLag() {
    if (!this._groupId || !this._topic) {
      logger.logWarning("Consumer group or _topic not configured, returning 0 lag");
      return 0;
    }

    if (await KafkaManager.isTopicExisted(this._admin, this._topic)) {
      return await KafkaManager.calculateConsumerLag(this._groupId, this._topic, this._admin);
    } else {
      logger.logWarning(`Monitor skipped due the topic [ ${this._topic} ] does not existed`);
      return 0;
    }
  }

  /**
   * Disconnect from Kafka and stop monitoring
   * @returns {Promise<void>}
   */
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
