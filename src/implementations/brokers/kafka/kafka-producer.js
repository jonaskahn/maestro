/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Kafka Producer Implementation
 *
 * Concrete implementation of AbstractProducer using Kafka as the message broker.
 * Inherits all the rich functionality from AbstractProducer while providing
 * Kafka-specific message sending capabilities and native deduplication features.
 */
const AbstractProducer = require("../../../abstracts/abstract-producer");
const KafkaManager = require("./kafka-manager");
const KafkaMonitorService = require("./kafka-monitor-service");
const CacheClientFactory = require("../../cache/cache-client-factory");
const logger = require("../../../services/logger-service");

class KafkaProducer extends AbstractProducer {
  _groupId;
  _topicOptions;
  _clientOptions;
  _producerOptions;
  _admin;
  _producer;

  constructor(config = {}) {
    super(KafkaManager.standardizeConfig(config, "producer"));
    this._topicOptions = this._config.topicOptions || {};
    this._clientOptions = this._config.clientOptions;
    this._producerOptions = this._config.producerOptions;
    this._groupId = this._config.groupId;
  }

  _createCacheLayer(config = {}) {
    return CacheClientFactory.createClient(config.cacheOptions);
  }

  _createMonitorService(config) {
    return new KafkaMonitorService({
      topic: config.topic,
      groupId: config.groupId,
      lagThreshold: config.lagThreshold || 1_000,
      checkInterval: config.lagMonitorInterval || 60_000,
      clientOptions: config.clientOptions,
    });
  }

  async _connectToMessageBroker() {
    this._producer = await KafkaManager.createProducer(null, this._clientOptions, this._producerOptions);
    this._admin = await KafkaManager.createAdmin(null, this._clientOptions);
    await this._producer.connect();
    await this._admin.connect();
  }

  async _disconnectFromMessageBroker() {
    if (this._producer) {
      await this._producer.disconnect();
      this._producer = null;
    }
    if (this._admin) {
      await this._admin.disconnect();
      this._admin = null;
    }
    logger.logConnectionEvent("KafkaProducer", "disconnected from Kafka broker");
  }

  getMessageType() {
    return this._topic;
  }

  /**
   * Get the message broker type
   * @returns {string} Broker type
   */
  getBrokerType() {
    return "kafka";
  }

  /**
   * Create Kafka formatted messages from input items
   * @param {Array<Object>} items - Array of items to be converted to Kafka messages
   * @returns {Array<Object>} Array of Kafka formatted messages with key, value, headers, timestamp and partition
   * @private
   */
  _createBrokerMessages(items) {
    return KafkaManager.createMessages(items, this.getMessageType());
  }

  /**
   * Send messages to Kafka broker
   * @param {Array<Object>} messages - Array of formatted Kafka messages
   * @param {Object} _options - Additional send options (unused in this implementation)
   * @returns {Promise<Object>} Result object containing sent status, broker result, and message counts
   * @private
   */
  async _sendMessagesToBroker(messages, _options) {
    const sendOptions = {
      acks: this._producerOptions.acks,
      timeout: this._producerOptions.timeout,
      compression: this._producerOptions.compression,
    };

    const kafkaMessage = {
      topic: this._topic,
      messages,
      acks: sendOptions.acks,
      timeout: sendOptions.timeout,
    };

    if (sendOptions.getCompressionType) {
      kafkaMessage.compression = sendOptions.getCompressionType;
    }

    try {
      const result = await this._producer.send(kafkaMessage);
      return {
        sent: true,
        result,
        totalMessages: messages.length,
        sentMessages: messages.length,
        deduplicatedMessages: 0,
      };
    } catch (error) {
      logger.logError(`Failed to send messages to topic '${this._topic}'`, error);
      throw error;
    }
  }

  async _createTopicIfAllowed() {
    if (await KafkaManager.isTopicExisted(this._admin, this._topic)) {
      return true;
    }
    if (this._topicOptions.allowAutoTopicCreation) {
      return await KafkaManager.createTopic(this._admin, this._topic, this._topicOptions);
    }
    return false;
  }

  getItemId(item) {
    return item._id;
  }

  _getStatusConfig() {
    return {
      ...super._getStatusConfig(),
      kafkaProducerConnected: this._producer !== null,
      backpressureMonitorEnabled: this.getBackpressureMonitor() !== null,
      isIdempotent: this._producerOptions.idempotent,
      groupId: this._groupId,
    };
  }
}

module.exports = KafkaProducer;
