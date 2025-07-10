/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Kafka Producer Implementation.
 *
 * Concrete implementation of AbstractProducer using Kafka as the message broker.
 * Inherits all the rich functionality from AbstractProducer while providing
 * Kafka-specific message sending capabilities and native deduplication features.
 */
const { AbstractProducer } = require("../../../abstracts");
const KafkaManager = require("./kafka-manager");
const KafkaMonitorService = require("./kafka-monitor-service");
const CacheClientFactory = require("../../cache/cache-client-factory");
const logger = require("../../../services/logger-service");

class KafkaProducer extends AbstractProducer {
  _groupId;
  _clientOptions;
  _producerOptions;
  _client;
  _admin;
  _producer;

  constructor(config = {}) {
    super(KafkaManager.standardizeConfig(config, "producer"));
    this._clientOptions = this.config.clientOptions;
    this._producerOptions = this.config.producerOptions;
    this._groupId = this.config.groupId;
    this._client = KafkaManager.createClient(this._clientOptions);
    this._admin = KafkaManager.createAdmin(this._client, null);
    this._producer = KafkaManager.createProducer(this._client, null, this._producerOptions);
  }

  _createCacheLayer(config = {}) {
    return CacheClientFactory.createClient(config.cacheOptions);
  }

  _createMonitorService(config) {
    return new KafkaMonitorService({
      topic: config.topic,
      groupId: config.groupId,
      maxLag: config.maxLag || 1_000,
      checkInterval: config.checkLagInterval || 60_000,
      clientOptions: config.clientOptions,
    });
  }

  async _connectToMessageBroker() {
    await this._admin.connect();
    await this._producer.connect();
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
    this._client = null;
    logger.logConnectionEvent("KafkaProducer", "disconnected from Kafka broker");
  }

  /**
   * Gets the message type identifier
   * @returns {string} Message type
   */
  getMessageType() {
    return this._topic;
  }

  /**
   * Gets the message broker type
   * @returns {string} Broker type
   */
  getBrokerType() {
    return "kafka";
  }

  _createBrokerMessages(items) {
    return KafkaManager.createMessages(items, this.getMessageType());
  }

  async _isTopicExisted() {
    return await KafkaManager.isTopicExisted(this._admin, this._topic);
  }

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

  /**
   * Gets unique identifier for an item
   * @param {Object} item The item to get ID for
   * @returns {string} Unique identifier
   */
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
