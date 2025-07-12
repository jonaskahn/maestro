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
 * Provides Kafka-specific message serialization, partitioning strategies, and delivery
 * guarantees. Supports topic creation, compression, idempotence, and transaction
 * capabilities with built-in backpressure monitoring.
 */
const AbstractProducer = require("../../../abstracts/abstract-producer");
const KafkaManager = require("./kafka-manager");
const KafkaMonitorService = require("./kafka-monitor-service");
const CacheClientFactory = require("../../cache/cache-client-factory");
const logger = require("../../../services/logger-service");

/**
 * Kafka Producer
 *
 * Implements the AbstractProducer interface for Apache Kafka.
 * Manages connections to Kafka brokers, message serialization, and delivery
 * with configurable acknowledgment and compression options.
 */
class KafkaProducer extends AbstractProducer {
  _groupId;
  _topicOptions;
  _clientOptions;
  _producerOptions;
  _admin;
  _producer;

  /**
   * Creates a new Kafka producer instance
   *
   * @param {Object} config - Producer configuration object
   * @param {string} config.topic - Topic to produce messages to
   * @param {string} [config.groupId] - Producer group ID for monitoring
   * @param {Object} [config.clientOptions] - Kafka client connection options
   * @param {string} [config.clientOptions.brokers] - Comma-separated list of Kafka brokers
   * @param {Object} [config.clientOptions.ssl] - SSL configuration options
   * @param {Object} [config.clientOptions.sasl] - SASL authentication options
   * @param {Object} [config.producerOptions] - Kafka producer specific options
   * @param {number} [config.producerOptions.acks] - Acknowledgment level (-1, 0, 1)
   * @param {boolean} [config.producerOptions.idempotent] - Enable idempotent production
   * @param {string} [config.producerOptions.compression] - Compression type ('gzip', 'snappy', etc.)
   * @param {Object} [config.topicOptions] - Topic configuration options
   * @param {boolean} [config.topicOptions.allowAutoTopicCreation] - Whether to create topic if it doesn't exist
   */
  constructor(config = {}) {
    super(KafkaManager.standardizeConfig(config, "producer"));
    this._topicOptions = this._config.topicOptions || {};
    this._clientOptions = this._config.clientOptions;
    this._producerOptions = this._config.producerOptions;
    this._groupId = this._config.groupId;
  }

  /**
   * Creates a cache layer for message deduplication
   * @param {Object} config - Cache configuration
   * @returns {Object|null} Cache client or null if disabled
   */
  _createCacheLayer(config = {}) {
    return CacheClientFactory.createClient(config.cacheOptions);
  }

  /**
   * Creates a backpressure monitor service for Kafka
   * @param {Object} config - Monitor configuration
   * @returns {KafkaMonitorService} Monitor service instance
   */
  _createMonitorService(config) {
    return new KafkaMonitorService({
      topic: config.topic,
      groupId: config.groupId,
      lagThreshold: config.lagThreshold || 1_000,
      checkInterval: config.lagMonitorInterval || 60_000,
      clientOptions: config.clientOptions,
    });
  }

  /**
   * Connects to Kafka broker and admin client
   * @returns {Promise<void>}
   */
  async _connectToMessageBroker() {
    this._producer = await KafkaManager.createProducer(null, this._clientOptions, this._producerOptions);
    this._admin = await KafkaManager.createAdmin(null, this._clientOptions);
    await this._producer.connect();
    await this._admin.connect();
  }

  /**
   * Disconnects from Kafka broker and admin client
   * @returns {Promise<void>}
   */
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

  /**
   * Gets the message type identifier (topic name)
   * @returns {string} Message type identifier
   */
  getMessageType() {
    return this._topic;
  }

  /**
   * Gets the message broker type identifier
   * @returns {string} Broker type ('kafka')
   */
  getBrokerType() {
    return "kafka";
  }

  /**
   * Creates Kafka formatted messages from input items
   *
   * Transforms business objects into the format expected by Kafka,
   * including appropriate keys and partitioning information.
   *
   * @param {Array<Object>} items - Array of items to be converted to Kafka messages
   * @returns {Array<Object>} Array of Kafka formatted messages
   */
  _createBrokerMessages(items) {
    return KafkaManager.createMessages(items, this.getMessageType());
  }

  /**
   * Sends messages to Kafka broker
   *
   * Handles the actual delivery to Kafka with configured acknowledgment
   * levels and compression settings.
   *
   * @param {Array<Object>} messages - Array of formatted Kafka messages
   * @param {Object} _options - Additional send options
   * @returns {Promise<Object>} Result object with send status and details
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

  /**
   * Creates topic if it doesn't exist and auto-creation is enabled
   * @returns {Promise<boolean>} True if topic exists or was created
   */
  async _createTopicIfAllowed() {
    if (await KafkaManager.isTopicExisted(this._admin, this._topic)) {
      return true;
    }
    if (this._topicOptions.allowAutoTopicCreation) {
      return await KafkaManager.createTopic(this._admin, this._topic, this._topicOptions);
    }
    return false;
  }

  /**
   * Gets the item ID from an item object
   * @param {Object} item - Item to get ID from
   * @returns {string} Unique item identifier
   */
  getItemId(item) {
    return item._id;
  }

  /**
   * Gets configuration status information including Kafka-specific details
   * @returns {Object} Extended configuration status
   */
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
