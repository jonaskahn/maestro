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
   * @param {string} [config.groupId] - Producer group ID for monitoring (defaults to `${topic}-processors`)
   * @param {Object} [config.clientOptions] - Kafka client connection options
   * @param {string|string[]} [config.clientOptions.brokers=['localhost:9092']] - List of Kafka brokers
   * @param {string} [config.clientOptions.clientId] - Client ID (defaults to `${topic}-client-${timestamp}`)
   * @param {Object} [config.clientOptions.ssl] - SSL configuration options
   * @param {Object} [config.clientOptions.sasl] - SASL authentication options
   * @param {number} [config.clientOptions.connectionTimeout] - Connection timeout from TTLConfig
   * @param {number} [config.clientOptions.requestTimeout] - Request timeout from TTLConfig
   * @param {boolean} [config.clientOptions.enforceRequestTimeout=true] - Whether to enforce request timeout
   * @param {Object} [config.clientOptions.retry] - Retry options for client
   * @param {Object} [config.producerOptions] - Kafka producer specific options
   * @param {Function} [config.producerOptions.createPartitioner=Partitioners.LegacyPartitioner] - Partitioner function
   * @param {number} [config.producerOptions.acks=-1] - Acknowledgment level (-1=all, 0=none, 1=leader)
   * @param {number} [config.producerOptions.timeout] - Producer request timeout in ms (defaults to requestTimeout)
   * @param {string} [config.producerOptions.compression='none'] - Compression type ('gzip', 'snappy', 'lz4', 'zstd')
   * @param {boolean} [config.producerOptions.idempotent=false] - Enable idempotent production
   * @param {number} [config.producerOptions.maxInFlightRequests=5] - Max requests in flight
   * @param {number} [config.producerOptions.transactionTimeout] - Transaction timeout in ms
   * @param {number} [config.producerOptions.retries=10] - Number of retries for failed requests
   * @param {Object} [config.producerOptions.retry] - Retry configuration for producer
   * @param {Object} [config.topicOptions] - Topic configuration options
   * @param {number} [config.topicOptions.partitions=5] - Number of partitions for topic
   * @param {number} [config.topicOptions.replicationFactor=1] - Replication factor for topic
   * @param {boolean} [config.topicOptions.allowAutoTopicCreation=true] - Whether to create topic if it doesn't exist
   * @param {string} [config.topicOptions.keyPrefix] - Key prefix for cache (defaults to topic name uppercase)
   * @param {number} [config.topicOptions.processingTtl=5000] - TTL for processing state in ms
   * @param {number} [config.topicOptions.suppressionTtl] - TTL for suppression in ms (defaults to processingTtl * 5)
   * @param {boolean} [config.useSuppression=true] - Whether to use message suppression for deduplication
   * @param {boolean} [config.useDistributedLock=true] - Whether to use distributed lock for coordination
   * @param {number} [config.lagThreshold=100] - Consumer lag threshold for backpressure monitoring
   * @param {number} [config.lagMonitorInterval=5000] - Interval for checking consumer lag in ms
   * @param {Object} [config.cacheOptions] - Cache configuration for deduplication
   */
  constructor(config = {}) {
    super(KafkaManager.standardizeConfig(config, "producer"));
    this._topicOptions = this._config.topicOptions || {};
    this._clientOptions = this._config.clientOptions;
    this._producerOptions = this._config.producerOptions;
    this._groupId = this._config.groupId;
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
   * Creates Kafka formatted messages from input items
   *
   * Transforms business objects into the format expected by Kafka,
   * including appropriate keys and partitioning information.
   *
   * @param {Array<Object>} items - Array of items to be converted to Kafka messages
   * @returns {Array<Object>} Array of Kafka formatted messages
   */
  _createBrokerMessages(items) {
    return KafkaManager.createMessages(items, this.getMessageType(), { key: this.getItemId });
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
    const kafkaMessage = {
      topic: this._topic,
      messages,
      ...this._producerOptions,
      ..._options,
    };

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
}

module.exports = KafkaProducer;
