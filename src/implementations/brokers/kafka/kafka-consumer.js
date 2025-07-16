/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Kafka Consumer Implementation
 *
 * Provides Kafka-specific implementation of the AbstractConsumer interface.
 * Handles Kafka message consumption, offset management, consumer group coordination,
 * and message deserialization. Supports both auto-commit and manual offset commits,
 * topic creation, and integration with the cache layer for message deduplication.
 */
const AbstractConsumer = require("../../../abstracts/abstract-consumer");
const KafkaManager = require("./kafka-manager");
const CacheClientFactory = require("../../cache/cache-client-factory");
const logger = require("../../../services/logger-service");

/**
 * Kafka Consumer
 *
 * Implements the AbstractConsumer interface for the Apache Kafka message broker.
 * Manages consumer group lifecycle, message parsing, and offset management
 * while providing standardized methods for message consumption and processing.
 */
class KafkaConsumer extends AbstractConsumer {
  _topicOptions;
  _groupId;
  _clientOptions;
  _consumerOptions;
  _admin;
  _consumer;

  /**
   * Creates a new Kafka consumer instance
   *
   * @param {Object} config - Configuration object
   * @param {string} config.topic - Topic to consume from
   * @param {string} [config.groupId] - Consumer group ID for coordinated consumption (defaults to `${topic}-processors`)
   * @param {Object} [config.clientOptions] - Kafka client connection options
   * @param {string|string[]} [config.clientOptions.brokers=['localhost:9092']] - List of Kafka brokers
   * @param {string} [config.clientOptions.clientId] - Client ID (defaults to `${topic}-client-${timestamp}`)
   * @param {Object} [config.clientOptions.ssl] - SSL configuration options
   * @param {Object} [config.clientOptions.sasl] - SASL authentication options
   * @param {number} [config.clientOptions.connectionTimeout] - Connection timeout from TTLConfig
   * @param {number} [config.clientOptions.requestTimeout] - Request timeout from TTLConfig
   * @param {boolean} [config.clientOptions.enforceRequestTimeout=true] - Whether to enforce request timeout
   * @param {Object} [config.clientOptions.retry] - Retry options for client
   * @param {Object} [config.consumerOptions] - Kafka consumer specific options
   * @param {boolean} [config.consumerOptions.fromBeginning=true] - Whether to consume from beginning of topic
   * @param {boolean} [config.consumerOptions.autoCommit=true] - Whether to auto-commit offsets
   * @param {number} [config.consumerOptions.sessionTimeout] - Session timeout in ms (defaults to TASK_PROCESSING_STATE_TTL)
   * @param {number} [config.consumerOptions.rebalanceTimeout] - Rebalance timeout in ms (defaults to TASK_PROCESSING_STATE_TTL * 5)
   * @param {number} [config.consumerOptions.heartbeatInterval] - Heartbeat interval in ms (defaults to TASK_PROCESSING_STATE_TTL / 10)
   * @param {number} [config.consumerOptions.maxBytesPerPartition=1048576] - Max bytes per partition
   * @param {number} [config.consumerOptions.minBytes=1] - Min bytes to fetch
   * @param {number} [config.consumerOptions.maxBytes=10485760] - Max bytes to fetch
   * @param {number} [config.consumerOptions.maxWaitTimeInMs] - Max wait time in ms (defaults to connectionTimeout * 5)
   * @param {number} [config.consumerOptions.maxInFlightRequests=null] - Max in flight requests
   * @param {Object} [config.topicOptions] - Topic configuration options
   * @param {number} [config.topicOptions.partitions=5] - Number of partitions for topic
   * @param {number} [config.topicOptions.replicationFactor=1] - Replication factor for topic
   * @param {boolean} [config.topicOptions.allowAutoTopicCreation=true] - Whether to create topic if it doesn't exist
   * @param {string} [config.topicOptions.keyPrefix] - Key prefix for cache (defaults to topic name uppercase)
   * @param {number} [config.topicOptions.processingTtl=5000] - TTL for processing state in ms
   * @param {number} [config.topicOptions.suppressionTtl] - TTL for suppression in ms (defaults to processingTtl * 3)
   * @param {number} [config.maxConcurrency=1] - Max concurrent messages to process
   * @param {Object} [config.cacheOptions] - Cache configuration for deduplication
   */
  constructor(config) {
    super(KafkaManager.standardizeConfig(config, "consumer"));
    this._topicOptions = this._config.topicOptions;
    this._groupId = this._config.groupId;
    this._clientOptions = this._config.clientOptions;
    this._consumerOptions = this._config.consumerOptions;
    this._admin = KafkaManager.createAdmin(null, this._clientOptions);
    this._consumer = KafkaManager.createConsumer(null, this._clientOptions, this._consumerOptions);
  }

  /**
   * Gets the broker type identifier
   * @returns {string} Broker type ('kafka')
   */
  getBrokerType() {
    return "kafka";
  }

  /**
   * Gets consumer status information including Kafka-specific metrics
   * @returns {Object} Status object with connection and configuration details
   */
  getConfigStatus() {
    return {
      ...super.getConfigStatus(),
      groupId: this._groupId,
      sessionTimeout: this._consumerOptions.sessionTimeout,
      heartbeatInterval: this._consumerOptions.heartbeatInterval,
      maxBytesPerPartition: this._consumerOptions.maxBytesPerPartition,
      autoCommit: this._consumerOptions.autoCommit,
      fromBeginning: this._consumerOptions.fromBeginning,
      partitionsConsumedConcurrently: this.maxConcurrency,
    };
  }

  /**
   * Connects to Kafka broker and admin client
   * @returns {Promise<void>}
   */
  async _connectToMessageBroker() {
    await this._consumer.connect();
    await this._admin.connect();
    logger.logConnectionEvent("Kafka Consumer", "connected to Kafka broker");
  }

  /**
   * Creates topic if it doesn't exist and auto-creation is enabled
   * @returns {Promise<void>}
   */
  async _createTopicIfAllowed() {
    if (await KafkaManager.isTopicExisted(this._admin, this._topic)) {
      return;
    }
    if (this._topicOptions.allowAutoTopicCreation) {
      await KafkaManager.createTopic(this._admin, this._topic, this._topicOptions);
    }
  }

  /**
   * Creates a cache layer for message deduplication
   * @param {Object} cacheOptions - Cache configuration options
   * @returns {Object|null} Cache client instance or null if disabled
   */
  _createCacheLayer(cacheOptions) {
    if (!cacheOptions) {
      logger.logWarning("Cache layer is disabled, _config is not yet defined");
      return null;
    }
    return CacheClientFactory.createClient(cacheOptions);
  }

  /**
   * Starts consuming messages from Kafka topic
   *
   * Sets up subscription to the topic and configures message handling with
   * business logic processing and offset management.
   *
   * @param {Object} _options - Consumption options
   * @returns {Promise<void>}
   */
  async _startConsumingFromBroker(_options = {}) {
    await this._consumer.subscribe({
      topic: this._topic,
      fromBeginning: this._consumerOptions.fromBeginning,
    });

    await this._consumer.run({
      eachBatchAutoResolve: this._config.eachBatchAutoResolve,
      partitionsConsumedConcurrently: this.maxConcurrency,
      autoCommit: this._config.autoCommit,
      autoCommitInterval: this._config.autoCommitInterval,
      autoCommitThreshold: this._config.autoCommitThreshold,
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const standardizeMessage = this.#extractBrokerMessage({
            topic,
            partition,
            message,
          });

          await this._defaultBusinessHandler(
            standardizeMessage.type,
            standardizeMessage.messageId,
            standardizeMessage.item
          );
          if (this._consumerOptions.autoCommit) {
            logger.logDebug(`Auto-committed message offset ${message.offset}`);
          } else {
            if (!standardizeMessage.committed) {
              await this._consumer.commitOffsets([
                {
                  topic,
                  partition,
                  offset: (parseInt(message.offset) + 1).toString(),
                },
              ]);
              standardizeMessage.committed = true;
              logger.logDebug(`Manually committed message offset ${message.offset} on partition ${partition}`);
            }
          }
        } catch (error) {
          logger.logError(`Error processing Kafka message from ${topic}:${partition}:${message.offset}`, error);
          if (!this._consumerOptions.autoCommit) {
            try {
              await this._consumer.commitOffsets([
                {
                  topic,
                  partition,
                  offset: (parseInt(message.offset) + 1).toString(),
                },
              ]);
              logger.logWarning(`Committed failed message offset ${message.offset} to prevent reprocessing`);
            } catch (commitError) {
              logger.logError(`Failed to commit offset after error`, commitError);
            }
          }
        }
      },
    });

    logger.logInfo(`Kafka consumer started for topic '${this._topic}' in group '${this._groupId}'`);
  }

  /**
   * Converts Kafka message to standardized format
   *
   * Extracts essential information from raw Kafka message format into a standardized
   * message object that can be processed by the business logic handler.
   *
   * @param {Object} kafkaMessage - Native Kafka message
   * @param {string} kafkaMessage.topic - The Kafka topic
   * @param {number} kafkaMessage.partition - The Kafka partition
   * @param {Object} kafkaMessage.message - The raw Kafka message object
   * @param {string} kafkaMessage.message.offset - Message offset in the partition
   * @param {Buffer} kafkaMessage.message.value - Message value as Buffer
   * @returns {Object} Standardized message with type, messageId, item and committed status
   * @private
   */
  #extractBrokerMessage(kafkaMessage) {
    logger.logDebug(`KafkaConsumer starting to unwrap received message`);
    const { topic, partition, message } = kafkaMessage;
    const messageId = KafkaManager.createMessageId(topic, partition, message?.offset);
    const content = KafkaManager.parseMessageValue(message?.value);
    const standardizeMessage = {
      type: topic,
      messageId,
      item: content,
      committed: false,
    };
    logger.logDebug(`KafkaConsumer unwrapped message with ID ${standardizeMessage.messageId}`);
    return standardizeMessage;
  }

  /**
   * Stops message consumption from Kafka
   * @returns {Promise<void>}
   */
  async _stopConsumingFromBroker() {
    await this._consumer?.stop();
    logger.logInfo(`Kafka consumer stopped for topic '${this._topic}'`);
  }

  /**
   * Disconnects from Kafka broker and admin client
   * @returns {Promise<void>}
   */
  async _disconnectFromMessageBroker() {
    if (this._consumer) {
      await this._consumer.disconnect();
      this._consumer = null;
    }
    if (this._admin) {
      await this._admin.disconnect();
      this._admin = null;
    }
    logger.logConnectionEvent("Kafka Consumer", "disconnected from Kafka broker");
  }
}

module.exports = KafkaConsumer;
