/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Kafka Consumer Implementation
 *
 * Provides specialized Kafka implementation of AbstractConsumer
 * with support for message consumption, deserialization, and batching.
 */
const AbstractConsumer = require("../../../abstracts/abstract-consumer");
const KafkaManager = require("./kafka-manager");
const CacheClientFactory = require("../../cache/cache-client-factory");
const logger = require("../../../services/logger-service");

class KafkaConsumer extends AbstractConsumer {
  _topicOptions;
  _groupId;
  _clientOptions;
  _consumerOptions;
  _admin;
  _consumer;

  /**
   * Create a new Kafka consumer instance
   * @param {Object} config - Configuration object
   * @param {string} config.topic - Topic to consume from
   * @param {string} config.groupId - Consumer group ID
   * @param {Object} config.clientOptions - Kafka _client connection options
   * @param {Object} config.consumerOptions - Kafka consumer specific options
   * @param {boolean} [config.consumerOptions.fromBeginning=false] - Whether to consume from beginning
   * @param {boolean} [config.consumerOptions.autoCommit=true] - Whether to auto-commit offsets
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

  _createCacheLayer(cacheOptions) {
    if (!cacheOptions) {
      logger.logWarning("⁉️Cache layer is disabled, _config is not yet defined");
      return null;
    }
    return CacheClientFactory.createClient(cacheOptions);
  }

  /**
   * Get the broker type
   * @returns {string} Broker type
   */
  getBrokerType() {
    return "kafka";
  }

  async _createTopicIfAllowed() {
    if (await KafkaManager.isTopicExisted(this._admin, this._topic)) {
      return;
    }
    if (this._topicOptions.allowAutoTopicCreation) {
      await KafkaManager.createTopic(this._admin, this._topic, this._topicOptions);
    }
  }

  async _connectToMessageBroker() {
    await this._consumer.connect();
    await this._admin.connect();
    logger.logConnectionEvent("🔌 Kafka Consumer", "connected to Kafka broker");
  }

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
            logger.logDebug(`🔄 Auto-committed message offset ${message.offset}`);
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
              logger.logDebug(`☑️ Manually committed message offset ${message.offset}`);
            }
          }
        } catch (error) {
          logger.logError(`❌ Error processing Kafka message from ${topic}:${partition}:${message.offset}`, error);
          if (!this._consumerOptions.autoCommit) {
            try {
              await this._consumer.commitOffsets([
                {
                  topic,
                  partition,
                  offset: (parseInt(message.offset) + 1).toString(),
                },
              ]);
              logger.logWarning(`⚠️ Committed failed message offset ${message.offset} to prevent reprocessing`);
            } catch (commitError) {
              logger.logError(`❌ Failed to commit offset after error`, commitError);
            }
          }
        }
      },
    });

    logger.logInfo(`📨 Kafka consumer started for topic '${this._topic}' in group '${this._groupId}'`);
  }

  /**
   * Convert Kafka message to standardized format following STANDARDIZED_MESSAGE_INTERFACE
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
    logger.logDebug(`ℹ️ KafkaConsumer start to unwrap received message`);
    const { topic, partition, message } = kafkaMessage;
    const messageId = KafkaManager.createMessageId(topic, partition, message?.offset);
    const content = KafkaManager.parseMessageValue(message?.value);
    const standardizeMessage = {
      type: topic,
      messageId,
      item: content,
      committed: false,
    };
    logger.logDebug(`ℹ️ KafkaConsumer unwrap a message offset ${standardizeMessage.messageId}`);
    return standardizeMessage;
  }

  async _stopConsumingFromBroker() {
    await this._consumer?.stop();
    logger.logInfo(`⏹️ Kafka consumer stopped for topic '${this._topic}'`);
  }

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
}

module.exports = KafkaConsumer;
