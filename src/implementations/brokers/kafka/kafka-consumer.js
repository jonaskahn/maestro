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
const logger = require("../../../services/logger-service");

// Using the new KafkaManager that combines utilities and client factory
const KafkaManager = require("./kafka-manager");
const CacheClientFactory = require("../../cache/cache-client-factory");

class KafkaConsumer extends AbstractConsumer {
  /**
   * Create a new Kafka consumer instance
   * @param {Object} config - Configuration object
   * @param {string} config.topic - Topic to consume from
   * @param {string} config.groupId - Consumer group ID
   * @param {Object} config.clientOptions - Kafka client connection options
   * @param {Object} config.consumerOptions - Kafka consumer specific options
   * @param {boolean} [config.consumerOptions.fromBeginning=false] - Whether to consume from beginning
   * @param {boolean} [config.consumerOptions.autoCommit=true] - Whether to auto-commit offsets
   */
  constructor(config) {
    super(KafkaManager.standardizeConfig(config, "consumer"));
    this.groupId = this.config.groupId;
    this.clientOptions = this.config.clientOptions;
    this.consumerOptions = this.config.consumerOptions;
    this.consumer = KafkaManager.createConsumer(null, this.clientOptions, this.consumerOptions);
  }

  _createCacheLayer(cacheOptions) {
    if (!cacheOptions) {
      logger.logWarning("⁉️Cache layer is disabled, config is not yet defined");
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

  async _connectToMessageBroker() {
    await this.consumer.connect();
    logger.logConnectionEvent("🔌 Kafka Consumer", "connected to Kafka broker");
  }

  async _disconnectFromMessageBroker() {
    if (this.consumer) {
      await this.consumer.disconnect();
      this.consumer = null;
    }
    logger.logConnectionEvent("Kafka Consumer", "disconnected from Kafka broker");
  }

  async _startConsumingFromBroker(_options = {}) {
    await this.consumer.subscribe({
      topic: this.topic,
      fromBeginning: this.consumerOptions.fromBeginning,
    });

    await this.consumer.run({
      partitionsConsumedConcurrently: this.maxConcurrency,
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
          if (this.consumerOptions.autoCommit) {
            logger.logDebug(`🔄 Auto-committed message offset ${message.offset}`);
          } else {
            if (!standardizeMessage.committed) {
              await this.consumer.commitOffsets([
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
          if (!this.consumerOptions.autoCommit) {
            try {
              await this.consumer.commitOffsets([
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

    logger.logInfo(`📨 Kafka consumer started for topic '${this.topic}' in group '${this.groupId}'`);
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
    await this.consumer?.stop();
    logger.logInfo(`⏹️ Kafka consumer stopped for topic '${this.topic}'`);
  }

  getConfigStatus() {
    return {
      ...super.getConfigStatus(),
      groupId: this.groupId,
      sessionTimeout: this.consumerOptions.sessionTimeout,
      heartbeatInterval: this.consumerOptions.heartbeatInterval,
      maxBytesPerPartition: this.consumerOptions.maxBytesPerPartition,
      autoCommit: this.consumerOptions.autoCommit,
      fromBeginning: this.consumerOptions.fromBeginning,
      partitionsConsumedConcurrently: this.maxConcurrency,
    };
  }
}

module.exports = KafkaConsumer;
