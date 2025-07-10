/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Kafka Consumer Implementation
 *
 * Concrete implementation of AbstractConsumer using Kafka as the message broker.
 * Provides specialized support for Kafka message consumption, deserialization,
 * offset management, and parallel partition processing.
 */
const AbstractConsumer = require("../../../abstracts/abstract-consumer");
const logger = require("../../../services/logger-service");

const KafkaManager = require("./kafka-manager");
const CacheClientFactory = require("../../cache/cache-client-factory");

class KafkaConsumer extends AbstractConsumer {
  _groupId;
  _clientOptions;
  _consumerOptions;
  _client;
  _consumer;

  /**
   * Create a new Kafka consumer instance
   * @param {Object} config Configuration object
   */
  constructor(config) {
    super(KafkaManager.standardizeConfig(config, "consumer"));
    this._groupId = this.config.groupId;
    this._clientOptions = this.config.clientOptions;
    this._consumerOptions = this.config.consumerOptions;
    this._client = KafkaManager.createClient(this._clientOptions);
    this._consumer = KafkaManager.createConsumer(this._client, null, this._consumerOptions);
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
    await this._consumer.connect();
    logger.logConnectionEvent("🔌 Kafka Consumer", "connected to Kafka broker");
  }

  async _disconnectFromMessageBroker() {
    if (this._consumer) {
      await this._consumer.disconnect();
      this._consumer = null;
    }
    this._client = null;
    logger.logConnectionEvent("Kafka Consumer", "disconnected from Kafka broker");
  }

  async _startConsumingFromBroker(_options = {}) {
    await this._consumer.subscribe({
      topic: this._topic,
      fromBeginning: this._consumerOptions.fromBeginning,
    });

    await this._consumer.run({
      partitionsConsumedConcurrently: this.maxConcurrency,
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const standardizeMessage = this.#extractBrokerMessage({
            _topic: topic,
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

    logger.logInfo(`📨 Kafka consumer started for topic '${this._topic}' in group '${this.groupId}'`);
  }

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
      groupId: this.groupId,
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
