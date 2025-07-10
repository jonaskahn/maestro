/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the Apache License 2.0 found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Kafka Producer Implementation
 *
 * Concrete implementation of AbstractProducer using Kafka as the message broker.
 * Inherits all the rich functionality from AbstractProducer while providing
 * Kafka-specific message sending capabilities and native deduplication features.
 */
const AbstractProducer = require("../../../abstracts/abstract-producer");
const logger = require("../../../services/logger-service");

// Using the new KafkaManager that combines utilities and client factory
const KafkaManager = require("./kafka-manager");
const KafkaMonitorService = require("./kafka-monitor-service");
const CacheClientFactory = require("../../cache/cache-client-factory");

class KafkaProducer extends AbstractProducer {
  #topic;
  #groupId;
  #clientOptions;
  #producerOptions;
  #producer;

  constructor(config = {}) {
    super(KafkaManager.standardizeConfig(config, "producer"));
    this.#clientOptions = this.config.clientOptions;
    this.#producerOptions = this.config.producerOptions;
    this.#topic = this.config.topic;
    this.#groupId = this.config.groupId;
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
    this.#producer = await KafkaManager.createProducer(
      null,
      this.#clientOptions,
      this.#producerOptions,
    );
    await this.#producer.connect();
  }

  async _disconnectFromMessageBroker() {
    if (this.#producer) {
      await this.#producer.disconnect();
      this.#producer = null;
    }
    logger.logConnectionEvent(
      "KafkaProducer",
      "disconnected from Kafka broker",
    );
  }

  getMessageType() {
    return this.#topic;
  }

  /**
   * Get the message broker type
   * @returns {string} Broker type
   */
  getBrokerType() {
    return "kafka";
  }

  /**
   * @param items
   * @return {{key: string, value: *, headers: {}|*, timestamp, partition: *}[]}
   * @private
   */
  _createBrokerMessages(items) {
    return KafkaManager.createMessages(items, this.getMessageType());
  }

  /**
   *
   * @param messages
   * @param _options
   * @return {Promise<{sent: boolean, result: *, totalMessages, sentMessages, deduplicatedMessages: number}>}
   * @private
   */
  async _sendMessagesToBroker(messages, _options) {
    const sendOptions = this.#getSendOptions();

    const kafkaMessage = {
      topic: this.#topic,
      messages,
      acks: sendOptions.acks,
      timeout: sendOptions.timeout,
    };

    if (sendOptions.getCompressionType) {
      kafkaMessage.compression = sendOptions.getCompressionType;
    }

    try {
      const result = await this.#producer.send(kafkaMessage);
      return {
        sent: true,
        result,
        totalMessages: messages.length,
        sentMessages: messages.length,
        deduplicatedMessages: 0,
      };
    } catch (error) {
      logger.logError(
        `Failed to send messages to topic '${this.#topic}'`,
        error,
      );
      throw error;
    }
  }

  #getSendOptions() {
    const baseSendOptions = {
      acks: this.#producerOptions.acks,
      timeout: this.#producerOptions.timeout,
    };

    const compression = this.#getValidCompressionType(
      this.#producerOptions.compression,
    );
    if (compression) {
      baseSendOptions.compression = compression;
    }
    return baseSendOptions;
  }

  #getValidCompressionType(compression) {
    if (!compression || compression === "none") {
      return undefined;
    }

    const validTypes = ["gzip", "snappy", "lz4", "zstd"];
    const normalizedCompression = compression.toLowerCase();

    if (validTypes.includes(normalizedCompression)) {
      return normalizedCompression;
    }

    logger.logWarning(
      `Invalid compression type "${compression}", falling back to no compression. Valid types: ${validTypes.join(", ")}`,
    );
    return undefined;
  }

  getItemId(item) {
    return item._id;
  }

  _getStatusConfig() {
    return {
      ...super._getStatusConfig(),
      kafkaProducerConnected: this.#producer !== null,
      backpressureMonitorEnabled: this.getBackpressureMonitor() !== null,
      isIdempotent: this.#producerOptions.idempotent,
      groupId: this.#groupId,
    };
  }
}

module.exports = KafkaProducer;
