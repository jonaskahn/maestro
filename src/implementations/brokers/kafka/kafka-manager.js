/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Kafka Manager
 *
 * Comprehensive manager for Kafka operations including:
 * - Client, producer, consumer and admin creation
 * - Configuration management with smart defaults
 * - Utility methods for message handling
 * - Monitoring and performance tools
 */
const { Kafka, CompressionTypes, Partitioners } = require("kafkajs");
const TtlConfig = require("../../../config/ttl-config");
const logger = require("../../../services/logger-service");
require("dotenv").config();

const crypto = require("crypto");

const KafkaTtlConfig = TtlConfig.getKafkaConfig();
const TopicTtlConfig = TtlConfig.getTopicConfig();

class KafkaManager {
  /**
   * Default topic configuration values
   */
  static get TOPIC_DEFAULTS() {
    return {
      NUM_PARTITIONS: 3,
      REPLICATION_FACTOR: 1,
      RETENTION_MS: 7 * 24 * 60 * 60 * 1000, // 7 days
      SEGMENT_MS: 24 * 60 * 60 * 1000, // 1 day
      COMPRESSION_TYPE: "gzip",
      CLEANUP_POLICY: "delete",
    };
  }

  /**
   * Default Kafka _client configuration
   */
  static get CLIENT_DEFAULTS() {
    return {
      clientId: process.env.MO_KAFKA_CLIENT_ID || `job-orchestrator-${new Date().getTime()}`,
      brokers: process.env.MO_KAFKA_BROKERS?.split(",") || ["localhost:9092"],
      connectionTimeout: KafkaTtlConfig.connectionTimeout,
      requestTimeout: KafkaTtlConfig.requestTimeout,
      enforceRequestTimeout: process.env.MO_KAFKA_ENFORCE_REQUEST_TIMEOUT !== "false",
      retry: {
        initialRetryTime: parseInt(process.env.MO_KAFKA_INITIAL_RETRY_TIME_MS) || 100,
        retries: parseInt(process.env.MO_KAFKA_RETRY_COUNT) || 10,
        factor: 0.2,
        multiplier: 2,
        maxRetryTime: parseInt(process.env.MO_KAFKA_MAX_RETRY_TIME_MS) || 30000,
      },
    };
  }

  /**
   * Default Kafka consumer configuration
   */
  static get CONSUMER_DEFAULTS() {
    const ttlValues = TtlConfig.getAllTtlValues();

    return {
      sessionTimeout: parseInt(process.env.MO_KAFKA_CONSUMER_SESSION_TIMEOUT) || ttlValues.TASK_PROCESSING_STATE_TTL,
      rebalanceTimeout:
        parseInt(process.env.MO_KAFKA_CONSUMER_REBALANCE_TIMEOUT) || ttlValues.TASK_PROCESSING_STATE_TTL * 5,
      heartbeatInterval:
        parseInt(process.env.MO_KAFKA_CONSUMER_HEARTBEAT_INTERVAL) || ttlValues.TASK_PROCESSING_STATE_TTL / 10,
      maxBytesPerPartition: parseInt(process.env.MO_KAFKA_CONSUMER_MAX_BYTES_PER_PARTITION) || 1048576,
      minBytes: parseInt(process.env.MO_KAFKA_CONSUMER_MIN_BYTES) || 1,
      maxBytes: parseInt(process.env.MO_KAFKA_CONSUMER_MAX_BYTES) || 10485760,
      maxWaitTimeInMs: parseInt(process.env.MO_KAFKA_CONSUMER_MAX_WAIT_TIME_MS) || KafkaTtlConfig.connectionTimeout * 5,
      fromBeginning: process.env.MO_KAFKA_CONSUMER_FROM_BEGINNING !== "false",
      maxInFlightRequests: parseInt(process.env.MO_KAFKA_CONSUMER_MAX_IN_FLIGHT_REQUESTS) || null,
      retry: {
        initialRetryTime: 300,
        retries: parseInt(process.env.MO_KAFKA_CONSUMER_RETRIES) || 5,
        factor: 0.2,
        multiplier: 2,
        maxRetryTime: 30000,
      },
    };
  }

  /**
   * Default Kafka producer configuration
   */
  static get PRODUCER_DEFAULTS() {
    return {
      createPartitioner: Partitioners.LegacyPartitioner,
      acks: parseInt(process.env.MO_KAFKA_PRODUCER_ACKS) || -1,
      timeout: parseInt(process.env.MO_KAFKA_PRODUCER_TIMEOUT) || KafkaTtlConfig.requestTimeout,
      compression: KafkaManager.getCompressionType(process.env.MO_KAFKA_COMPRESSION_TYPE) || CompressionTypes.None,
      idempotent: process.env.MO_KAFKA_PRODUCER_IDEMPOTENT === "true",
      maxInFlightRequests: parseInt(process.env.MO_KAFKA_PRODUCER_MAX_IN_FLIGHT_REQUESTS) || 5,
      transactionTimeout:
        parseInt(process.env.MO_KAFKA_PRODUCER_TRANSACTION_TIMEOUT) || KafkaTtlConfig.requestTimeout * 2,
      retries: parseInt(process.env.MO_KAFKA_PRODUCER_RETRIES) || 10,
      retry: {
        initialRetryTime: 300,
        retries: parseInt(process.env.MO_KAFKA_PRODUCER_RETRIES) || 5,
        factor: 0.2,
        multiplier: 2,
        maxRetryTime: 30000,
      },
    };
  }

  /**
   * Create a Kafka _client instance
   * @param {Object} clientOptions - Kafka _client options
   * @returns {Object} Kafka _client instance
   */
  static createClient(clientOptions = {}) {
    if (!clientOptions.brokers) {
      throw new Error(
        `Kafka client options must include 'brokers' array. Received: ${JSON.stringify(clientOptions)}. Example: { brokers: ['localhost:9092'] }`
      );
    }
    const client = new Kafka(clientOptions);
    logger.logDebug(
      ` Kafka client created: ${clientOptions.clientId || "unknown"} connecting to ${clientOptions.brokers.join(", ")}`
    );
    return client;
  }

  /**
   * Create a Kafka admin _client
   * @param {Object} client - Existing Kafka _client (optional)
   * @param {Object} clientOptions - Kafka _client options (used if no _client provided)
   * @returns {Object} Kafka admin _client
   */
  static createAdmin(client, clientOptions) {
    if (!client && !clientOptions) {
      throw new Error(`Can not create admin due no client or clientOptions defined yet.`);
    }
    const kafkaClient = client ?? this.createClient(clientOptions);
    return kafkaClient.admin();
  }

  /**
   * Create a Kafka producer instance
   * @param {Object} client - Existing Kafka _client (optional)
   * @param {Object} clientOptions - Kafka _client options (used if no _client provided)
   * @param {Object} producerOptions - Producer-specific options
   * @returns {Object} Kafka producer instance
   */
  static createProducer(client, clientOptions, producerOptions) {
    if (!client && !clientOptions) {
      throw new Error(`Can not create producer due no client or clientOptions defined yet.`);
    }
    const kafkaClient = client ?? KafkaManager.createClient(clientOptions);
    return kafkaClient.producer(producerOptions);
  }

  /**
   * Create a Kafka consumer instance
   * @param {Object} client - Existing Kafka _client (optional)
   * @param {Object} clientOptions - Kafka _client options (used if no _client provided)
   * @param {Object} consumerOptions - Consumer-specific options
   * @returns {Object} Kafka consumer instance
   */
  static createConsumer(client, clientOptions, consumerOptions) {
    if (!client && !clientOptions) {
      throw new Error(`Can not create consumer due no client or clientOptions defined yet.`);
    }
    const kafkaClient = client ?? KafkaManager.createClient(clientOptions);
    return kafkaClient.consumer(consumerOptions);
  }

  /**
   * Get compression type enum value from string
   * @param {string} type - Compression type name
   * @return {CompressionTypes} KafkaJS compression type
   */
  static getCompressionType(type) {
    const expectedType = type?.toLowerCase();
    switch (expectedType) {
      case "gzip":
        return CompressionTypes.GZIP;
      case "lz4":
        return CompressionTypes.LZ4;
      case "zstd":
        return CompressionTypes.ZSTD;
      default:
        return CompressionTypes.None;
    }
  }

  /**
   * @param {string} type
   * Generate unique sequence ID for messages
   * @returns {string} Unique sequence ID
   */
  static generateSequenceId(type) {
    return `${type ?? "SEQ"}_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
  }

  /**
   * Parse message content safely
   * @param {Buffer|string} messageValue - Raw message value
   * @returns {any} Parsed content or original string
   */
  static parseMessageValue(messageValue) {
    if (!messageValue) {
      return null;
    }
    try {
      const result = JSON.parse(messageValue.toString());
      logger.logDebug("Success parse message content from Kafka Broker");
      return result;
    } catch (error) {
      logger.logWarning("Failed to parse message content from Kafka Broker", error.message);
      return messageValue.toString();
    }
  }

  /**
   * Create message ID from Kafka message metadata
   * @param {string} topic - Topic name
   * @param {number} partition - Partition number
   * @param {string} offset - Message offset
   * @returns {string} Formatted message ID
   */
  static createMessageId(topic, partition, offset) {
    return `${topic}:${partition}:${offset}`;
  }

  /**
   * Convert headers to Kafka-compatible format (Buffers)
   * @param {Object} headers - Headers object
   * @returns {Object} Headers with Buffer values
   */
  static convertHeadersToBuffers(headers) {
    if (!headers || typeof headers !== "object") {
      return {};
    }

    const bufferedHeaders = {};
    for (const [key, value] of Object.entries(headers)) {
      if (value !== null && value !== undefined) {
        bufferedHeaders[key] = Buffer.isBuffer(value) ? value : Buffer.from(String(value));
      }
    }

    return bufferedHeaders;
  }

  static async createTopic(admin, topic, topicOptions) {
    try {
      const result = await admin.createTopics({
        topics: [
          {
            topic,
            numPartitions: topicOptions.partitions,
            replicationFactor: topicOptions.replicationFactor,
          },
        ],
      });
      logger.logDebug(
        `Topic ${topic} created ${result ? "successfully" : "failed"} with ${JSON.stringify(topicOptions, null, 2)}`
      );
      return result;
    } catch (error) {
      logger.logWarning(`🧤 Kafka topic [ ${topic} ] failed to create. Due ${error.message}`);
      return false;
    }
  }

  static async isTopicExisted(admin, topic) {
    try {
      const metadata = await admin.fetchTopicMetadata({
        topics: [topic],
      });
      return metadata.topics.some(x => x.name === topic);
    } catch (error) {
      logger.logWarning(`📢 Topic ${topic} not found: ${error.message}`);
      return false;
    }
  }

  /**
   * Merge Kafka configuration with smart defaults
   * @param {Object} userConfig - User provided configuration
   * @param {string} userConfig.topic - The Kafka topic name
   * @param {string} [userConfig.groupId] - The consumer group ID (required for consumers)
   * @param {Object} [userConfig.clientOptions] - Kafka client connection options
   * @param {Object} [userConfig.producerOptions] - Producer-specific options
   * @param {Object} [userConfig.consumerOptions] - Consumer-specific options
   * @param {boolean} [userConfig.useSuppression] - Whether to use message suppression
   * @param {boolean} [userConfig.useDistributedLock] - Whether to use distributed lock
   * @param {Object} [userConfig.cacheOptions] - Cache configuration options
   * @param {string} type - Component type ('consumer' or 'producer')
   * @returns {Object} Standardized configuration with all required options
   */
  static standardizeConfig(userConfig = {}, type) {
    if (!["consumer", "producer"].includes(type)) {
      throw new Error("Type must be either 'consumer' or 'producer'");
    }
    const {
      topicOptions = {},
      cacheOptions = {},
      clientOptions = {},
      consumerOptions = {},
      producerOptions = {},
    } = userConfig;

    const intendedClientId = `${userConfig.topic}-client-${new Date().getTime()}`;
    const mergedClientOptions = {
      ...this.CLIENT_DEFAULTS,
      clientId: intendedClientId,
      ...clientOptions,
      retry: {
        ...this.CLIENT_DEFAULTS.retry,
        ...(clientOptions.retry || {}),
      },
    };

    if (clientOptions.ssl !== undefined) {
      mergedClientOptions.ssl = clientOptions.ssl;
    }
    if (clientOptions.sasl !== undefined) {
      mergedClientOptions.sasl = clientOptions.sasl;
    }

    userConfig.groupId = consumerOptions.groupId ?? userConfig.groupId ?? `${userConfig.topic}-processors`;
    const standardizeConfig = {
      topic: userConfig.topic,
      topicOptions: {
        partitions: topicOptions?.partitions || parseInt(process.env.MO_KAFKA_TOPIC_PARTITIONS) || 5,
        replicationFactor:
          topicOptions?.replicationFactor || parseInt(process.env.MO_KAFKA_TOPIC_REPLICATION_FACTOR) || 1,
        allowAutoTopicCreation:
          topicOptions?.allowAutoTopicCreation ?? process.env.MO_KAFKA_TOPIC_AUTO_CREATION !== "false",
      },
      groupId: userConfig.groupId,
      cacheOptions: userConfig.cacheOptions ?? {},
      clientOptions: mergedClientOptions,
    };

    const keyPrefix = topicOptions?.keyPrefix || cacheOptions?.keyPrefix || `${userConfig.topic.toUpperCase()}`;
    const processingTtl =
      topicOptions?.processingTtl || cacheOptions?.processingTtl || TopicTtlConfig.processingTtl || 30000;
    const suppressionTtl = topicOptions?.suppressionTtl || cacheOptions?.suppressionTtl || processingTtl * 3 || 90000;
    if (processingTtl < 1000) {
      throw new Error(`Processing TTL must be greater than or equals 1000 ms`);
    }
    if (suppressionTtl <= processingTtl) {
      throw new Error(`Processing TTL must be less then Suppression TTL`);
    }
    standardizeConfig.topicOptions.keyPrefix = keyPrefix;
    standardizeConfig.topicOptions.processingTtl = processingTtl;
    standardizeConfig.topicOptions.suppressionTtl = suppressionTtl;

    // KEEP OBSOLETE CACHE CONFIGURATION FOR COMPATIBLE
    cacheOptions.keyPrefix = keyPrefix;
    cacheOptions.processingTtl = processingTtl;
    cacheOptions.suppressionTtl = suppressionTtl;
    standardizeConfig.cacheOptions = {
      ...cacheOptions,
    };

    if (type === "consumer") {
      standardizeConfig.clearSuppressionOnFailure =
        userConfig?.clearSuppressionOnFailure ??
        topicOptions?.clearSuppressionOnFailure ??
        process.env.MO_CLEAR_SUPPRESSION_ON_FAILURE === "true";
      standardizeConfig.maxConcurrency =
        parseInt(userConfig?.maxConcurrency) ||
        parseInt(topicOptions?.maxConcurrency) ||
        parseInt(process.env.MO_MAX_CONCURRENT_MESSAGES) ||
        1;
      standardizeConfig.eachBatchAutoResolve = process.env.MO_KAFKA_CONSUMER_EACH_BATCH_AUTO_RESOLVE !== "false";
      standardizeConfig.autoCommit = process.env.MO_KAFKA_CONSUMER_AUTO_COMMIT !== "false";
      standardizeConfig.autoCommitInterval = parseInt(process.env.MO_KAFKA_CONSUMER_AUTO_COMMIT_INTERVAL) || 5000;
      standardizeConfig.autoCommitThreshold = parseInt(process.env.MO_KAFKA_CONSUMER_AUTO_COMMIT_THRESHOLD) || 100;
      standardizeConfig.consumerOptions = {
        groupId: userConfig.groupId,
        ...this.CONSUMER_DEFAULTS,
        ...consumerOptions,
      };
    }

    if (type === "producer") {
      standardizeConfig.producerOptions = {
        ...this.PRODUCER_DEFAULTS,
        ...producerOptions,
      };
      standardizeConfig.lagThreshold =
        userConfig?.lagThreshold ||
        topicOptions?.lagThreshold ||
        parseInt(process.env.MO_KAFKA_PRODUCER_LAG_THRESHOLD) ||
        100;
      standardizeConfig.lagMonitorInterval =
        userConfig?.lagMonitorInterval ||
        topicOptions?.lagMonitorInterval ||
        parseInt(process.env.MO_KAFKA_PRODUCER_LAG_INTERVAL) ||
        5000;
      standardizeConfig.useSuppression =
        userConfig?.useSuppression ?? process.env.MO_KAFKA_PRODUCER_USE_SUPPRESSION !== "false";
      standardizeConfig.useDistributedLock =
        userConfig.useDistributedLock ?? process.env.MO_KAFKA_PRODUCER_DISTRIBUTED_LOCK !== "false";
    }

    return standardizeConfig;
  }

  /**
   * Calculate consumer lag for a topic and consumer group
   * @param {string} consumerGroup - Consumer group ID
   * @param {string} topic - Topic name
   * @param {Object} admin - Admin _client (optional, will create if not provided)
   * @returns {Promise<number>} Total lag across all partitions
   */
  static async calculateConsumerLag(consumerGroup, topic, admin = null) {
    if (!consumerGroup || !topic || !admin) {
      logger.logWarning("Consumer group or topic not specified for lag calculation");
      return 0;
    }
    try {
      const [latestOffsets, committedOffsets] = await Promise.all([
        this._getLatestOffsets(admin, topic),
        this._getCommittedOffsets(admin, consumerGroup, topic),
      ]);

      let totalLag = 0;

      for (const partition in latestOffsets) {
        const latestOffset = parseInt(latestOffsets[partition]);
        const committedOffset = parseInt(committedOffsets[partition] || "0");
        const partitionLag = Math.max(0, latestOffset - committedOffset);
        totalLag += partitionLag;
      }

      logger.logDebug(`Consumer lag for group '${consumerGroup}' on topic '${topic}': ${totalLag}`);

      return totalLag;
    } catch (error) {
      logger.logError(`Failed to calculate consumer lag for group '${consumerGroup}' on topic '${topic}'`, error);
      return 0;
    }
  }

  /**
   * Get latest offsets for all partitions of a topic
   * @param {Object} admin - Kafka admin _client
   * @param {string} topic - Topic name
   * @returns {Promise<Object>} Map of partition to latest offset
   */
  static async _getLatestOffsets(admin, topic) {
    try {
      const topicOffsets = await admin.fetchTopicOffsets(topic);
      const offsetMap = {};

      topicOffsets.forEach(partitionInfo => {
        offsetMap[partitionInfo.partition] = partitionInfo.high;
      });

      return offsetMap;
    } catch (error) {
      logger.logError(`Failed to fetch latest offsets for topic '${topic}'`, error);
      return {};
    }
  }

  /**
   * Get committed offsets for a consumer group on a topic
   * @param {Object} admin - Kafka admin _client
   * @param {string} consumerGroup - Consumer group ID
   * @param {string} topic - Topic name
   * @returns {Promise<Object>} Map of partition to committed offset
   */
  static async _getCommittedOffsets(admin, consumerGroup, topic) {
    try {
      const groupOffsets = await admin.fetchOffsets({
        groupId: consumerGroup,
        topics: [topic],
      });

      const offsetMap = {};

      groupOffsets.forEach(topicInfo => {
        if (topicInfo.topic === topic) {
          topicInfo.partitions.forEach(partitionInfo => {
            offsetMap[partitionInfo.partition] = partitionInfo.offset;
          });
        }
      });

      return offsetMap;
    } catch (error) {
      if (error.message?.includes("GroupIdNotFound") || error.type === "GROUP_ID_NOT_FOUND") {
        logger.logDebug(`Consumer group '${consumerGroup}' not found, assuming no committed offsets`);
        return {};
      }
      logger.logError(`Failed to fetch committed offsets for group '${consumerGroup}' on topic '${topic}'`, error);
      return {};
    }
  }

  /**
   * Creates multiple Kafka-formatted messages from input items
   * @param {Array<Object>} items - Array of message payloads/items to be converted
   * @param {string} type - Topic or message type identifier
   * @param {Object} [options={}] - Message creation options
   * @param {string|Function} [options.key] - Custom key or key generation function
   * @param {Object} [options.headers] - Custom headers to include in the message
   * @param {string} [options.timestamp] - Custom timestamp for the message
   * @param {number} [options.partition] - Specific partition to send the message to
   * @returns {Array<Object>} Array of Kafka formatted messages with key, value, headers, timestamp and partition
   */
  static createMessages(items, type, options = {}) {
    if (!Array.isArray(items) || items.length === 0) {
      throw new Error("Batch message requires non-empty items array");
    }

    if (!type) {
      throw new Error("Batch message requires a type");
    }

    return items.map(item => {
      let serializedValue;
      try {
        serializedValue = typeof item === "object" ? JSON.stringify(item) : String(item);
      } catch (error) {
        logger.logWarning(`Failed to stringify message: ${error.message}`);
        serializedValue = String(item);
      }

      return {
        key: KafkaManager.#generateMessageKey(item, type, options.key),
        value: serializedValue,
        headers: KafkaManager.#prepareMessageHeaders(item, options.headers),
        timestamp: options.timestamp || Date.now().toString(),
        partition: options.partition,
      };
    });
  }

  /**
   * Generate a consistent message key
   * @param {Object} data - Message payload
   * @param {string} type - Message type
   * @param {string|Function} keyOverride - Optional key override
   * @returns {string} Message key
   */
  static #generateMessageKey(data, type, keyOverride) {
    const delta = new Date().getTime();
    if (keyOverride) {
      let key;
      if (typeof keyOverride === "function") {
        key = keyOverride(data);
      } else {
        key = String(keyOverride);
      }
      return type ? `${type}-${key}-${delta}` : `${key}-${delta}`;
    }
    const itemId = data?._id ?? data?._getId() ?? data?.id ?? data?.getId();
    if (type && itemId) {
      return `${type?.toUpperCase()}-${itemId}-${delta}`;
    }
    return KafkaManager.generateSequenceId(type);
  }

  /**
   * Prepare message headers in Kafka-compatible format
   * @param {Object} data - Message data
   * @param {Object} headersOverride - Optional headers override
   * @returns {Object} Headers object with Buffer values
   */
  static #prepareMessageHeaders(data, headersOverride = {}) {
    const defaultHeaders = {
      messageId: crypto.randomUUID(),
      timestamp: Date.now().toString(),
      contentType: "application/json",
    };

    if (data && data.type) {
      defaultHeaders.messageType = data.type;
    }

    const combinedHeaders = {
      ...defaultHeaders,
      ...headersOverride,
    };
    return KafkaManager.convertHeadersToBuffers(combinedHeaders);
  }
}

module.exports = KafkaManager;
