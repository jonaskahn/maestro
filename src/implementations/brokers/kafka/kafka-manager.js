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
const logger = require("../../../services/logger-service");
const TTLConfig = require("../../../config/ttl-config");

// Load Kafka-specific TTL values
const KAFKA_TTL_CONFIG = TTLConfig.getKafkaConfig();

class KafkaManager {
  /**
   * ========================================
   * CONFIGURATION DEFAULTS
   * ========================================
   */

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
   * Default Kafka client configuration
   */
  static get CLIENT_DEFAULTS() {
    return {
      clientId: process.env.JO_KAFKA_CLIENT_ID || `job-orchestrator-${new Date().getTime()}`,
      brokers: process.env.JO_KAFKA_BROKERS?.split(",") || ["localhost:9092"],
      connectionTimeout: KAFKA_TTL_CONFIG.connectionTimeout,
      requestTimeout: KAFKA_TTL_CONFIG.requestTimeout,
      enforceRequestTimeout: process.env.JO_KAFKA_ENFORCE_REQUEST_TIMEOUT !== "false",
      retry: {
        initialRetryTime: parseInt(process.env.JO_KAFKA_INITIAL_RETRY_TIME_MS) || 100,
        retries: parseInt(process.env.JO_KAFKA_RETRY_COUNT) || 10,
        factor: 0.2,
        multiplier: 2,
        maxRetryTime: parseInt(process.env.JO_KAFKA_MAX_RETRY_TIME_MS) || 30000,
      },
    };
  }

  /**
   * Default Kafka consumer configuration
   */
  static get CONSUMER_DEFAULTS() {
    const ttlValues = TTLConfig.getAllTTLValues();

    return {
      sessionTimeout: parseInt(process.env.JO_KAFKA_CONSUMER_SESSION_TIMEOUT) || ttlValues.TASK_PROCESSING_STATE_TTL,
      rebalanceTimeout:
        parseInt(process.env.JO_KAFKA_CONSUMER_REBALANCE_TIMEOUT) || ttlValues.TASK_PROCESSING_STATE_TTL * 2,
      heartbeatInterval:
        parseInt(process.env.JO_KAFKA_CONSUMER_HEARTBEAT_INTERVAL) || ttlValues.TASK_PROCESSING_STATE_TTL / 10,
      maxBytesPerPartition: parseInt(process.env.JO_KAFKA_CONSUMER_MAX_BYTES_PER_PARTITION) || 1048576,
      minBytes: parseInt(process.env.JO_KAFKA_CONSUMER_MIN_BYTES) || 1,
      maxBytes: parseInt(process.env.JO_KAFKA_CONSUMER_MAX_BYTES) || 10485760,
      maxWaitTimeInMs:
        parseInt(process.env.JO_KAFKA_CONSUMER_MAX_WAIT_TIME_MS) || KAFKA_TTL_CONFIG.connectionTimeout * 5,
      allowAutoTopicCreation: process.env.JO_KAFKA_ALLOW_AUTO_TOPIC_CREATION !== "false",
      autoCommit: process.env.JO_KAFKA_CONSUMER_AUTO_COMMIT === "true",
      autoCommitInterval: parseInt(process.env.JO_KAFKA_CONSUMER_AUTO_COMMIT_INTERVAL) || 5000,
      fromBeginning: process.env.JO_KAFKA_CONSUMER_FROM_BEGINNING !== "false",
      maxInFlightRequests: parseInt(process.env.JO_KAFKA_CONSUMER_MAX_IN_FLIGHT_REQUESTS) || 1,
    };
  }

  /**
   * Default Kafka producer configuration
   */
  static get PRODUCER_DEFAULTS() {
    return {
      createPartitioner: Partitioners.LegacyPartitioner,
      acks: parseInt(process.env.JO_KAFKA_PRODUCER_ACKS) || -1,
      timeout: parseInt(process.env.JO_KAFKA_PRODUCER_TIMEOUT) || KAFKA_TTL_CONFIG.requestTimeout,
      compression: KafkaManager.getCompressionType(process.env.JO_KAFKA_COMPRESSION_TYPE) || CompressionTypes.None,
      idempotent: process.env.JO_KAFKA_PRODUCER_IDEMPOTENT === "true",
      maxInFlightRequests: parseInt(process.env.JO_KAFKA_PRODUCER_MAX_IN_FLIGHT_REQUESTS) || 5,
      allowAutoTopicCreation: process.env.JO_KAFKA_ALLOW_AUTO_TOPIC_CREATION !== "false",
      transactionTimeout:
        parseInt(process.env.JO_KAFKA_PRODUCER_TRANSACTION_TIMEOUT) || KAFKA_TTL_CONFIG.requestTimeout * 2,
      retries: parseInt(process.env.JO_KAFKA_PRODUCER_RETRIES) || 10,
      retry: {
        initialRetryTime: 300,
        retries: parseInt(process.env.JO_KAFKA_PRODUCER_RETRIES) || 10,
        factor: 0.2,
        multiplier: 2,
        maxRetryTime: 30000,
      },
    };
  }

  /**
   * ========================================
   * CLIENT CREATION METHODS
   * ========================================
   */

  /**
   * Create a Kafka client instance
   * @param {Object} clientOptions - Kafka client options
   * @returns {Object} Kafka client instance
   */
  static createClient(clientOptions = {}) {
    if (!clientOptions.brokers) {
      throw new Error(
        `Kafka client options must include 'brokers' array. Received: ${JSON.stringify(clientOptions)}. Example: { brokers: ['localhost:9092'] }`
      );
    }
    const client = new Kafka(clientOptions);
    logger.logDebug(
      `📡 Kafka client created: ${clientOptions.clientId || "unknown"} connecting to ${clientOptions.brokers.join(", ")}`
    );
    return client;
  }

  /**
   * Create a Kafka admin client
   * @param {Object} client - Existing Kafka client (optional)
   * @param {Object} clientOptions - Kafka client options (used if no client provided)
   * @returns {Object} Kafka admin client
   */
  static createAdmin(client, clientOptions) {
    if (!client && !clientOptions) {
      throw new Error(`‼️ Can not create admin due no client or clientOptions defined yet.`);
    }
    const kafkaClient = client ?? this.createClient(clientOptions);
    return kafkaClient.admin();
  }

  /**
   * Create a Kafka producer instance
   * @param {Object} client - Existing Kafka client (optional)
   * @param {Object} clientOptions - Kafka client options (used if no client provided)
   * @param {Object} producerOptions - Producer-specific options
   * @returns {Object} Kafka producer instance
   */
  static createProducer(client, clientOptions, producerOptions) {
    if (!client && !clientOptions) {
      throw new Error(`‼️ Can not create producer due no client or clientOptions defined yet.`);
    }
    const kafkaClient = client ?? KafkaManager.createClient(clientOptions);
    return kafkaClient.producer(producerOptions);
  }

  /**
   * Create a Kafka consumer instance
   * @param {Object} client - Existing Kafka client (optional)
   * @param {Object} clientOptions - Kafka client options (used if no client provided)
   * @param {Object} consumerOptions - Consumer-specific options
   * @returns {Object} Kafka consumer instance
   */
  static createConsumer(client, clientOptions, consumerOptions) {
    if (!client && !clientOptions) {
      throw new Error(`‼️ Can not create consumer due no client or clientOptions defined yet.`);
    }
    const kafkaClient = client ?? KafkaManager.createClient(clientOptions);
    return kafkaClient.consumer(consumerOptions);
  }

  /**
   * ========================================
   * CONFIGURATION MANAGEMENT
   * ========================================
   */

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
   * Enriches configuration with cache instance if cacheOptions are provided but no cacheInstance exists
   * @param {Object} config - The broker configuration
   * @returns {Object} Enhanced configuration with cache instance
   */
  static enrichConfig(config) {
    config.brokerOptions = config.brokerOptions || {};
    config.brokerOptions.clientOptions = config.brokerOptions.clientOptions || {};
    config.brokerOptions.consumerOptions = config.brokerOptions.consumerOptions || {};
    config.brokerOptions.producerOptions = config.brokerOptions.producerOptions || {};

    if (!config.groupId && config.topic) {
      config.groupId = `${config.topic}-consumer-group-processors`;
    }

    if (!config.brokerOptions.clientOptions.clientId) {
      config.brokerOptions.clientOptions.clientId = `${config.topic}.client.${new Date().getTime()}`;
    }
    return config;
  }

  /**
   * Merge Kafka configuration with smart defaults
   * @param {Object} userConfig - User provided configuration
   * @param {string} userConfig.topic - The Kafka topic name
   * @param {string} [userConfig.groupId] - The consumer group ID (required for consumers)
   * @param {Object} [userConfig.brokerOptions] - Broker configuration options
   * @param {Object} [userConfig.brokerOptions.clientOptions] - Kafka client connection options
   * @param {Object} [userConfig.brokerOptions.producerOptions] - Producer-specific options
   * @param {Object} [userConfig.brokerOptions.consumerOptions] - Consumer-specific options
   * @param {boolean} [userConfig.useSuppression] - Whether to use message suppression
   * @param {boolean} [userConfig.useDistributedLock] - Whether to use distributed lock
   * @param {Object} [userConfig.cacheOptions] - Cache configuration options
   * @param {string} type - Component type ('consumer' or 'producer')
   * @returns {Object} Standardized configuration with all required options
   */
  static standardizeConfig(userConfig = {}, type) {
    const { brokerOptions = {} } = userConfig;
    const { clientOptions = {}, consumerOptions = {}, producerOptions = {} } = brokerOptions;

    const mergedClientOptions = {
      ...this.CLIENT_DEFAULTS,
      ...clientOptions,
      retry: {
        ...this.CLIENT_DEFAULTS.retry,
        ...(clientOptions.retry || {}),
      },
    };

    mergedClientOptions.clientId = clientOptions.clientId ?? `${userConfig.topic}-client-${new Date().getTime()}`;
    if (clientOptions.ssl !== undefined) {
      mergedClientOptions.ssl = clientOptions.ssl;
    }
    if (clientOptions.sasl !== undefined) {
      mergedClientOptions.sasl = clientOptions.sasl;
    }

    userConfig.groupId = consumerOptions.groupId ?? userConfig.groupId ?? `${userConfig.topic}-processors`;
    const standardizeConfig = {
      topic: userConfig.topic,
      groupId: userConfig.groupId,
      cacheOptions: userConfig.cacheOptions ?? {},
      clientOptions: mergedClientOptions,
    };

    if (type === "consumer") {
      standardizeConfig.maxConcurrency =
        userConfig.maxConcurrency || parseInt(process.env.JO_MAX_CONCURRENT_MESSAGES) || 1;
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
      standardizeConfig.maxLag = userConfig.maxLag;
      standardizeConfig.useSuppression = userConfig.useSuppression !== "false";
      standardizeConfig.useDistributedLock = userConfig.useDistributedLock !== "false";
    }

    return standardizeConfig;
  }

  /**
   * ========================================
   * MESSAGE HANDLING UTILITIES
   * ========================================
   */

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
      logger.logDebug("☑️ Success parse message content from Kafka Broker");
      return result;
    } catch (error) {
      logger.logWarning("⚠️ Failed to message content from Kafka Broker", error.message);
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

  /**
   * Generate unique sequence ID for messages
   * @returns {string} Unique sequence ID
   */
  static generateSequenceId() {
    return `seq_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
  }

  /**
   * ========================================
   * MONITORING & PERFORMANCE
   * ========================================
   */

  /**
   * Calculate consumer lag for a topic and consumer group
   * @param {string} consumerGroup - Consumer group ID
   * @param {string} topic - Topic name
   * @param {Object} admin - Admin client (optional, will create if not provided)
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

      logger.logDebug(`📊 Consumer lag for group '${consumerGroup}' on topic '${topic}': ${totalLag}`);

      return totalLag;
    } catch (error) {
      logger.logError(`Failed to calculate consumer lag for group '${consumerGroup}' on topic '${topic}'`, error);
      return 0;
    }
  }

  /**
   * Get latest offsets for all partitions of a topic
   * @param {Object} admin - Kafka admin client
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
   * @param {Object} admin - Kafka admin client
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
        key: KafkaManager.#generateMessageKey(item, options.key),
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
   * @param {string|Function} keyOverride - Optional key override
   * @returns {string} Message key
   */
  static #generateMessageKey(data, keyOverride) {
    if (keyOverride) {
      if (typeof keyOverride === "function") {
        return keyOverride(data);
      }
      return String(keyOverride);
    }

    if (data && data.id) {
      return String(data.id);
    }
    return KafkaManager.generateSequenceId();
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
