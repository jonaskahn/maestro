/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Abstract Consumer Base Class for message broker implementations
 *
 * Provides unified interface for message consumption across Kafka, RabbitMQ, BullMQ with
 * standardized message processing, concurrency management, business logic handling, and metrics tracking.
 * This class must be extended by specific broker implementations.
 */
const logger = require("../services/logger-service");

const DEFAULT_VALUES = {
  MAX_CONCURRENCY: 1,
  STATUS_REPORT_INTERVAL_MS: 30000,
};

const ENV_KEYS = {
  MAX_CONCURRENT_MESSAGES: ["MO_MAX_CONCURRENT_MESSAGES"],
  STATUS_REPORT_INTERVAL: ["MO_STATUS_REPORT_INTERVAL"],
};

const CONSUMER_STATES = {
  CONNECTED: true,
  DISCONNECTED: false,
  CONSUMING: true,
  NOT_CONSUMING: false,
};

const METRICS_PROPERTIES = {
  TOTAL_PROCESSED: "totalProcessed",
  TOTAL_FAILED: "totalFailed",
  START_TIME: "startTime",
};

/**
 * Abstract Consumer Base Class
 *
 * Provides unified interface for message consumption across different message brokers
 * with standardized message processing, concurrency management, business logic handling,
 * and metrics tracking.
 */
class AbstractConsumer {
  _cacheLayer;

  _isConnected;
  _isConsuming;
  _isShuttingDown;

  _statusReportInterval;
  _statusReportTimer;

  /**
   * Creates a consumer instance with configuration validation and initialization
   *
   * @param {Object} config - Consumer configuration object
   * @param {string} config.topic - Topic name to consume messages from
   * @param {string} [config.topicOptions] - Topic options for the consumer
   * @param {number} [config.maxConcurrency] - Maximum number of concurrent messages to process
   * @param {number} [config.statusReportInterval] - Interval for status reporting in ms
   * @param {Object} [config.cacheOptions] - Configuration for the cache layer
   * @param {string} config.cacheOptions.keyPrefix - Required prefix for cache keys
   */
  constructor(config) {
    this.#preventDirectInstantiation();
    this.#validateConfiguration(config);
    this.#initializeConfiguration(config);
    this.#initializeDependencies(config);
    this.#initializeState();
    this.#setupGracefulShutdown();
    this._logConfigurationLoaded();
  }

  #preventDirectInstantiation() {
    if (this.constructor === AbstractConsumer) {
      throw new Error("AbstractConsumer cannot be instantiated directly");
    }
  }

  #validateConfiguration(config) {
    this.#validateConfigurationStructure(config);
    this.#validateTopicConfiguration(config);
    this.#validateCacheOptions(config);
  }

  #validateConfigurationStructure(config) {
    if (!config || typeof config !== "object") {
      throw new Error("Consumer configuration must be an object");
    }
  }

  #validateTopicConfiguration(config) {
    if (!config.topic || typeof config.topic !== "string" || config.topic?.trim() === "") {
      throw new Error("Consumer configuration must include a topic string");
    }
  }

  #validateCacheOptions(config) {
    if (config.cacheOptions) {
      if (typeof config.cacheOptions !== "object") {
        throw new Error("Cache options must be an object");
      }

      if (!config.cacheOptions.keyPrefix) {
        throw new Error("Cache options must include keyPrefix");
      }
    }
  }

  #initializeConfiguration(config) {
    this.#setBasicConfiguration(config);
    this.#initializeMetrics();
    this.#setStatusReporting(config);
  }

  #setBasicConfiguration(config) {
    this._topic = config.topic;
    this._config = config;
    this._clearSuppressionOnFailed = config.clearSuppressionOnFailed ?? false;
    this.maxConcurrency =
      config.maxConcurrency ||
      parseInt(this.#getEnvironmentValue(ENV_KEYS.MAX_CONCURRENT_MESSAGES)) ||
      DEFAULT_VALUES.MAX_CONCURRENCY;
  }

  #getEnvironmentValue(keys) {
    for (const key of keys) {
      const value = process.env[key];
      if (value !== undefined) {
        return value;
      }
    }
    return null;
  }

  #initializeMetrics() {
    this.metrics = {
      [METRICS_PROPERTIES.TOTAL_PROCESSED]: 0,
      [METRICS_PROPERTIES.TOTAL_FAILED]: 0,
      [METRICS_PROPERTIES.START_TIME]: Date.now(),
    };
  }

  #setStatusReporting(config) {
    this._statusReportInterval = this.#resolveStatusReportInterval(config);
  }

  #resolveStatusReportInterval(config) {
    return (
      config.statusReportInterval ||
      parseInt(this.#getEnvironmentValue(ENV_KEYS.STATUS_REPORT_INTERVAL)) ||
      DEFAULT_VALUES.STATUS_REPORT_INTERVAL_MS
    );
  }

  #initializeDependencies(config) {
    this._cacheLayer = this._createCacheLayer(config?.cacheOptions);
    this._isShuttingDown = false;
  }

  /**
   * Creates a cache layer for message deduplication and state tracking
   * Override to implement specific cache layer creation
   *
   * @param {Object} _config - Cache configuration
   * @returns {Object|null} Cache layer instance or null if disabled
   */
  _createCacheLayer(_config) {
    logger.logDebug("Cache layer is disabled");
    return null;
  }

  #initializeState() {
    this._isConnected = CONSUMER_STATES.DISCONNECTED;
    this._isConsuming = CONSUMER_STATES.NOT_CONSUMING;
    this._statusReportTimer = null;
    this._isShuttingDown = false;
  }

  #setupGracefulShutdown() {
    process.on("SIGINT", this.#handleGracefulShutdownConsumer.bind(this, "SIGINT"));
    process.on("SIGTERM", this.#handleGracefulShutdownConsumer.bind(this, "SIGTERM"));
  }

  async #handleGracefulShutdownConsumer(signal = "unknown") {
    if (this._isShuttingDown) {
      return;
    }

    try {
      this._isShuttingDown = true;
      logger.logInfo(`${this.getBrokerType()} consumer received ${signal} signal, shutting down gracefully`);
      if (this.#isCurrentlyConsuming()) {
        logger.logInfo(`Stopping message consumption`);
        await this._stopConsumingFromBroker().catch(error => {
          logger.logWarning("Error stopping consumption during shutdown", error);
        });
      }

      if (this.#isCurrentlyConnected()) {
        await this.#performDisconnection().catch(error => {
          logger.logWarning("Error during disconnection in shutdown", error);
        });
      }

      this._removeShutdownListeners();

      logger.logInfo(`${this.getBrokerType()} consumer shutdown complete`);
    } catch (error) {
      logger.logError(`Error during graceful shutdown of ${this.getBrokerType()} consumer`, error);
    } finally {
      if (signal !== "uncaughtException" && signal !== "unhandledRejection") {
        if (typeof process.exit === "function") {
          process.exit(0);
        }
      }
    }
  }

  /**
   * Removes shutdown event listeners
   */
  _removeShutdownListeners() {
    process.removeListener("SIGINT", this.#handleGracefulShutdownConsumer);
    process.removeListener("SIGTERM", this.#handleGracefulShutdownConsumer);
    process.removeListener("uncaughtException", this.#handleGracefulShutdownConsumer);
    process.removeListener("unhandledRejection", this.#handleGracefulShutdownConsumer);
  }

  #isCurrentlyConsuming() {
    return this._isConsuming === CONSUMER_STATES.CONSUMING;
  }

  #isCurrentlyConnected() {
    return this._isConnected === CONSUMER_STATES.CONNECTED;
  }

  async #performDisconnection() {
    await this.#stopConsumingIfActive();
    await this._disconnectFromMessageBroker();
    await this.#disconnectFromCache();
  }

  async #stopConsumingIfActive() {
    if (this._isConsuming === CONSUMER_STATES.CONSUMING) {
      await this.stopConsuming();
    }
  }

  /**
   * Logs the configuration loaded for the consumer
   */
  _logConfigurationLoaded() {
    logger.logDebug(
      `${this.getBrokerType()?.toUpperCase()} Consumer loaded with configuration ${JSON.stringify(this._config, null, 2)}`
    );
  }

  /**
   * Returns the broker type identifier
   * @returns {string} Broker type ('kafka', 'rabbitmq', 'bullmq')
   * @throws {Error} When method is not implemented by subclass
   */
  getBrokerType() {
    throw new Error("getBrokerType method must be implemented by subclass");
  }

  /**
   * Establishes connection to message broker and cache layer
   * @returns {Promise<void>}
   * @throws {Error} When connection fails
   */
  async connect() {
    if (this.#isAlreadyConnected()) {
      logger.logInfo(`${this.getBrokerType()} consumer is already connected`);
      return;
    }
    try {
      await this.#establishConnection();
      await this._createTopicIfAllowed();
      this.#markAsConnected();
      logger.logInfo(`${this.getBrokerType()?.toUpperCase()} consumer is connected to topic [ ${this._topic} ]`);
    } catch (error) {
      this.#handleConnectionError(error);
      throw error;
    }
  }

  #isAlreadyConnected() {
    return this._isConnected === CONSUMER_STATES.CONNECTED;
  }

  async #establishConnection() {
    await this.#connectToCache();
    await this._connectToMessageBroker();
  }

  async #connectToCache() {
    if (!this._cacheLayer) {
      return;
    }

    try {
      await this._cacheLayer.connect();
      logger.logDebug(
        `${this.getBrokerType()?.toUpperCase()} consumer for topic ${this._topic} connected to cache layer`
      );
    } catch (error) {
      logger.logWarning("Failed to connect to cache layer, continuing without cache", error);
    }
  }

  /**
   * Implementation-specific method to connect to message broker
   * @returns {Promise<void>}
   */
  async _connectToMessageBroker() {
    throw new Error("_connectToMessageBroker method must be implemented by subclass");
  }

  /**
   * Creates topic if allowed by broker and configuration
   * Override to implement topic creation logic
   * @returns {Promise<boolean>} True if topic was created or already exists
   */
  async _createTopicIfAllowed() {
    logger.logWarning(
      `You see this log because you do not implemented _createTopicIfAllowed in Consumer. But it's safe to ignore`
    );
  }

  #markAsConnected() {
    this._isConnected = CONSUMER_STATES.CONNECTED;
  }

  #handleConnectionError(error) {
    this._isConnected = CONSUMER_STATES.DISCONNECTED;
    logger.logError(`Failed to connect ${this.getBrokerType()} consumer`, error);
  }

  /**
   * Disconnects from message broker and cache layer
   * @returns {Promise<void>}
   * @throws {Error} When disconnection fails
   */
  async disconnect() {
    if (!this.#isCurrentlyConnected()) {
      logger.logWarning(`${this.getBrokerType()} consumer is already disconnected`);
      return;
    }

    try {
      await this.#performDisconnection();
      this.#markAsDisconnected();
      logger.logInfo(`${this.getBrokerType()} consumer disconnected successfully`);
    } catch (error) {
      this.#handleDisconnectionError(error);
      throw error;
    }
  }

  #markAsDisconnected() {
    this._isConnected = CONSUMER_STATES.DISCONNECTED;
  }

  #handleDisconnectionError(error) {
    logger.logError(`Error disconnecting ${this.getBrokerType()} consumer`, error);
    this._isConsuming = CONSUMER_STATES.NOT_CONSUMING;
    this._isConnected = CONSUMER_STATES.DISCONNECTED;
  }

  /**
   * Implementation-specific method to disconnect from message broker
   * @returns {Promise<void>}
   */
  async _disconnectFromMessageBroker() {
    throw new Error("_disconnectFromMessageBroker method must be implemented by subclass");
  }

  async #disconnectFromCache() {
    if (!this._cacheLayer) {
      return;
    }

    try {
      await this._cacheLayer.disconnect();
      logger.logInfo(`${this.getBrokerType()} consumer disconnected from cache layer`);
    } catch (error) {
      logger.logWarning("Error disconnecting from cache layer", error);
    }
  }

  /**
   * Starts consuming messages from the broker
   * @param {Object} options - Consumption options
   * @returns {Promise<void>}
   * @throws {Error} When consumption fails to start
   */
  async consume(options = {}) {
    this.#ensureConnected();

    if (this._isConsuming === CONSUMER_STATES.CONSUMING) {
      logger.logWarning(`${this.getBrokerType()} consumer is already consuming messages`);
      return;
    }

    try {
      await this._startConsumingFromBroker(options);
      this._isConsuming = CONSUMER_STATES.CONSUMING;
      logger.logDebug(`${this.getBrokerType()} consumer started consuming for topic [ ${this._topic} ]`);

      if (this._statusReportInterval > 0) {
        this.#startStatusReporting();
      }
    } catch (error) {
      this._handleConsumingStartError(error);
      throw error;
    }
  }

  #ensureConnected() {
    if (this._isShuttingDown) {
      throw new Error(`${this.getBrokerType()} consumer is shutting down`);
    }

    if (!this.#isCurrentlyConnected()) {
      throw new Error(`${this.getBrokerType()} consumer is not connected`);
    }
  }

  /**
   * Implementation-specific method to start consuming from broker
   * @param {Object} _handler - Message handler function
   * @param {Object} _options - Consumption options
   * @returns {Promise<void>}
   */
  async _startConsumingFromBroker(_handler, _options) {
    throw new Error("_startConsumingFromBroker method must be implemented by subclass");
  }

  #startStatusReporting() {
    if (this._statusReportTimer) {
      clearInterval(this._statusReportTimer);
    }

    this._statusReportTimer = setInterval(() => {
      const status = this.getConfigStatus();
      logger.logInfo(
        `${this.getBrokerType()} consumer status for topic [${this._topic}]: processed=${status.processedCount}, failed=${status.failedCount}, active=0`
      );
    }, this._statusReportInterval);
  }

  /**
   * Handles errors during consumption start
   * @param {Error} error - Error that occurred
   */
  _handleConsumingStartError(error) {
    this._isConsuming = CONSUMER_STATES.NOT_CONSUMING;
    logger.logError(`Failed to start consuming from ${this._topic}`, error);
  }

  /**
   * Stops consuming messages from the broker
   * @returns {Promise<void>}
   */
  async stopConsuming() {
    if (!this.#isCurrentlyConsuming()) {
      logger.logWarning(`${this.getBrokerType()} consumer is not currently consuming`);
      return;
    }

    try {
      await this._stopConsumingFromBroker();
      this.#markAsNotConsuming();
      logger.logInfo(`${this.getBrokerType()} consumer stopped consuming from ${this._topic}`);

      if (this._statusReportTimer) {
        clearInterval(this._statusReportTimer);
        this._statusReportTimer = null;
      }
    } catch (error) {
      logger.logError(`Error stopping consumption from ${this._topic}`, error);
    }
  }

  /**
   * Implementation-specific method to stop consuming from broker
   * @returns {Promise<void>}
   */
  async _stopConsumingFromBroker() {
    throw new Error("_stopConsumingFromBroker method must be implemented by subclass");
  }

  #markAsNotConsuming() {
    this._isConsuming = CONSUMER_STATES.NOT_CONSUMING;
  }

  /**
   * Processes a message with business logic
   * Must be implemented by user to define business processing logic
   * @param {Object} _item - Message data to process
   * @returns {Promise<void>}
   */
  async process(_item) {
    throw new Error("process method must be implemented by user");
  }

  /**
   * Extracts item ID from message data
   * Override to implement custom ID extraction
   * @param {Object} _item - Extracted message data
   * @returns {string} Item identifier
   */
  getItemId(_item) {
    throw new Error("getItemId must be implemented");
  }

  /**
   * Gets message key for cache operations
   * @param {Object} item - Extracted message data
   * @returns {string} Message key
   */
  getMessageKey(item) {
    return this.getItemId(item);
  }

  /**
   * Default business message handling flow
   * @param {string} type - Message type
   * @param {string} messageId - Message ID from broker
   * @param {Object} item - Message data
   * @returns {Promise<void>}
   */
  async _defaultBusinessHandler(type, messageId, item) {
    const startTime = Date.now();
    const itemId = this.getItemId(item);
    const messageKey = this.getMessageKey(item);

    try {
      logger.logDebug(`Processing message: ${itemId} (key: ${messageKey})`);
      if (await this.#isMessageAlreadyCompleted(itemId)) {
        logger.logWarning(`Skipping already completed message in topic [ ${this._topic} ]: ${itemId} (${messageId})`);
        return;
      }

      const result = await this.#markAsProcessingStart(itemId);
      if (!result) {
        logger.logWarning(
          `Another ${this.getBrokerType()} consumer is processing for this item ${itemId} in topic [ ${this._topic} ]`
        );
        return;
      }

      const processingTtl = this._config?.topicOptions?.processingTtl;
      const suppressionTtl = this._config?.topicOptions?.suppressionTtl;

      if (this._cacheLayer && processingTtl && suppressionTtl && suppressionTtl > processingTtl) {
        const processPromise = this.process(item);
        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(() => {
            reject(new Error(`Processing time reached ${processingTtl}ms, attempting TTL extension`));
          }, processingTtl - 300);
        });
        await Promise.race([processPromise, timeoutPromise]).catch(async error => {
          if (error.message.includes("attempting TTL extension")) {
            logger.logDebug(`Processing time limit reached for ${itemId}, extending TTL`);
            const extensionTtl = suppressionTtl - processingTtl;
            await this._cacheLayer.extendProcessingTtl(itemId, extensionTtl);
            logger.logDebug(`Extended processing TTL for ${itemId} by ${extensionTtl}ms`);
            const finalTimeoutPromise = new Promise((_, reject) => {
              setTimeout(() => {
                reject(new Error(`Extended processing time reached ${extensionTtl}ms, marking as failed`));
              }, extensionTtl);
            });
            await Promise.race([processPromise, finalTimeoutPromise]);
          } else {
            throw error;
          }
        });
        await this.#afterProcessSuccess(itemId, messageKey, startTime);
      } else {
        await this.process(item);
        await this.#afterProcessSuccess(itemId, messageKey, startTime);
      }
    } catch (error) {
      await this.#handleProcessingFailure(itemId, messageKey, error);
    } finally {
      await this.#markAsProcessingEnd(itemId);
    }
  }

  async #isMessageAlreadyCompleted(itemId) {
    try {
      return await this._isItemProcessed(itemId);
    } catch (error) {
      logger.logWarning(`Failed to check if message ${itemId} is already completed`, error);
      return false;
    }
  }

  /**
   * Checks if an item is already completed in the cache
   * Override to implement specific completion check
   * @param {string} _itemId - Item identifier
   * @returns {Promise<boolean>} True if item is completed
   */
  async _isItemProcessed(_itemId) {
    throw new Error("_isItemProcessed must be implemented by subclass");
  }

  async #markAsProcessingStart(itemId) {
    try {
      if (!this._cacheLayer) {
        return false;
      }
      return await this._cacheLayer?.markAsProcessing(itemId);
    } catch (error) {
      logger.logWarning(`Failed to mark item ${itemId} as processing start`, error);
      return false;
    }
  }

  async #afterProcessSuccess(itemId, messageKey, startTime) {
    await this._onItemProcessSuccess(itemId);
    this.metrics[METRICS_PROPERTIES.TOTAL_PROCESSED]++;
    const duration = Date.now() - startTime;
    logger.logInfo(
      `Successfully processed message in topic [${this._topic}] for ${itemId} (key: ${messageKey}, duration: ${duration}ms)`
    );
  }

  /**
   * Called when item processing is successful
   * Override to implement custom success handling
   * @param {string} _itemId - Item identifier
   * @returns {Promise<void>}
   */
  async _onItemProcessSuccess(_itemId) {
    throw new Error("_markItemAsCompleted method must be implemented by subclass");
  }

  async #handleProcessingFailure(itemId, messageKey, error) {
    try {
      await this.#clearSuppressionOnFailure(itemId);
      await this._onItemProcessFailed(itemId, error);
      this.metrics[METRICS_PROPERTIES.TOTAL_FAILED]++;
      logger.logWarning(
        `Failed to consume message on topic [${this._topic}] : ${itemId} (Message Key: ${messageKey}), but error will be ignored to throw. Due ${error ? `${error?.message}` : null}`
      );
    } catch (error) {
      logger.logWarning(
        `Failed to process failed message: ${itemId} (Key: ${messageKey}), but error will be ignored to throw [${error?.message}]`
      );
    }
  }

  async #clearSuppressionOnFailure(itemId) {
    if (!this._clearSuppressionOnFailed) {
      return;
    }
    try {
      await this._cacheLayer.clearProcessingState(itemId);
    } catch (error) {
      logger.logWarning(`Failed to clear processing state ${itemId}: ${error?.message}`);
    }
  }

  /**
   * Called when item processing fails
   * Override to implement custom failure handling
   * @param {string} _itemId - Item identifier
   * @param {Error} _error - Error that occurred
   * @returns {Promise<void>}
   */
  async _onItemProcessFailed(_itemId, _error) {
    throw new Error("_onItemProcessFailed method must be implemented by subclass");
  }

  async #markAsProcessingEnd(itemId) {
    if (!this._cacheLayer) {
      return;
    }
    try {
      return await this._cacheLayer?.clearProcessingState(itemId);
    } catch (error) {
      logger.logWarning(`Failed to mark item ${itemId} as processing completed`, error);
      return false;
    }
  }

  /**
   * Gets consumer status information
   * @returns {Object} Status object with connection and metrics details
   */
  getConfigStatus() {
    const uptime = Date.now() - this.metrics[METRICS_PROPERTIES.START_TIME];

    return {
      topic: this._topic,
      maxConcurrency: this.maxConcurrency,
      isConsuming: this.#isCurrentlyConsuming(),
      cacheConnected: this.#isCacheConnected(),
      activeMessages: 0,
      processedCount: this.metrics[METRICS_PROPERTIES.TOTAL_PROCESSED],
      failedCount: this.metrics[METRICS_PROPERTIES.TOTAL_FAILED],
      uptime: Math.floor(uptime / 1000),
    };
  }

  #isCacheConnected() {
    return this._cacheLayer ? this._cacheLayer.isConnected : false;
  }
}

module.exports = AbstractConsumer;
