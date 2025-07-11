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
  MAX_CONCURRENT_MESSAGES: ["JO_MAX_CONCURRENT_MESSAGES"],
  STATUS_REPORT_INTERVAL: ["JO_STATUS_REPORT_INTERVAL"],
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

class AbstractConsumer {
  #cacheLayer;
  #isConnected;
  #isConsuming;
  #statusReportInterval;
  #statusReportTimer;
  #isShuttingDown;

  /**
   * Create consumer instance with configuration validation and initialization
   * @param {Object} config Consumer configuration including topic, concurrency, cache options
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

  #getEnvironmentValue(keys) {
    for (const key of keys) {
      const value = process.env[key];
      if (value !== undefined) {
        return value;
      }
    }
    return null;
  }

  #validateConfigurationStructure(config) {
    if (!config || typeof config !== "object") {
      throw new Error("Consumer configuration must be an object");
    }
  }

  #validateTopicConfiguration(config) {
    if (!config.topic || typeof config.topic !== "string") {
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

  #validateConfiguration(config) {
    this.#validateConfigurationStructure(config);
    this.#validateTopicConfiguration(config);
    this.#validateCacheOptions(config);
  }

  #setBasicConfiguration(config) {
    this.topic = config.topic;
    this.config = config;
  }

  #setConcurrencyConfiguration(config) {
    this.maxConcurrency =
      config.maxConcurrency ||
      parseInt(this.#getEnvironmentValue(ENV_KEYS.MAX_CONCURRENT_MESSAGES)) ||
      DEFAULT_VALUES.MAX_CONCURRENCY;
  }

  #initializeMetrics() {
    this.metrics = {
      [METRICS_PROPERTIES.TOTAL_PROCESSED]: 0,
      [METRICS_PROPERTIES.TOTAL_FAILED]: 0,
      [METRICS_PROPERTIES.START_TIME]: Date.now(),
    };
  }

  #resolveStatusReportInterval(config) {
    return (
      config.statusReportInterval ||
      parseInt(this.#getEnvironmentValue(ENV_KEYS.STATUS_REPORT_INTERVAL)) ||
      DEFAULT_VALUES.STATUS_REPORT_INTERVAL_MS
    );
  }

  #setStatusReporting(config) {
    this.#statusReportInterval = this.#resolveStatusReportInterval(config);
  }

  #initializeConfiguration(config) {
    this.#setBasicConfiguration(config);
    this.#setConcurrencyConfiguration(config);
    this.#initializeMetrics();
    this.#setStatusReporting(config);
  }

  _createCacheLayer(_config) {
    logger.logDebug("⁉️Cache layer is disabled");
    return null;
  }

  #initializeDependencies(config) {
    this.#cacheLayer = this._createCacheLayer(config?.cacheOptions);
    this.#isShuttingDown = false;
  }

  #initializeState() {
    this.#isConnected = CONSUMER_STATES.DISCONNECTED;
    this.#isConsuming = CONSUMER_STATES.NOT_CONSUMING;
    this.#statusReportTimer = null;
    this.#isShuttingDown = false;
  }

  _removeShutdownListeners() {
    process.removeListener("SIGINT", this.#handleGracefulShutdownConsumer);
    process.removeListener("SIGTERM", this.#handleGracefulShutdownConsumer);
    process.removeListener("uncaughtException", this.#handleGracefulShutdownConsumer);
    process.removeListener("unhandledRejection", this.#handleGracefulShutdownConsumer);
  }

  #markAsNotConsuming() {
    this.#isConsuming = CONSUMER_STATES.NOT_CONSUMING;
  }

  async _stopConsumingFromBroker() {
    throw new Error("_stopConsumingFromBroker method must be implemented by subclass");
  }

  #isCurrentlyConsuming() {
    return this.#isConsuming === CONSUMER_STATES.CONSUMING;
  }

  async stopConsuming() {
    if (!this.#isCurrentlyConsuming()) {
      logger.logWarning(`${this.getBrokerType()} consumer is not currently consuming`);
      return;
    }

    try {
      await this._stopConsumingFromBroker();
      this.#markAsNotConsuming();
      logger.logInfo(`${this.getBrokerType()} consumer stopped consuming from ${this.topic}`);

      if (this.#statusReportTimer) {
        clearInterval(this.#statusReportTimer);
        this.#statusReportTimer = null;
      }
    } catch (error) {
      logger.logError(`Error stopping consumption from ${this.topic}`, error);
    }
  }

  async #stopConsumingIfActive() {
    if (this.#isConsuming === CONSUMER_STATES.CONSUMING) {
      await this.stopConsuming();
    }
  }

  async _disconnectFromMessageBroker() {
    throw new Error("_disconnectFromMessageBroker method must be implemented by subclass");
  }

  async #disconnectFromCache() {
    if (!this.#cacheLayer) {
      return;
    }

    try {
      await this.#cacheLayer.disconnect();
      logger.logInfo(`${this.getBrokerType()} consumer disconnected from cache layer`);
    } catch (error) {
      logger.logWarning("Error disconnecting from cache layer", error);
    }
  }

  async #performDisconnection() {
    await this.#stopConsumingIfActive();
    await this._disconnectFromMessageBroker();
    await this.#disconnectFromCache();
  }

  #isCurrentlyConnected() {
    return this.#isConnected === CONSUMER_STATES.CONNECTED;
  }

  async #handleGracefulShutdownConsumer(signal = "unknown") {
    if (this.#isShuttingDown) {
      return;
    }

    try {
      this.#isShuttingDown = true;
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

  #setupGracefulShutdown() {
    process.on("SIGINT", this.#handleGracefulShutdownConsumer.bind(this, "SIGINT"));
    process.on("SIGTERM", this.#handleGracefulShutdownConsumer.bind(this, "SIGTERM"));
    process.on("uncaughtException", this.#handleGracefulShutdownConsumer.bind(this, "uncaughtException"));
    process.on("unhandledRejection", this.#handleGracefulShutdownConsumer.bind(this, "unhandledRejection"));
  }

  #preventDirectInstantiation() {
    if (this.constructor === AbstractConsumer) {
      throw new Error("AbstractConsumer cannot be instantiated directly");
    }
  }

  /**
   * Returns the broker type identifier
   * @returns {string} Broker type ('kafka', 'rabbitmq', 'bullmq')
   * @throws {Error} When method is not implemented by subclass
   */
  getBrokerType() {
    throw new Error("getBrokerType method must be implemented by subclass");
  }

  _logConfigurationLoaded() {
    logger.logDebug(
      `🐞 ${this.getBrokerType()?.toUpperCase()} Consumer loaded with configuration ${JSON.stringify(this.config, null, 2)}`
    );
  }

  #isAlreadyConnected() {
    return this.#isConnected === CONSUMER_STATES.CONNECTED;
  }

  async #connectToCache() {
    if (!this.#cacheLayer) {
      return;
    }

    try {
      await this.#cacheLayer.connect();
      logger.logInfo(`🔌 ${this.getBrokerType()?.toUpperCase()} consumer connected to cache layer`);
    } catch (error) {
      logger.logWarning("Failed to connect to cache layer, continuing without cache", error);
    }
  }

  async _connectToMessageBroker() {
    throw new Error("_connectToMessageBroker method must be implemented by subclass");
  }

  async #establishConnection() {
    await this.#connectToCache();
    await this._connectToMessageBroker();
  }

  #markAsConnected() {
    this.#isConnected = CONSUMER_STATES.CONNECTED;
  }

  #handleConnectionError(error) {
    this.#isConnected = CONSUMER_STATES.DISCONNECTED;
    logger.logError(`Failed to connect ${this.getBrokerType()} consumer`, error);
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
      this.#markAsConnected();
    } catch (error) {
      this.#handleConnectionError(error);
      throw error;
    }
  }

  #markAsDisconnected() {
    this.#isConnected = CONSUMER_STATES.DISCONNECTED;
  }

  #handleDisconnectionError(error) {
    logger.logError(`Error disconnecting ${this.getBrokerType()} consumer`, error);
    this.#isConsuming = CONSUMER_STATES.NOT_CONSUMING;
    this.#isConnected = CONSUMER_STATES.DISCONNECTED;
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

  #ensureConnected() {
    if (this.#isShuttingDown) {
      throw new Error(`${this.getBrokerType()} consumer is shutting down`);
    }

    if (!this.#isCurrentlyConnected()) {
      throw new Error(`${this.getBrokerType()} consumer is not connected`);
    }
  }

  async _startConsumingFromBroker(_handler, _options) {
    throw new Error("_startConsumingFromBroker method must be implemented by subclass");
  }

  _handleConsumingStartError(error) {
    this.#isConsuming = CONSUMER_STATES.NOT_CONSUMING;
    logger.logError(`Failed to start consuming from ${this.topic}`, error);
  }

  #startStatusReporting() {
    if (this.#statusReportTimer) {
      clearInterval(this.#statusReportTimer);
    }

    this.#statusReportTimer = setInterval(() => {
      const status = this.getConfigStatus();
      logger.logInfo(
        `${this.getBrokerType()} consumer status: processed=${status.processedCount}, failed=${status.failedCount}, active=0`
      );
    }, this.#statusReportInterval);
  }

  /**
   * Starts consuming messages from the broker
   * @param {Object} options - Consumption options
   * @returns {Promise<void>}
   * @throws {Error} When consumption fails to start
   */
  async consume(options = {}) {
    this.#ensureConnected();

    if (this.#isConsuming === CONSUMER_STATES.CONSUMING) {
      logger.logWarning(`${this.getBrokerType()} consumer is already consuming messages`);
      return;
    }

    try {
      await this._startConsumingFromBroker(options);
      this.#isConsuming = CONSUMER_STATES.CONSUMING;
      logger.logInfo(`${this.getBrokerType()} consumer started consuming from ${this.topic}`);

      if (this.#statusReportInterval > 0) {
        this.#startStatusReporting();
      }
    } catch (error) {
      this._handleConsumingStartError(error);
      throw error;
    }
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
   * Checks if an item is already completed in the cache
   * @param {string} _itemId - Item identifier
   * @returns {Promise<boolean>} True if item is completed
   */
  async _isItemProcessed(_itemId) {
    throw new Error("_isItemProcessed must be implemented by subclass");
  }

  async #isMessageAlreadyCompleted(itemId) {
    try {
      return await this._isItemProcessed(itemId);
    } catch (error) {
      logger.logWarning(`Failed to check if message ${itemId} is already completed`, error);
      return false;
    }
  }

  async #markAsProcessingStart(itemId) {
    try {
      if (!this.#cacheLayer) {
        return false;
      }
      return await this.#cacheLayer?.markAsProcessing(itemId);
    } catch (error) {
      logger.logWarning(`Failed to mark item ${itemId} as processing start`, error);
      return false;
    }
  }

  async #markAsProcessingEnd(itemId) {
    if (this.#cacheLayer) {
      return;
    }
    try {
      return await this.#cacheLayer?.markAsCompletedProcessing(itemId);
    } catch (error) {
      logger.logWarning(`Failed to mark item ${itemId} as processing completed`, error);
      return false;
    }
  }

  /**
   * Processes a message with business logic
   * This is the method that should be overridden by user to implement business logic
   * @returns {Promise<void>}
   * @param _item
   */
  async process(_item) {
    throw new Error("process method must be implemented by user");
  }

  /**
   * Called when item processing is successful
   * Override to implement custom success handling
   * @param {string} itemId - Item identifier
   * @returns {Promise<void>}
   */
  async _onItemProcessSuccess(_itemId) {
    throw new Error("_markItemAsCompleted method must be implemented by subclass");
  }

  async #afterProcessSuccess(itemId, messageKey, startTime) {
    await this._onItemProcessSuccess(itemId);
    this.metrics[METRICS_PROPERTIES.TOTAL_PROCESSED]++;
    const duration = Date.now() - startTime;
    logger.logInfo(`Successfully processed message: ${itemId} (key: ${messageKey}, duration: ${duration}ms)`);
  }

  async _onItemProcessFailed(_itemId, _error) {
    throw new Error("_onItemProcessFailed method must be implemented by subclass");
  }

  async #handleProcessingFailure(itemId, messageKey, error) {
    await this._onItemProcessFailed(itemId, error);
    this.metrics[METRICS_PROPERTIES.TOTAL_FAILED]++;
    logger.logError(
      `Failed to process message: ${itemId} (Key: ${messageKey}), but error will be ignored to throw`,
      error
    );
  }

  async _defaultBusinessHandler(type, messageId, item) {
    const startTime = Date.now();
    const itemId = this.getItemId(item);
    const messageKey = this.getMessageKey(item);
    try {
      logger.logDebug(`Processing message: ${itemId} (key: ${messageKey})`);
      if (await this.#isMessageAlreadyCompleted(itemId)) {
        logger.logInfo(`Skipping already completed message: ${itemId} (${messageId})`);
        return;
      }
      const result = await this.#markAsProcessingStart(itemId);
      if (!result) {
        logger.logWarning(`${this.getBrokerType()} consumer is processing`);
        return;
      }
      await this.process(item);
      await this.#afterProcessSuccess(itemId, messageKey, startTime);
    } catch (error) {
      await this.#handleProcessingFailure(itemId, messageKey, error);
    } finally {
      await this.#markAsProcessingEnd(itemId);
    }
  }

  #isCacheConnected() {
    return this.#cacheLayer ? this.#cacheLayer.isConnected : false;
  }

  getConfigStatus() {
    const uptime = Date.now() - this.metrics[METRICS_PROPERTIES.START_TIME];

    return {
      topic: this.topic,
      maxConcurrency: this.maxConcurrency,
      isConsuming: this.#isCurrentlyConsuming(),
      cacheConnected: this.#isCacheConnected(),
      activeMessages: 0,
      processedCount: this.metrics[METRICS_PROPERTIES.TOTAL_PROCESSED],
      failedCount: this.metrics[METRICS_PROPERTIES.TOTAL_FAILED],
      uptime: Math.floor(uptime / 1000),
    };
  }
}

module.exports = AbstractConsumer;
