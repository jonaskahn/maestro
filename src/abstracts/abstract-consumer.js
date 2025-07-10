/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Abstract Consumer Base Class
 *
 * Provides unified interface for message consumption across various message broker systems
 * such as Kafka, RabbitMQ, and BullMQ. Supports standardized message processing,
 * concurrency management, business logic handling, and metrics tracking.
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

class AbstractConsumer {
  _cacheLayer;

  _isConnected;
  _isConsuming;
  _statusReportInterval;
  _statusReportTimer;
  _isShuttingDown;

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
    this.#logConfigurationLoaded();
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
    this._topic = config.topic;
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
    this._statusReportInterval = this.#resolveStatusReportInterval(config);
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
    this._cacheLayer = this._createCacheLayer(config?.cacheOptions);
    this._isShuttingDown = false;
  }

  #initializeState() {
    this._isConnected = CONSUMER_STATES.DISCONNECTED;
    this._isConsuming = CONSUMER_STATES.NOT_CONSUMING;
    this._statusReportTimer = null;
    this._isShuttingDown = false;
  }

  _removeShutdownListeners() {
    process.removeListener("SIGINT", this.#handleGracefulShutdownConsumer);
    process.removeListener("SIGTERM", this.#handleGracefulShutdownConsumer);
    process.removeListener("uncaughtException", this.#handleGracefulShutdownConsumer);
    process.removeListener("unhandledRejection", this.#handleGracefulShutdownConsumer);
  }

  #markAsNotConsuming() {
    this._isConsuming = CONSUMER_STATES.NOT_CONSUMING;
  }

  /**
   * Stop consuming messages from the broker - must be implemented by subclasses
   * @abstract
   * @returns {Promise<void>}
   * @throws {Error} When method is not implemented
   */
  async _stopConsumingFromBroker() {
    throw new Error("_stopConsumingFromBroker method must be implemented by subclass");
  }

  #isCurrentlyConsuming() {
    return this._isConsuming === CONSUMER_STATES.CONSUMING;
  }

  /**
   * Stop consuming messages from the topic
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

  async #stopConsumingIfActive() {
    if (this._isConsuming === CONSUMER_STATES.CONSUMING) {
      await this.stopConsuming();
    }
  }

  /**
   * Disconnect from the message broker - must be implemented by subclasses
   * @abstract
   * @returns {Promise<void>}
   * @throws {Error} When method is not implemented
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

  async #performDisconnection() {
    await this.#stopConsumingIfActive();
    await this._disconnectFromMessageBroker();
    await this.#disconnectFromCache();
  }

  #isCurrentlyConnected() {
    return this._isConnected === CONSUMER_STATES.CONNECTED;
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
    } catch (error) {
      logger.logError(`Error during graceful shutdown of ${this.getBrokerType()} consumer`, error);
    } finally {
      process.exit(0);
    }
  }

  #preventDirectInstantiation() {
    if (this.constructor === AbstractConsumer) {
      throw new Error("AbstractConsumer cannot be instantiated directly");
    }
  }

  #setupGracefulShutdown() {
    process.on("SIGINT", this.#handleGracefulShutdownConsumer.bind(this, "SIGINT"));
    process.on("SIGTERM", this.#handleGracefulShutdownConsumer.bind(this, "SIGTERM"));
    process.on("uncaughtException", this.#handleGracefulShutdownConsumer.bind(this, "uncaughtException"));
    process.on("unhandledRejection", this.#handleGracefulShutdownConsumer.bind(this, "unhandledRejection"));
  }

  #logConfigurationLoaded() {
    logger.logInfo(
      `${this.getBrokerType()?.toUpperCase()} Consumer loaded with configuration for topic ${this._topic}`
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
   * Connect to the message broker - must be implemented by subclasses
   * @abstract
   * @returns {Promise<void>}
   * @throws {Error} When method is not implemented
   */
  async _connectToMessageBroker() {
    throw new Error("_connectToMessageBroker method must be implemented by subclass");
  }

  /**
   * Establishes connection to message broker and cache layer
   * @returns {Promise<void>}
   * @throws {Error} When connection fails
   */
  async connect() {
    if (this._isConnected) {
      logger.logInfo(`${this.getBrokerType()} consumer is already connected`);
      return;
    }

    try {
      if (this._cacheLayer) {
        await this._cacheLayer.connect();
        logger.logInfo(`${this.getBrokerType()} consumer connected to cache layer`);
      }

      await this._connectToMessageBroker();
      this._isConnected = CONSUMER_STATES.CONNECTED;
      logger.logInfo(`${this.getBrokerType()} consumer connected to broker`);
    } catch (error) {
      this._isConnected = CONSUMER_STATES.DISCONNECTED;
      logger.logError(`Failed to connect ${this.getBrokerType()} consumer`, error);
      throw error;
    }
  }

  /**
   * Disconnects from message broker and cache layer
   * @returns {Promise<void>}
   */
  async disconnect() {
    if (!this._isConnected) {
      logger.logWarning(`${this.getBrokerType()} consumer is not connected`);
      return;
    }

    try {
      await this.#performDisconnection();
      this._isConnected = CONSUMER_STATES.DISCONNECTED;
      logger.logInfo(`${this.getBrokerType()} consumer disconnected`);
    } catch (error) {
      logger.logError(`Error disconnecting ${this.getBrokerType()} consumer`, error);
      throw error;
    }
  }

  /**
   * Start consuming messages from broker - must be implemented by subclasses
   * @abstract
   * @param {Function} _handler Message handler function
   * @param {Object} _options Consumer options
   * @returns {Promise<void>}
   * @throws {Error} When method is not implemented
   */
  async _startConsumingFromBroker(_handler, _options) {
    throw new Error("_startConsumingFromBroker method must be implemented by subclass");
  }

  /**
   * Start consuming messages from the topic
   * @param {Object} options Consumption options
   * @returns {Promise<void>}
   */
  async consume(options = {}) {
    if (!this._isConnected) {
      logger.logWarning(`${this.getBrokerType()} consumer is not connected, connecting first`);
      await this.connect();
    }

    if (this._isConsuming) {
      logger.logWarning(`${this.getBrokerType()} consumer is already consuming from ${this._topic}`);
      return;
    }

    try {
      const handler = this._processReceivedItem.bind(this);
      await this._startConsumingFromBroker(handler, options);
      this._isConsuming = CONSUMER_STATES.CONSUMING;
      logger.logInfo(`${this.getBrokerType()} consumer started consuming from ${this._topic}`);

      this._startStatusReporting();
    } catch (error) {
      logger.logError(`Error starting consumption from ${this._topic}`, error);
      throw error;
    }
  }

  /**
   * Extract unique identifier from an item
   * @param {Object} item Item to extract ID from
   * @returns {string} Unique identifier
   */
  getItemId(item) {
    return item.id || item._id || item.key || JSON.stringify(item);
  }

  /**
   * Extract message key for partitioning from an item
   * @param {Object} item Item to extract key from
   * @returns {string} Message key for partitioning
   */
  getMessageKey(item) {
    return this.getItemId(item);
  }

  /**
   * Check if an item has already been processed
   * @param {string} _itemId Item identifier
   * @returns {Promise<boolean>} True if item has been processed
   */
  async _isItemProcessed(_itemId) {
    if (!this._cacheLayer) {
      return false;
    }

    try {
      const cacheKey = `${this._topic}-${_itemId}`;
      const result = await this._cacheLayer.get(cacheKey);
      return !!result;
    } catch (error) {
      logger.logWarning(`Error checking if item ${_itemId} is processed, assuming not processed`, error);
      return false;
    }
  }

  async _processReceivedItem(item) {
    if (!item) {
      logger.logWarning(`${this.getBrokerType()} consumer received empty item`);
      return true;
    }

    const itemId = this.getItemId(item);
    const isAlreadyProcessed = await this._isItemProcessed(itemId);

    if (isAlreadyProcessed) {
      logger.logDebug(`${this.getBrokerType()} consumer skipping already processed item ${itemId}`);
      return true;
    }

    try {
      logger.logDebug(`${this.getBrokerType()} consumer processing item ${itemId}`);
      await this.process(item);
      this.metrics.totalProcessed++;
      await this._onItemProcessSuccess(itemId);
      return true;
    } catch (error) {
      this.metrics.totalFailed++;
      await this._onItemProcessFailed(itemId, error);
      return false;
    }
  }

  /**
   * Process a received item
   * @param {Object} item Item to be processed
   * @returns {Promise<Object>} Processing result
   */
  async process(item) {
    throw new Error("process method must be implemented by subclass");
  }

  /**
   * Handle successful message processing
   * @param {string} _itemId Processed item identifier
   * @returns {Promise<void>}
   */
  async _onItemProcessSuccess(_itemId) {
    if (!this._cacheLayer) {
      return;
    }

    try {
      const cacheKey = `${this._topic}-${_itemId}`;
      await this._cacheLayer.set(cacheKey, "processed", { ttl: 86400 }); // 24 hours
    } catch (error) {
      logger.logWarning(`Error marking item ${_itemId} as processed`, error);
    }
  }

  /**
   * Handle failed message processing
   * @param {string} _itemId Failed item identifier
   * @param {Error} _error Processing error
   * @returns {Promise<void>}
   */
  async _onItemProcessFailed(_itemId, _error) {
    logger.logError(`Failed to process item ${_itemId}`, _error);
    if (!this._cacheLayer) {
      return;
    }

    try {
      const cacheKey = `${this._topic}-${_itemId}-error`;
      await this._cacheLayer.set(cacheKey, _error.message, { ttl: 86400 }); // 24 hours
    } catch (error) {
      logger.logWarning(`Error logging failed item ${_itemId}`, error);
    }
  }

  /**
   * Default business logic handler
   * @param {string} type Message type
   * @param {string} messageId Message identifier
   * @param {Object} item Message payload
   * @returns {Promise<void>}
   */
  async _defaultBusinessHandler(type, messageId, item) {
    const handlerMap = {
      "order-status-update": async () => {
        logger.logInfo(`Processing order status update for order ${item.orderId} to ${item.status}`);
        // Default implementation would do nothing
      },
      "product-inventory-update": async () => {
        logger.logInfo(`Processing inventory update for product ${item.productId} to ${item.quantity}`);
        // Default implementation would do nothing
      },
      default: async () => {
        logger.logWarning(`No specific handler for message type ${type} with ID ${messageId}`);
        // Default implementation would do nothing
      },
    };

    const handler = handlerMap[type] || handlerMap.default;
    await handler();
  }

  _startStatusReporting() {
    if (!this._statusReportInterval || this._statusReportInterval <= 0) {
      return;
    }

    this._statusReportTimer = setInterval(() => {
      const now = Date.now();
      const uptimeMs = now - this.metrics.startTime;
      const uptimeSec = Math.floor(uptimeMs / 1000);
      const messageRate = uptimeSec > 0 ? (this.metrics.totalProcessed / uptimeSec).toFixed(2) : 0;

      logger.logInfo(
        `${this.getBrokerType()} consumer status: ` +
          `processed=${this.metrics.totalProcessed}, ` +
          `failed=${this.metrics.totalFailed}, ` +
          `rate=${messageRate} msg/sec, ` +
          `uptime=${uptimeSec}s`
      );
    }, this._statusReportInterval);
  }

  /**
   * Get current configuration status
   * @returns {Object} Configuration status object
   */
  getConfigStatus() {
    const now = Date.now();
    const uptimeMs = now - this.metrics.startTime;

    return {
      broker: this.getBrokerType(),
      topic: this._topic,
      connected: this._isConnected,
      consuming: this._isConsuming,
      metrics: {
        processed: this.metrics.totalProcessed,
        failed: this.metrics.totalFailed,
        uptime: uptimeMs,
        rate: uptimeMs > 0 ? (this.metrics.totalProcessed / (uptimeMs / 1000)).toFixed(2) : 0,
      },
      maxConcurrency: this.maxConcurrency,
      cacheEnabled: !!this._cacheLayer,
    };
  }
}

module.exports = AbstractConsumer;
