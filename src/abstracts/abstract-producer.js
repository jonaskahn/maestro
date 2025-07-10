/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Abstract Producer Base Class for message broker implementations
 *
 * Provides unified interface for message production across Kafka, RabbitMQ, BullMQ with
 * deduplication strategies, caching, connection management, and business logic processing.
 * This class must be extended by specific broker implementations.
 */
const logger = require("../services/logger-service");

const DistributedLockService = require("../services/distributed-lock-service");

const REQUIRED_CONFIG_FIELDS = ["topic"];

class AbstractProducer {
  _topic;
  _cacheLayer;
  _distributedLockService;
  _monitorService;

  _hasConnected;
  _isShuttingDown;
  _topicExisted;

  _enabledSuppression;
  _enabledDistributedLock;

  /**
   * Create producer instance with configuration validation and initialization
   * @param {Object} config Producer configuration including topic and options
   */
  constructor(config) {
    this.#ensureNotDirectInstantiation();
    this.#validateAndInitialize(config);
    this.#setupGracefulShutdown();
    this.#logConfigurationLoaded();
  }

  /**
   * Returns the broker type identifier
   * @returns {string} Broker type ('kafka', 'rabbitmq', 'bullmq')
   * @throws {Error} When method is not implemented by subclass
   */
  getBrokerType() {
    throw new Error("getBrokerType method must be implemented by subclass");
  }

  #ensureNotDirectInstantiation() {
    if (this.constructor === AbstractProducer) {
      throw new Error("AbstractProducer cannot be instantiated directly");
    }
  }

  #validateConfigIsObject(config) {
    if (!config || typeof config !== "object") {
      throw new Error("Producer configuration must be an object");
    }
  }

  #validateRequiredFields(config) {
    const missingFields = REQUIRED_CONFIG_FIELDS.filter(field => !config[field]);
    if (missingFields.length > 0) {
      throw new Error(`Producer configuration missing required fields: ${missingFields.join(", ")}`);
    }
  }

  #validateConfiguration(config) {
    this.#validateConfigIsObject(config);
    this.#validateRequiredFields(config);
  }

  #initializeConfiguration(config) {
    this._topic = config.topic;
    this._enabledSuppression = config.useSuppression;
    this._enabledDistributedLock = config.useDistributedLock;
    this._topicExisted = false;
    this.config = config;
  }

  _createCacheLayer(_config) {
    logger.logWarning("Producer cache layer is disabled");
    return null;
  }

  #generateLockKey(topic) {
    return `${this.getBrokerType()?.toUpperCase()}-PRODUCER-DISTRIBUTED-LOCK-${topic?.toUpperCase()}`;
  }

  _createDistributedLockService(config) {
    if (!this._cacheLayer || !this._enabledDistributedLock) {
      logger.logWarning("Producer distributed lock is disabled");
      return null;
    }

    const lockKey = this.#generateLockKey(config?.topic);
    const lockTtl = config?.lockTtlMs || 600000;
    return new DistributedLockService(lockKey, lockTtl, this._cacheLayer);
  }

  _createMonitorService(_config) {
    logger.logDebug("Producer backpressure monitor is disabled");
    return null;
  }

  #initializeDependencies(config) {
    this._cacheLayer = this._createCacheLayer(config);
    this._distributedLockService = this._createDistributedLockService(config);
    this._monitorService = this._createMonitorService(config);
    this._isShuttingDown = false;
    this._hasConnected = false;
  }

  #validateAndInitialize(config) {
    this.#validateConfiguration(config);
    this.#initializeConfiguration(config);
    this.#initializeDependencies(config);
  }

  #removeShutdownListeners() {
    process.removeListener("SIGINT", this.#handleGracefulShutdownProducer);
    process.removeListener("SIGTERM", this.#handleGracefulShutdownProducer);
    process.removeListener("uncaughtException", this.#handleGracefulShutdownProducer);
    process.removeListener("unhandledRejection", this.#handleGracefulShutdownProducer);
  }

  async #handleGracefulShutdownProducer(signal = "unknown") {
    if (this._isShuttingDown) {
      return;
    }

    try {
      this._isShuttingDown = true;
      logger.logInfo(
        `${this.getBrokerType().toUpperCase()} producer received ${signal} signal, shutting down gracefully`
      );

      this.#removeShutdownListeners();

      if (this.#isAlreadyConnected()) {
        await this.disconnect();
      }
    } catch (error) {
      logger.logError(`Error during graceful shutdown of ${this.getBrokerType()} producer`, error);
    } finally {
      if (signal !== "uncaughtException" && signal !== "unhandledRejection") {
        if (typeof process.exit === "function") {
          process.exit(0);
        }
      }
    }
  }

  #setupGracefulShutdown() {
    process.on("SIGINT", this.#handleGracefulShutdownProducer.bind(this, "SIGINT"));
    process.on("SIGTERM", this.#handleGracefulShutdownProducer.bind(this, "SIGTERM"));
    process.on("uncaughtException", this.#handleGracefulShutdownProducer.bind(this, "uncaughtException"));
    process.on("unhandledRejection", this.#handleGracefulShutdownProducer.bind(this, "unhandledRejection"));
  }

  #logConfigurationLoaded() {
    logger.logDebug(
      `${this.getBrokerType()?.toUpperCase()} Producer loaded with configuration for topic ${this._topic}`
    );
  }

  #isAlreadyConnected() {
    return this._hasConnected;
  }

  async #connectCacheIfAvailable() {
    if (!this._cacheLayer) {
      return;
    }

    await this._cacheLayer.connect();
    logger.logInfo(`${this.getBrokerType().toUpperCase()} producer connected to cache layer`);
  }

  async #connectToMonitorServiceIfAvailable() {
    if (!this._monitorService) {
      return;
    }
    await this._monitorService.connect();
    logger.logInfo(`${this.getBrokerType()?.toUpperCase()} producer connected to monitor layer`);
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

  async performConnection() {
    await this.#connectCacheIfAvailable();
    await this.#connectToMonitorServiceIfAvailable();
    await this._connectToMessageBroker();
  }

  #markAsConnected() {
    this._hasConnected = true;
  }

  #markAsDisconnected() {
    this._hasConnected = false;
  }

  /**
   * Establishes connection to message broker and cache layer
   * @returns {Promise<void>}
   * @throws {Error} When connection fails
   */
  async connect() {
    if (this.#isAlreadyConnected()) {
      logger.logInfo(`${this.getBrokerType()} producer is already connected`);
      return;
    }

    try {
      await this.performConnection();
      this.#markAsConnected();
    } catch (error) {
      this.#markAsDisconnected();
      logger.logError(`Failed to connect ${this.getBrokerType()} producer`, error);
      throw error;
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

  async #disconnectCacheIfAvailable() {
    try {
      await this._cacheLayer?.disconnect();
      this._cacheLayer = null;
      logger.logInfo(`${this.getBrokerType()} producer disconnected from cache layer`);
    } catch (error) {
      logger.logWarning(`Error disconnecting from cache layer`, error);
    }
  }

  async #disConnectToMonitorServiceIfAvailable() {
    try {
      await this._monitorService?.disconnect();
      this._monitorService = null;
      logger.logInfo(`${this.getBrokerType()} producer disconnected from monitor layer`);
    } catch (error) {
      logger.logWarning(`Error disconnecting from monitor layer`, error);
    }
  }

  async performDisconnection() {
    await this.#disconnectCacheIfAvailable();
    await this.#disConnectToMonitorServiceIfAvailable();
    await this._disconnectFromMessageBroker();
  }

  /**
   * Disconnects from message broker and cache layer
   * @returns {Promise<void>}
   * @throws {Error} When disconnection fails
   */
  async disconnect() {
    if (!this.#isAlreadyConnected()) {
      logger.logWarning(`${this.getBrokerType()} producer is not connected`);
      return;
    }

    try {
      await this.performDisconnection();
      this.#markAsDisconnected();
      logger.logInfo(`${this.getBrokerType()} producer disconnected`);
    } catch (error) {
      logger.logError(`Error disconnecting ${this.getBrokerType()} producer`, error);
      throw error;
    }
  }

  /**
   * Check if topic exists in the message broker
   * @abstract
   * @returns {Promise<boolean>} True if topic exists
   */
  async _isTopicExisted() {
    throw new Error("_isTopicExisted method must be implemented by subclass");
  }

  async #ensureTopicExists() {
    try {
      this._topicExisted = await this._isTopicExisted();
      return this._topicExisted;
    } catch (error) {
      logger.logWarning(`Error checking if topic ${this._topic} exists`, error);
      return false;
    }
  }

  async #checkTopicExists() {
    if (!this.#isAlreadyConnected()) {
      logger.logWarning(`${this.getBrokerType()} producer is not connected, connecting first`);
      await this.connect();
    }

    return await this.#ensureTopicExists();
  }

  async #createLock() {
    if (!this._distributedLockService) {
      return true;
    }

    try {
      const lockResult = await this._distributedLockService.acquire();
      if (lockResult) {
        logger.logDebug(`${this.getBrokerType()} producer acquired distributed lock for topic ${this._topic}`);
      } else {
        logger.logWarning(
          `${this.getBrokerType()} producer failed to acquire distributed lock for topic ${this._topic}`
        );
      }
      return lockResult;
    } catch (error) {
      logger.logWarning(`Error acquiring distributed lock for topic ${this._topic}`, error);
      return false;
    }
  }

  async #releaseLock() {
    if (!this._distributedLockService) {
      return true;
    }

    try {
      const releaseResult = await this._distributedLockService.release();
      if (releaseResult) {
        logger.logDebug(`${this.getBrokerType()} producer released distributed lock for topic ${this._topic}`);
      } else {
        logger.logWarning(
          `${this.getBrokerType()} producer failed to release distributed lock for topic ${this._topic}`
        );
      }
      return releaseResult;
    } catch (error) {
      logger.logWarning(`Error releasing distributed lock for topic ${this._topic}`, error);
      return false;
    }
  }

  async #handleBackpressure(options = {}) {
    if (!this._monitorService || !options?.respectBackpressure) {
      return false;
    }

    try {
      const backpressureResult = await this._monitorService.shouldThrottleProduction();
      if (backpressureResult) {
        const { reason, lagDetails } = backpressureResult;
        logger.logWarning(
          `${this.getBrokerType()} producer backpressure detected for topic ${
            this._topic
          }: ${reason}, lag details: ${JSON.stringify(lagDetails)}`
        );
      }
      return backpressureResult?.shouldThrottle || false;
    } catch (error) {
      logger.logWarning(`Error checking backpressure for topic ${this._topic}`, error);
      return false;
    }
  }

  /**
   * Get next items to be processed
   * @param {Object} _criteria Query criteria for fetching items
   * @param {number} _limit Maximum number of items to fetch
   * @param {Array<string>} _excludedIds IDs to exclude from query
   * @returns {Promise<Array<Object>>} Array of items to be processed
   */
  async getNextItems(_criteria, _limit, _excludedIds) {
    throw new Error("getNextItems method must be implemented by subclass");
  }

  async #getItemsToProcess(criteria, limit, options = {}) {
    const excludedIds = options?.excludedIds || [];
    const items = await this.getNextItems(criteria, limit, excludedIds);

    if (!items || items.length === 0) {
      logger.logDebug(`${this.getBrokerType()} producer found no items to process for topic ${this._topic}`);
      return [];
    }

    logger.logDebug(`${this.getBrokerType()} producer found ${items.length} items to process for topic ${this._topic}`);
    return items;
  }

  async #isTopicPrimaryProducer(options = {}) {
    if (!this._enabledDistributedLock) {
      return true;
    }

    if (options?.skipLock) {
      logger.logDebug(`${this.getBrokerType()} producer skipping lock for topic ${this._topic}`);
      return true;
    }

    return await this.#createLock();
  }

  async #cleanUpAfterProducing(options = {}) {
    if (this._enabledDistributedLock && !options?.skipLock) {
      await this.#releaseLock();
    }
  }

  /**
   * Get unique identifier for the item
   * @param {Object} item Message item
   * @returns {string} Unique identifier
   */
  getItemId(item) {
    return item.id || item._id || item.key || JSON.stringify(item);
  }

  /**
   * Get message key for the broker
   * @param {Object} item Message item
   * @returns {string} Message key
   */
  getMessageKey(item) {
    return this.getItemId(item);
  }

  /**
   * Create broker-specific messages from items
   * @abstract
   * @param {Array<Object>} _items Items to convert to broker messages
   * @returns {Array<Object>} Broker-specific message objects
   */
  _createBrokerMessages(_items) {
    throw new Error("_createBrokerMessages method must be implemented by subclass");
  }

  /**
   * Send messages to the broker
   * @abstract
   * @param {Array<Object>} _messages Broker-specific message objects
   * @param {Object} _options Send options
   * @returns {Promise<Object>} Result of send operation
   */
  async _sendMessagesToBroker(_messages, _options) {
    throw new Error("_sendMessagesToBroker method must be implemented by subclass");
  }

  async #filterAlreadySentItems(items) {
    if (!this._cacheLayer || !this._enabledSuppression) {
      return {
        filteredItems: items,
        skippedItems: [],
      };
    }

    const filteredItems = [];
    const skippedItems = [];

    for (const item of items) {
      const itemId = this.getItemId(item);
      const cacheKey = `${this._topic}-${itemId}`;

      try {
        const isSent = await this._cacheLayer.get(cacheKey);
        if (isSent) {
          skippedItems.push(item);
        } else {
          filteredItems.push(item);
        }
      } catch (error) {
        logger.logWarning(`Error checking if item ${itemId} was already sent, including it anyway`, error);
        filteredItems.push(item);
      }
    }

    return {
      filteredItems,
      skippedItems,
    };
  }

  async #markItemsAsSent(items) {
    if (!this._cacheLayer || !this._enabledSuppression) {
      return;
    }

    const promises = items.map(async item => {
      const itemId = this.getItemId(item);
      const cacheKey = `${this._topic}-${itemId}`;

      try {
        await this._cacheLayer.set(cacheKey, "sent", { ttl: 86400 }); // 24 hours
      } catch (error) {
        logger.logWarning(`Error marking item ${itemId} as sent`, error);
      }
    });

    await Promise.all(promises);
  }

  async #processItemsAndSendToBroker(items, options) {
    const { filteredItems, skippedItems } = await this.#filterAlreadySentItems(items);

    if (filteredItems.length === 0) {
      logger.logInfo(
        `${this.getBrokerType()} producer skipping all ${skippedItems.length} items for topic ${
          this._topic
        }, already sent`
      );
      return {
        sent: false,
        totalMessages: items.length,
        sentMessages: 0,
        skippedMessages: skippedItems.length,
        deduplicatedMessages: 0,
      };
    }

    const messages = this._createBrokerMessages(filteredItems);
    const result = await this._sendMessagesToBroker(messages, options);
    await this.#markItemsAsSent(filteredItems);

    return {
      ...result,
      totalMessages: items.length,
      skippedMessages: skippedItems.length,
    };
  }

  async #processAndProduceMessages(criteria, limit, options = {}) {
    if (await this.#handleBackpressure(options)) {
      logger.logWarning(`${this.getBrokerType()} producer throttling due to backpressure for topic ${this._topic}`);
      return {
        sent: false,
        totalMessages: 0,
        sentMessages: 0,
        skippedMessages: 0,
        deduplicatedMessages: 0,
        backpressureDetected: true,
      };
    }

    const items = await this.#getItemsToProcess(criteria, limit, options);
    if (items.length === 0) {
      return {
        sent: false,
        totalMessages: 0,
        sentMessages: 0,
        skippedMessages: 0,
        deduplicatedMessages: 0,
      };
    }

    return await this.#processItemsAndSendToBroker(items, options);
  }

  /**
   * Get the message type (usually topic name)
   * @returns {string} Message type identifier
   */
  getMessageType() {
    return this._topic;
  }

  async #handleProduceError(criteria, limit, options, error) {
    const loggingOptions = {
      criteria,
      limit,
      topic: this._topic,
      options,
    };

    logger.logError(`${this.getBrokerType()} producer failed to produce messages`, {
      error,
      options: loggingOptions,
    });

    return {
      sent: false,
      error: error.message,
      totalMessages: 0,
      sentMessages: 0,
      skippedMessages: 0,
      deduplicatedMessages: 0,
      errorDetails: error,
    };
  }

  async #ensureConnectionAndTopic() {
    if (!this.#isAlreadyConnected()) {
      await this.connect();
    }

    if (!this._topicExisted) {
      await this.#checkTopicExists();
    }
  }

  /**
   * Produce messages to the broker
   * @param {Object} criteria Query criteria for fetching items
   * @param {number} limit Maximum number of items to produce
   * @param {Object} options Additional options for production
   * @returns {Promise<Object>} Result of production operation
   */
  async produce(criteria, limit, options = {}) {
    try {
      await this.#ensureConnectionAndTopic();

      const isPrimaryProducer = await this.#isTopicPrimaryProducer(options);
      if (!isPrimaryProducer) {
        logger.logWarning(`${this.getBrokerType()} producer is not primary for topic ${this._topic}`);
        return {
          sent: false,
          totalMessages: 0,
          sentMessages: 0,
          skippedMessages: 0,
          deduplicatedMessages: 0,
          notPrimary: true,
        };
      }

      const result = await this.#processAndProduceMessages(criteria, limit, options);
      return result;
    } catch (error) {
      return await this.#handleProduceError(criteria, limit, options, error);
    } finally {
      await this.#cleanUpAfterProducing(options);
    }
  }

  /**
   * Get producer configuration for status reporting
   * @returns {Object} Producer configuration object
   */
  _getStatusConfig() {
    return {
      broker: this.getBrokerType(),
      topic: this._topic,
      connected: this._hasConnected,
      topicExists: this._topicExisted,
      suppressionEnabled: this._enabledSuppression,
      distributedLockEnabled: this._enabledDistributedLock,
      monitorEnabled: this._monitorService !== null,
      cacheEnabled: this._cacheLayer !== null,
    };
  }

  /**
   * Get producer status information
   * @returns {Object} Producer status object
   */
  getStatus() {
    return {
      ...this._getStatusConfig(),
      status: this._isShuttingDown ? "shutting_down" : this._hasConnected ? "connected" : "disconnected",
    };
  }

  /**
   * Get the backpressure monitor service if available
   * @returns {Object|null} Backpressure monitor service or null
   */
  getBackpressureMonitor() {
    return this._monitorService;
  }
}

module.exports = AbstractProducer;
