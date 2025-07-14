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

/**
 * Abstract Producer Base Class
 *
 * Provides unified interface for message production across different message brokers
 * with deduplication strategies, caching, connection management, and distributed
 * lock support for coordinated message publishing.
 */
class AbstractProducer {
  _topic;
  _config;

  _cacheLayer;
  _distributedLockService;
  _monitorService;

  _isConnected;
  _isShuttingDown;
  _topicExisted;

  _enabledSuppression;
  _enabledDistributedLock;

  /**
   * Creates a producer instance with configuration validation and initialization
   *
   * @param {Object} config - Producer configuration object
   * @param {string} config.topic - Topic name to produce messages to
   * @param {boolean} [config.useSuppression] - Whether to enable message suppression/deduplication
   * @param {boolean} [config.useDistributedLock] - Whether to enable distributed lock for coordination
   * @param {Object} [config.lockTtlMs] - TTL for distributed locks in milliseconds
   * @param {boolean} [config.includeItems] - Whether to include original items in result
   */
  constructor(config) {
    this.#ensureNotDirectInstantiation();
    this.#validateAndInitialize(config);
    this.#setupGracefulShutdown();
    this._logConfigurationLoaded();
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
    this._topicExisted = false;
    this._enabledSuppression = config.useSuppression;
    this._enabledDistributedLock = config.useDistributedLock;
    this._config = config;
  }

  /**
   * Creates a cache layer for message deduplication and coordination
   * Override to implement specific cache layer creation
   *
   * @param {Object} _config - Cache configuration
   * @returns {Object|null} Cache layer instance or null if disabled
   */
  _createCacheLayer(_config) {
    logger.logWarning("Producer cache layer is disabled");
    return null;
  }

  #generateLockKey(topic) {
    return `${this.getBrokerType()?.toUpperCase()}-PRODUCER-DISTRIBUTED-LOCK-${topic?.toUpperCase()}`;
  }

  /**
   * Creates distributed lock service for coordinated message production
   *
   * @param {Object} config - Configuration object
   * @param {string} config.topic - Topic name for lock key generation
   * @param {number} [config.lockTtlMs] - Lock TTL in milliseconds
   * @returns {Object|null} Distributed lock service or null if disabled
   */
  _createDistributedLockService(config) {
    if (!this._cacheLayer || !this._enabledDistributedLock) {
      logger.logWarning("Producer distributed lock is disabled");
      return null;
    }

    const lockKey = this.#generateLockKey(config?.topic);
    const lockTtl = config?.lockTtlMs || 600000;
    return new DistributedLockService(lockKey, lockTtl, this._cacheLayer);
  }

  /**
   * Creates backpressure monitor service for adaptive rate limiting
   * Override to implement specific monitor creation
   *
   * @param {Object} _config - Monitor configuration
   * @returns {Object|null} Monitor service instance or null if disabled
   */
  _createMonitorService(_config) {
    logger.logDebug("Producer backpressure monitor is disabled");
    return null;
  }

  #initializeDependencies(config) {
    this._cacheLayer = this._createCacheLayer(config);
    this._distributedLockService = this._createDistributedLockService(config);
    this._monitorService = this._createMonitorService(config);
    this._isShuttingDown = false;
    this._isConnected = false;
  }

  #validateAndInitialize(config) {
    this.#validateConfiguration(config);
    this.#initializeConfiguration(config);
    this.#initializeDependencies(config);
  }

  /**
   * Removes shutdown event listeners
   */
  #removeShutdownListeners() {
    process.removeListener("SIGINT", this.#handleGracefulShutdownProducer);
    process.removeListener("SIGTERM", this.#handleGracefulShutdownProducer);
    process.removeListener("uncaughtException", this.#handleGracefulShutdownProducer);
    process.removeListener("unhandledRejection", this.#handleGracefulShutdownProducer);
  }

  async #handleGracefulShutdownProducer(signal = "unknown") {
    console.log(`\n SIGNAL RECEIVED: ${signal}`);
    if (this._isShuttingDown) {
      return;
    }

    try {
      this._isShuttingDown = true;
      logger.logInfo(
        `⏼ ${this.getBrokerType().toUpperCase()} producer received ${signal} signal, shutting down gracefully`
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

  /**
   * Logs the configuration loaded for the producer
   */
  _logConfigurationLoaded() {
    logger.logDebug(
      `${this.getBrokerType()?.toUpperCase()} Producer loaded with configuration ${JSON.stringify(this._config, null, 2)}`
    );
  }

  #isAlreadyConnected() {
    return this._isConnected;
  }

  async #connectCacheIfAvailable() {
    if (!this._cacheLayer) {
      return;
    }

    await this._cacheLayer.connect();
    logger.logDebug(`${this.getBrokerType().toUpperCase()} producer connected to cache layer`);
  }

  async #connectToMonitorServiceIfAvailable() {
    if (!this._monitorService) {
      return;
    }
    await this._monitorService.connect();
    logger.logDebug(`${this.getBrokerType()?.toUpperCase()} producer connected to monitor layer`);
  }

  /**
   * Implementation-specific method to connect to message broker
   * @returns {Promise<void>}
   */
  async _connectToMessageBroker() {
    throw new Error("_connectToMessageBroker method must be implemented by subclass");
  }

  /**
   * Performs connection to all dependent services
   * @returns {Promise<void>}
   */
  async performConnection() {
    await this.#connectCacheIfAvailable();
    await this.#connectToMonitorServiceIfAvailable();
    await this._connectToMessageBroker();
  }

  #markAsConnected() {
    this._isConnected = true;
  }

  #markAsDisconnected() {
    this._isConnected = false;
  }

  /**
   * Establishes connection to message broker and dependent services
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
      await this.#processAfterConnected();
      await this._createTopicIfAllowed();
      this.#markAsConnected();
      logger.logInfo(`${this.getBrokerType()?.toUpperCase()} producer is connected to topic [ ${this._topic} ]`);
    } catch (error) {
      this.#markAsDisconnected();
      logger.logError(`Failed to connect ${this.getBrokerType()} producer`, error);
      throw error;
    }
  }

  async #processAfterConnected() {
    this._topicExisted = await this._createTopicIfAllowed();
  }

  /**
   * Creates topic if allowed by broker and configuration
   * Override to implement topic creation logic
   * @returns {Promise<boolean>} True if topic was created or already exists
   */
  async _createTopicIfAllowed() {
    logger.logWarning(
      `You see this log because you do not implemented _createTopicIfAllowed in Producer. But it's safe to ignore`
    );
    return true;
  }

  /**
   * Implementation-specific method to disconnect from message broker
   * @returns {Promise<void>}
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

  async #cleanDistributedLockIfAvailable() {
    try {
      await this._distributedLockService?.release();
      this._distributedLockService = null;
      logger.logInfo(`${this.getBrokerType()} producer stop distributed lock service`);
    } catch (error) {
      logger.logWarning(`Error stop distributed lock service`, error);
    }
  }

  /**
   * Performs disconnection from all dependent services
   * @returns {Promise<void>}
   */
  async performDisconnection() {
    await this._disconnectFromMessageBroker();
    await this.#disconnectCacheIfAvailable();
    await this.#disConnectToMonitorServiceIfAvailable();
    await this.#cleanDistributedLockIfAvailable();
  }

  /**
   * Disconnects from message broker and dependent services
   * @returns {Promise<void>}
   * @throws {Error} When disconnection fails
   */
  async disconnect() {
    if (!this.#isAlreadyConnected()) {
      logger.logWarning(`${this.getBrokerType()} producer is already disconnected`);
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

  async #handleExtendedBackpressure() {
    try {
      const status = await this._monitorService.getBackpressureStatus();
      const delay = status.recommendedDelay || 1000;

      logger.logWarning(`Backpressure detected (${status.backpressureLevel}), pausing for ${delay}ms`, {
        topic: this._topic,
        brokerType: this.getBrokerType(),
        metrics: status.metrics,
      });

      await new Promise(resolve => setTimeout(resolve, delay));

      const stillInBackpressure = await this._monitorService.shouldPauseProcessing();

      if (stillInBackpressure) {
        const newStatus = await this._monitorService.getBackpressureStatus();
        logger.logWarning(`System still under backpressure (${newStatus.backpressureLevel}) after waiting ${delay}ms`, {
          topic: this._topic,
          brokerType: this.getBrokerType(),
          metrics: newStatus.metrics,
        });
        return true;
      } else {
        return false;
      }
    } catch (error) {
      logger.logWarning("Error handling extended backpressure", error);
      return true;
    }
  }

  async #isMessageBrokerUnderPressure() {
    if (!this._monitorService) {
      return false;
    }

    try {
      const shouldPause = await this._monitorService.shouldPauseProcessing();
      if (shouldPause) {
        return await this.#handleExtendedBackpressure();
      }
      return false;
    } catch (error) {
      logger.logWarning("Error checking backpressure status", error);
      return true;
    }
  }

  async #getProcessingIds() {
    if (!this._cacheLayer) {
      return [];
    }

    try {
      return await this._cacheLayer.getProcessingIds();
    } catch (error) {
      logger.logWarning("Failed to get processing IDs from cache", error);
      return [];
    }
  }

  async #getSuppressedIds() {
    if (!this._cacheLayer) {
      return [];
    }

    try {
      return await this._cacheLayer.getSuppressedIds();
    } catch (error) {
      logger.logWarning("Failed to get freezing IDs from cache", error);
      return [];
    }
  }

  #isSuppressionFullyEnabled() {
    return this._cacheLayer && this._enabledSuppression;
  }

  async #getExcludedIds() {
    try {
      const hardExclusions = (await this.#getProcessingIds()) ?? [];
      const softExclusions = this.#isSuppressionFullyEnabled() ? ((await this.#getSuppressedIds()) ?? []) : [];
      return [...new Set([...hardExclusions, ...softExclusions])];
    } catch (error) {
      logger.logWarning("Failed to get excluded IDs, continuing without exclusions", error);
      return [];
    }
  }

  /**
   * Gets next batch of items to process based on criteria
   * @param {Object} _criteria - Query criteria for items
   * @param {number} _limit - Maximum number of items to fetch
   * @param {Array<string>} _excludedIds - IDs to exclude from results
   * @returns {Promise<Array<Object>>} Items to process
   */
  async getNextItems(_criteria, _limit, _excludedIds) {
    throw new Error("getNextItems method must be implemented by subclass");
  }

  #itemNotFound(items) {
    return !items || items.length === 0;
  }

  #createEmptyResult(success) {
    return {
      success,
      messageType: this.getMessageType(),
      total: 0,
      sent: 0,
      skipped: 0,
      error: null,
      itemIds: [],
      details: {
        topic: this._topic,
        brokerType: this.getBrokerType(),
        timestamp: Date.now(),
        reason: "no_items_found",
      },
    };
  }

  async #markItemAsSuppressed(items) {
    try {
      if (!this._cacheLayer) {
        return;
      }
      for (const item of items) {
        await this._cacheLayer.markAsSuppressed(this.getItemId(item));
      }
    } catch (error) {
      logger.logError(
        `Producer failed to send suppressed messages, aware of duplication will be appeared faster than excepted.`,
        error
      );
    }
  }

  /**
   * Gets the item ID from an item object
   * Override to implement custom ID extraction
   * @param {Object} item - Item to get ID from
   * @returns {string} Unique item identifier
   */
  getItemId(item) {
    throw new Error("getItemId method must be implemented by subclass");
  }

  /**
   * Gets the message key for an item
   * @param {Object} item - Item to get key from
   * @returns {string} Message key
   */
  getMessageKey(item) {
    return this.getItemId(item);
  }

  /**
   * Creates broker-specific message format from items
   * @param {Array<Object>} _items - Items to convert to messages
   * @returns {Array<Object>} Broker-specific messages
   */
  _createBrokerMessages(_items) {
    throw new Error("_createBrokerMessages method must be implemented by subclass");
  }

  /**
   * Implementation-specific method to send messages to broker
   * @param {Array<Object>} _messages - Messages to send
   * @param {Object} _options - Send options
   * @returns {Promise<Object>} Send result details
   */
  async _sendMessagesToBroker(_messages, _options) {
    throw new Error("_sendMessagesToBroker method must be implemented by subclass");
  }

  async #acquireLock(waitTime) {
    if (!this._distributedLockService) {
      return true;
    }

    try {
      return await this._distributedLockService.acquire(waitTime);
    } catch (error) {
      logger.logError(`Error acquiring lock for ${this.getBrokerType()} producer`, error);
      return false;
    }
  }

  /**
   * Handles message skipping when lock acquisition fails
   * @param {Array<Object>} envelopedMessages - Messages that would have been sent
   * @returns {Object} Result with skipped count information
   */
  _skipMessagesSending(envelopedMessages) {
    const skippedCount = envelopedMessages.length;
    logger.logWarning(
      `Skipped sending ${skippedCount} messages to ${this.getBrokerType()} broker lock acquisition failure)`
    );

    return {
      success: true,
      sent: 0,
      skipped: skippedCount,
      error: null,
      details: {
        reason: "lock_timeout",
      },
    };
  }

  async #releaseLock() {
    try {
      return await this._distributedLockService?.release();
    } catch (error) {
      logger.logWarning(`Error releasing lock for ${this.getBrokerType()} producer`, error);
      return false;
    }
  }

  async #sendItemToBrokerWithoutLock(items, options = {}) {
    try {
      if (this.#isSuppressionFullyEnabled()) {
        await this.#markItemAsSuppressed(items);
      }
      const messages = this._createBrokerMessages(items);
      const sendResult = await this._sendMessagesToBroker(messages, options);
      logger.logDebug(`Successfully sent ${messages.length} messages to ${this.getBrokerType()} broker`);
      return {
        success: true,
        sent: messages.length,
        skipped: 0,
        error: null,
        details: sendResult || {},
      };
    } catch (error) {
      logger.logError(`Failed to send messages to ${this.getBrokerType()} broker`, error);
      throw error;
    }
  }

  async #retryWithExponentialBackoff(messages, options = {}) {
    const maxRetries = options.maxRetries || 3;
    const currentRetry = options.currentRetry || 0;

    if (currentRetry >= maxRetries) {
      throw new Error(`Max retries (${maxRetries}) exceeded for ${this.getBrokerType()} producer`);
    }

    const baseDelay = options?.baseDelay || 1000;
    const delay = baseDelay * Math.pow(2, currentRetry);

    logger.logInfo(`Retrying message send after ${delay}ms (attempt ${currentRetry + 1}/${maxRetries})`);

    await new Promise(resolve => setTimeout(resolve, delay));

    const retryOptions = {
      ...options,
      currentRetry: currentRetry + 1,
    };

    return await this.#sendItemsToBroker(messages, retryOptions);
  }

  async #handleLockAcquisitionFailure(items, options = {}) {
    logger.logWarning(`Failed to acquire lock for ${this.getBrokerType()} producer`);

    if (options?.failOnLockTimeout) {
      throw new Error(`Failed to acquire lock for ${this.getBrokerType()} producer`);
    }

    if (options?.skipOnLockTimeout) {
      return this._skipMessagesSending(items);
    }

    if (options?.ignoreLocksAndSend) {
      const skipLockOptions = { ...options, skipLock: true };
      return await this.#sendItemToBrokerWithoutLock(items, skipLockOptions);
    }

    return await this.#retryWithExponentialBackoff(items, options);
  }

  async #sendItemToBrokerWithLock(items, options = {}) {
    const waitTime = options?.lockWaitTime || 5000;

    try {
      const lockAcquired = await this.#acquireLock(waitTime);

      if (!lockAcquired) {
        return this.#handleLockAcquisitionFailure(items, options);
      }
      try {
        return await this.#sendItemToBrokerWithoutLock(items, options);
      } catch (e) {
        logger.logWarning("Producer failed to send message to Broker", e);
        return {
          success: false,
          sent: 0,
          skipped: items.length,
          error: e,
          details: {
            reason: "broker_send_error",
            errorMessage: e.message,
          },
        };
      } finally {
        await this.#releaseLock();
      }
    } catch (error) {
      if (options?.skipRetries) {
        throw error;
      }

      return this.#retryWithExponentialBackoff(items, options);
    }
  }

  async #sendItemsToBroker(items, options = {}) {
    this.#ensureConnected();
    if (this._distributedLockService) {
      return await this.#sendItemToBrokerWithLock(items, options);
    } else {
      return await this.#sendItemToBrokerWithoutLock(items, options);
    }
  }

  #validateItems(items) {
    if (!Array.isArray(items) || items.length === 0) {
      throw new Error("Items must be a non-empty array");
    }
  }

  /**
   * Returns the message type identifier
   * Override to provide specific message type
   * @returns {string} Message type
   */
  getMessageType() {
    throw new Error("getMessageType method must be implemented by subclass");
  }

  #buildProcessingResult(items, sendResult) {
    const { success, sent, skipped, error, details } = sendResult;

    const itemIds = items.map(item => this.getItemId(item));

    return {
      success,
      messageType: this.getMessageType(),
      total: items.length,
      sent,
      skipped,
      error: error ? error.message : null,
      itemIds,
      items: this._config?.includeItems ? items : undefined,
      details: {
        topic: this._topic,
        brokerType: this.getBrokerType(),
        timestamp: Date.now(),
        ...details,
      },
    };
  }

  async #processItems(items, options = {}) {
    this.#validateItems(items);
    const sendResult = await this.#sendItemsToBroker(items, options);
    return this.#buildProcessingResult(items, sendResult);
  }

  #logProductionSuccess(result) {
    const { sent, skipped, total } = result;

    if (total === 0) {
      logger.logInfo(`No ${this.getMessageType()} items found for processing`);
      return;
    }

    logger.logInfo(`Produced ${sent} ${this.getMessageType()} messages (${skipped} skipped) to ${this._topic} topic`);
  }

  /**
   * Produces messages based on item criteria
   * @param {Object} criteria - Query criteria for items
   * @param {number} limit - Maximum number of items to process
   * @param {Object} options - Production options
   * @param {number} [options.maxRetries] - Maximum number of retries on failure
   * @param {number} [options.baseDelay] - Base delay for exponential backoff in ms
   * @param {boolean} [options.skipOnLockTimeout] - Whether to skip sending if lock acquisition fails
   * @param {boolean} [options.failOnLockTimeout] - Whether to fail if lock acquisition fails
   * @param {boolean} [options.ignoreLocksAndSend] - Whether to ignore locks and send anyway
   * @param {number} [options.lockWaitTime] - How long to wait for lock acquisition in ms
   * @returns {Promise<Object>} Production result with counts and details
   */
  async produce(criteria, limit, options = {}) {
    const successResult = this.#createEmptyResult(true);
    const errorResult = this.#createEmptyResult(false);
    try {
      this.#ensureConnected();
      if (await this._topicExisted) {
        const isPressure = await this.#isMessageBrokerUnderPressure();
        if (isPressure) {
          logger.logWarning("System is under pressure, stop sending new items");
          return successResult;
        }
        const excludedIds = (await this.#getExcludedIds()) ?? [];
        const items = (await this.getNextItems(criteria, limit, excludedIds)) ?? [];

        const filteredItems = items.filter(item => {
          const itemId = this.getItemId(item);
          return !excludedIds.includes(itemId);
        });

        if (this.#itemNotFound(filteredItems)) {
          logger.logDebug(`No ${this.getMessageType()} items found for processing`);
          return successResult;
        }
        const result = await this.#processItems(filteredItems, options);
        this.#logProductionSuccess(result);
        return result;
      }
      logger.logWarning(`Topic ${this._topic} seems does not existed`);
      return successResult;
    } catch (error) {
      logger.logError(`Failed to produce messages to ${this._topic} topic`, error);
      return errorResult;
    }
  }

  #ensureConnected() {
    if (this._isShuttingDown) {
      throw new Error(`${this.getBrokerType()} producer is shutting down`);
    }

    if (!this.#isAlreadyConnected()) {
      throw new Error(`${this.getBrokerType()} producer is not connected`);
    }
  }

  #isCacheConnected() {
    return this._cacheLayer ? this._cacheLayer.isConnected() : false;
  }

  /**
   * Gets configuration status information
   * @returns {Object} Configuration status
   */
  _getStatusConfig() {
    return {
      enabledSuppression: this.#isSuppressionFullyEnabled(),
      enabledDistributedLock: this._enabledDistributedLock && Boolean(this._distributedLockService),
      enabledBackpressure: Boolean(this._monitorService),
      enabledCache: Boolean(this._cacheLayer),
    };
  }

  #getLockStatus() {
    if (!this._distributedLockService) {
      return { enabled: false };
    }

    return {
      enabled: true,
      key: this._distributedLockService.getLockKey(),
      ttl: this._distributedLockService.getLockTtl(),
    };
  }

  /**
   * Gets producer status information
   * @returns {Object} Status object with connection and configuration details
   */
  getStatus() {
    return {
      brokerType: this.getBrokerType(),
      connected: this.#isAlreadyConnected(),
      cacheConnected: this.#isCacheConnected(),
      topic: this._topic,
      enabledSuppression: this.#isSuppressionFullyEnabled(),
      config: this._getStatusConfig(),
      lock: this.#getLockStatus(),
    };
  }

  /**
   * Gets the backpressure monitor instance
   * @returns {Object|null} Backpressure monitor
   */
  getBackpressureMonitor() {
    return this._monitorService;
  }
}

module.exports = AbstractProducer;
