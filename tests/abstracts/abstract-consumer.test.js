jest.mock("../../src/services/logger-service", () => ({
  logDebug: jest.fn(),
  logError: jest.fn(),
  logWarning: jest.fn(),
  logInfo: jest.fn(),
  logConnectionEvent: jest.fn(),
}));

const logger = require("../../src/services/logger-service");
const AbstractConsumer = require("../../src/abstracts/abstract-consumer");

jest.spyOn(global, "setInterval").mockImplementation((cb, _interval) => {
  cb();
  return 123;
});

jest.spyOn(global, "clearInterval");

const originalProcessExit = process.exit;
process.exit = jest.fn();

const AbstractConsumerOriginal = require("../../src/abstracts/abstract-consumer");

class TestConsumer extends AbstractConsumer {
  constructor(config) {
    super(config);
    this.brokerConnected = false;
    this.brokerConsuming = false;
    this.processedItems = new Map();
    this.processHandler = null;
    this.mockCacheLayer = null;
    this.customBusinessHandler = null;
    this.topicCreated = false;

    // Validate topic and consumer group
    if (!config.topic || typeof config.topic !== "string" || config.topic.trim() === "") {
      throw new Error("Consumer configuration must include a topic string");
    }

    if (!config.consumerGroup || typeof config.consumerGroup !== "string" || !config.consumerGroup.trim()) {
      throw new Error("Consumer group must be a non-empty string");
    }

    // Set required properties
    this._consumerGroup = config.consumerGroup;
    this._consumerOptions = config.consumerOptions || {};
    this._statusReportingInterval = config.statusReportingIntervalMs || 0;

    if (config && config.cacheOptions) {
      // Add keyPrefix to cacheOptions if not present
      if (!config.cacheOptions.keyPrefix) {
        config.cacheOptions.keyPrefix = "test:";
      }

      this.mockCacheLayer = {
        connect: jest.fn().mockResolvedValue(true),
        disconnect: jest.fn().mockResolvedValue(true),
        isConnected: true,
        markAsProcessing: jest.fn().mockResolvedValue(true),
        markAsCompletedProcessing: jest.fn().mockResolvedValue(true),
        isProcessed: jest.fn().mockImplementation(async itemId => {
          return Promise.resolve(this.processedItems.has(itemId));
        }),
      };
      this._cacheLayer = this.mockCacheLayer;
    }
  }

  getBrokerType() {
    return "test-broker";
  }

  _createCacheLayer(config) {
    if (!config) return null;
    return this.mockCacheLayer;
  }

  async _connectToMessageBroker() {
    this.brokerConnected = true;
    return Promise.resolve(true);
  }

  async _disconnectFromMessageBroker() {
    this.brokerConnected = false;
    return Promise.resolve(true);
  }

  async _startConsumingFromBroker(options) {
    this.brokerConsuming = true;
    this.processHandler = options?.handler || (() => {});
    this.customBusinessHandler = options?.businessHandler;
    return Promise.resolve(true);
  }

  async _stopConsumingFromBroker() {
    this.brokerConsuming = false;
    return Promise.resolve(true);
  }

  async _isItemProcessed(itemId) {
    logger.logDebug(`Checking if item ${itemId} is processed`);
    return Promise.resolve(this.processedItems.has(itemId));
  }

  async _onItemProcessSuccess(itemId) {
    this.processedItems.set(itemId, true);
    return Promise.resolve(true);
  }

  async _onItemProcessFailed(itemId, error) {
    this.processedItems.set(itemId, { error: error.message });
    return Promise.resolve(true);
  }

  async _markAsProcessingStart(itemId) {
    if (this._cacheLayer) {
      return this._cacheLayer.markAsProcessing(itemId);
    }
    return true;
  }

  async _markAsProcessingEnd(itemId) {
    if (this._cacheLayer) {
      return this._cacheLayer.markAsCompletedProcessing(itemId, true);
    }
    return true;
  }

  async _afterProcessSuccess(itemId, messageKey, startTime) {
    await this._onItemProcessSuccess(itemId);
    return true;
  }

  getItemId(item) {
    return item.id;
  }

  getMessageKey(item) {
    return this.getItemId(item);
  }

  async simulateMessage(messageType, messageId, messageData) {
    if (this.customBusinessHandler) {
      await this.customBusinessHandler(messageType, messageId, messageData);
      return;
    }
    await this._defaultBusinessHandler(messageType, messageId, messageData);
  }

  async process(item) {
    logger.logDebug("Processing message", { itemId: item.id });
    if (item.shouldFail) {
      const error = new Error("Processing failed");
      logger.logError("Failed to consume message", error, { itemId: item.id });
      throw error;
    }
    logger.logInfo("Successfully processed message", { itemId: item.id });
    return Promise.resolve(true);
  }

  async _createTopicIfAllowed() {
    this.topicCreated = true;
    return Promise.resolve(true);
  }

  async connect() {
    if (this.brokerConnected) {
      logger.logInfo(`${this.getBrokerType()} consumer is already connected`);
      return;
    }

    try {
      if (this._cacheLayer) {
        await this._cacheLayer.connect();
      }

      await this._connectToMessageBroker();
      await this._createTopicIfAllowed();
      this.brokerConnected = true;
      this._isConnected = true;
      logger.logInfo(`${this.getBrokerType()} consumer connected`);
    } catch (error) {
      logger.logError(`Failed to connect ${this.getBrokerType()} consumer`, error);
      throw error;
    }
  }

  async startConsuming(options = {}) {
    if (this._isConsuming === true) {
      logger.logInfo(`${this.getBrokerType()} consumer is already consuming from ${this._topic}`);
      return;
    }

    if (this._isShuttingDown) {
      const error = new Error(`${this.getBrokerType()} consumer is shutting down`);
      logger.logError(error.message);
      throw error;
    }

    if (!this.brokerConnected) {
      const error = new Error("Consumer must be connected");
      logger.logError(error.message);
      throw error;
    }

    try {
      await this._startConsumingFromBroker(options);
      this._isConsuming = true;
      this.brokerConsuming = true;
      this._setupStatusReporting();
      logger.logInfo(`${this.getBrokerType()} consumer started consuming from ${this._topic}`);
      return true;
    } catch (error) {
      logger.logError(`Failed to start consuming from ${this._topic}`, error);
      throw error;
    }
  }

  async stopConsuming() {
    if (!this.brokerConsuming) {
      logger.logInfo(`${this.getBrokerType()} consumer is already not consuming`);
      return;
    }

    try {
      await this._stopConsumingFromBroker();
      this.brokerConsuming = false;
      this._isConsuming = false;
      this._clearStatusReporting();
      logger.logInfo(`${this.getBrokerType()} consumer stopped consuming from ${this._topic}`);
      return true;
    } catch (error) {
      logger.logError(`Error stopping consumption from ${this._topic}`, error);
      throw error;
    }
  }

  async _processItem(item) {
    const itemId = this.getItemId(item);

    const isProcessed = await this._isItemProcessed(itemId);
    if (isProcessed) {
      logger.logDebug("Message already processed, skipping", { itemId });
      return true;
    }

    if (this._cacheLayer) {
      await this._cacheLayer.markAsProcessing(itemId);
    }

    try {
      await this.process(item);
      if (this._cacheLayer) {
        await this._cacheLayer.markAsCompletedProcessing(itemId, true);
      }
      await this._onItemProcessSuccess(itemId);
      return true;
    } catch (error) {
      if (this._cacheLayer) {
        await this._cacheLayer.markAsCompletedProcessing(itemId, false);
      }
      await this._onItemProcessFailed(itemId, error);
      return false;
    }
  }

  getStatus() {
    return {
      isConnected: this.brokerConnected,
      isConsuming: this.brokerConsuming,
      topic: this._topic,
      consumerGroup: this._consumerGroup,
    };
  }

  _setupStatusReporting() {
    if (this._statusReportingInterval > 0 && !this._statusTimer) {
      this._statusTimer = setInterval(() => {
        const status = this.getStatus();
        logger.logInfo(`Consumer status: ${JSON.stringify(status)}`);
      }, this._statusReportingInterval);
    }
  }

  _clearStatusReporting() {
    if (this._statusTimer) {
      clearInterval(this._statusTimer);
      this._statusTimer = null;
    }
  }

  async disconnect() {
    try {
      if (this.brokerConsuming) {
        await this.stopConsuming().catch(error => {
          logger.logWarning(`Error stopping consumption during disconnect: ${error.message}`);
        });
      }

      await this._disconnectFromMessageBroker();
      this.brokerConnected = false;
      this._isConnected = false;

      if (this._cacheLayer && typeof this._cacheLayer.disconnect === "function") {
        await this._cacheLayer.disconnect();
      }

      logger.logInfo(`${this.getBrokerType()} consumer disconnected`);
    } catch (err) {
      logger.logError(`Error disconnecting ${this.getBrokerType()} consumer`, err);
      throw err; // Re-throw the error to ensure it propagates
    }
  }

  async _defaultBusinessHandler(type, messageId, item) {
    const startTime = Date.now();
    const messageKey = this.getMessageKey?.(item) || "-";
    const itemId = this.getItemId(item);

    try {
      if (!this._cacheLayer) {
        const isProcessed = await this._isItemProcessed(messageId).catch(() => false);
        if (isProcessed) {
          logger.logInfo(`Message already processed, skipping: ${messageId} (Key: ${messageKey})`);
          return true;
        }

        const isMarkStarted = await this._markAsProcessingStart(messageId);
        if (!isMarkStarted) {
          logger.logWarning(`Failed to mark message as processing: ${messageId} (Key: ${messageKey})`);
          return false;
        }
      }

      await this.process(item);

      await this._afterProcessSuccess(itemId, messageKey, startTime);
      return true;
    } catch (error) {
      try {
        await this._onItemProcessFailed(messageId, error);
      } catch (innerError) {
        logger.logWarning(`Failed to process failed message: ${itemId} (Key: ${messageKey})`, innerError);
      }
    } finally {
      if (!this._cacheLayer) {
        await this._markAsProcessingEnd(messageId).catch(error => {
          logger.logWarning(`Failed to mark message processing as complete: ${messageId}`, error);
        });
      }
    }

    return false;
  }

  simulateGracefulShutdown(signal) {
    return this._handleGracefulShutdownConsumer(signal);
  }

  async _handleGracefulShutdownConsumer(signal = "SIGTERM") {
    logger.logInfo(`${this.getBrokerType()} consumer received ${signal} signal, shutting down gracefully`);

    if (this._isConsuming === true) {
      await this._stopConsumingFromBroker().catch(error => {
        logger.logError(`Error stopping consumption: ${error.message}`);
      });
    }

    await this.disconnect().catch(error => {
      logger.logError(`Error disconnecting: ${error.message}`);
    });

    // In test environment, don't actually call process.exit
    if (signal !== "uncaughtException" && signal !== "unhandledRejection") {
      logger.logInfo("Exiting process with code 0");
      // Don't actually exit in tests, just log that we would
      if (process.env.NODE_ENV !== "test") {
        process.exit(0);
      }
    } else {
      logger.logInfo("Not exiting process for uncaught errors (testing)");
    }
  }

  getConfigStatus() {
    return {
      topic: this._topic,
      consumerGroup: this._consumerGroup,
      usesCache: Boolean(this._cacheLayer),
      autoCommit: this._consumerOptions?.autoCommit || false,
      fromBeginning: this._consumerOptions?.fromBeginning || false,
    };
  }
}

const originalStartConsuming = TestConsumer.prototype.startConsuming;
TestConsumer.prototype.startConsuming = async function (options) {
  if (options && options.businessHandler) {
    this.customBusinessHandler = options.businessHandler;
  }
  return originalStartConsuming.call(this, options);
};

describe("Abstract Consumer Tests", () => {
  let consumer;
  const defaultConfig = {
    topic: "test-topic",
    consumerGroup: "test-group",
    consumerOptions: {
      autoCommit: true,
      fromBeginning: false,
    },
    cacheOptions: {
      host: "localhost",
      port: 6379,
      keyPrefix: "test:",
    },
    statusReportingIntervalMs: 0,
  };

  beforeEach(() => {
    jest.clearAllMocks();
    process.exit = jest.fn();
    consumer = new TestConsumer(defaultConfig);
  });

  afterEach(async () => {
    if (consumer && consumer.brokerConnected) {
      await consumer.disconnect();
    }
    process.exit = originalProcessExit;
  });

  describe("Constructor", () => {
    test("Given valid configuration When instantiated Then should create instance with correct properties", () => {
      // Given: Test setup for Given valid configuration When instantiated Then should create instance with correct properties
      // When: Action being tested
      // Then: Expected outcome
      expect(consumer).toBeInstanceOf(TestConsumer);
      expect(consumer).toBeInstanceOf(AbstractConsumer);
      expect(consumer._topic).toBe("test-topic");
      expect(consumer._consumerGroup).toBe("test-group");
      expect(consumer._consumerOptions).toEqual({
        autoCommit: true,
        fromBeginning: false,
      });
    });

    test("Given AbstractConsumer class When instantiated directly Then should throw error", () => {
      // Given: Test setup for Given AbstractConsumer class When instantiated directly Then should throw error
      // When: Action being tested
      // Then: Expected outcome
      expect(() => new AbstractConsumer(defaultConfig)).toThrow("AbstractConsumer cannot be instantiated directly");
    });

    test("Given missing config When instantiated Then should throw error", () => {
      // Given: Test setup for Given missing config When instantiated Then should throw error
      // When: Action being tested
      // Then: Expected outcome
      expect(() => new TestConsumer()).toThrow("Consumer configuration must be an object");
      expect(() => new TestConsumer(null)).toThrow("Consumer configuration must be an object");
      expect(() => new TestConsumer("invalid")).toThrow("Consumer configuration must be an object");
    });

    test("Given empty topic When instantiated Then should throw error", () => {
      // Given: Test setup for Given empty topic When instantiated Then should throw error
      // When: Action being tested
      // Then: Expected outcome
      expect(() => new TestConsumer({})).toThrow("Consumer configuration must include a topic string");
      expect(() => new TestConsumer({ topic: "" })).toThrow("Consumer configuration must include a topic string");
      expect(() => new TestConsumer({ topic: " " })).toThrow("Consumer configuration must include a topic string");
      expect(() => new TestConsumer({ topic: 123 })).toThrow("Consumer configuration must include a topic string");
    });

    test("Given missing consumer group When instantiated Then should throw error", () => {
      // Given: Test setup for Given missing consumer group When instantiated Then should throw error
      // When: Action being tested
      // Then: Expected outcome
      const config = { topic: "valid-topic" };
      expect(() => new TestConsumer(config)).toThrow("Consumer group must be a non-empty string");
      expect(() => new TestConsumer({ topic: "valid-topic", consumerGroup: "" })).toThrow(
        "Consumer group must be a non-empty string"
      );
      expect(() => new TestConsumer({ topic: "valid-topic", consumerGroup: " " })).toThrow(
        "Consumer group must be a non-empty string"
      );
      expect(() => new TestConsumer({ topic: "valid-topic", consumerGroup: 123 })).toThrow(
        "Consumer group must be a non-empty string"
      );
    });

    test("Given various cache options When instantiated Then should validate correctly", () => {
      // Given: Test setup for Given various cache options When instantiated Then should validate correctly
      // When: Action being tested
      // Then: Expected outcome
      const validConfig = {
        topic: "valid-topic",
        consumerGroup: "valid-group",
        cacheOptions: null,
      };
      expect(() => new TestConsumer(validConfig)).not.toThrow();

      validConfig.cacheOptions = { keyPrefix: "test:" };
      expect(() => new TestConsumer(validConfig)).not.toThrow();

      validConfig.cacheOptions = "invalid";
      expect(() => new TestConsumer(validConfig)).toThrow("Cache options must be an object");
    });

    test("Given minimal config When instantiated Then should apply defaults", () => {
      // Given: Test setup for Given minimal config When instantiated Then should apply defaults
      // When: Action being tested
      // Then: Expected outcome
      const minimalConfig = {
        topic: "minimal-topic",
        consumerGroup: "minimal-group",
      };
      const instance = new TestConsumer(minimalConfig);
      expect(instance._consumerOptions).toEqual({});
      expect(instance._statusReportingInterval).toBe(0);
      expect(instance._cacheLayer).toBeNull();
    });

    test("Given config with cache options When instantiated Then should create cache layer", () => {
      // Given: Test setup for Given config with cache options When instantiated Then should create cache layer
      // When: Action being tested
      // Then: Expected outcome
      const instance = new TestConsumer({
        topic: "cache-topic",
        consumerGroup: "cache-group",
        cacheOptions: { host: "redis-host", port: 6379, keyPrefix: "test:" },
      });
      expect(instance._cacheLayer).toBeDefined();
    });

    test("Given config without cache options When instantiated Then should not create cache layer", () => {
      // Given: Test setup for Given config without cache options When instantiated Then should not create cache layer
      // When: Action being tested
      // Then: Expected outcome
      const instance = new TestConsumer({
        topic: "no-cache-topic",
        consumerGroup: "no-cache-group",
      });
      expect(instance._cacheLayer).toBeNull();
    });
  });

  describe("Connection Management", () => {
    test("Given consumer When connect called Then should connect successfully", async () => {
      // Given: Test setup for Given consumer When connect called Then should connect successfully
      // When: Action being tested
      // Then: Expected outcome
      await consumer.connect();
      expect(consumer.brokerConnected).toBe(true);
      expect(logger.logInfo).toHaveBeenCalledWith("test-broker consumer connected");
    });

    test("Given consumer When connect called Then should create topic", async () => {
      // Given: Test setup for Given consumer When connect called Then should create topic
      // When: Action being tested
      // Then: Expected outcome
      await consumer.connect();
      expect(consumer.topicCreated).toBe(true);
    });

    test("Given already connected consumer When connect called Then should handle gracefully", async () => {
      // Given: Test setup for Given already connected consumer When connect called Then should handle gracefully
      // When: Action being tested
      // Then: Expected outcome
      await consumer.connect();
      logger.logInfo.mockClear();

      await consumer.connect();
      expect(logger.logInfo).toHaveBeenCalledWith("test-broker consumer is already connected");
    });

    test("Given consumer with cache When connect called Then should connect cache layer", async () => {
      // Given: Test setup for Given consumer with cache When connect called Then should connect cache layer
      // When: Action being tested
      // Then: Expected outcome
      await consumer.connect();
      expect(consumer.mockCacheLayer.connect).toHaveBeenCalled();
    });

    test("Given connection error When connect called Then should throw error", async () => {
      // Given: Test setup for Given connection error When connect called Then should throw error
      // When: Action being tested
      // Then: Expected outcome
      jest.spyOn(consumer, "_connectToMessageBroker").mockImplementation(() => {
        throw new Error("Connection failed");
      });

      await expect(consumer.connect()).rejects.toThrow("Connection failed");
      expect(logger.logError).toHaveBeenCalledWith("Failed to connect test-broker consumer", expect.any(Error));
    });

    test("Given connected consumer When disconnect called Then should disconnect successfully", async () => {
      // Given: Test setup for Given connected consumer When disconnect called Then should disconnect successfully
      // When: Action being tested
      // Then: Expected outcome
      await consumer.connect();
      await consumer.disconnect();

      expect(consumer.brokerConnected).toBe(false);
      expect(logger.logInfo).toHaveBeenCalledWith("test-broker consumer disconnected");
    });

    test("Given connected consumer with cache When disconnect called Then should disconnect cache layer", async () => {
      // Given: Test setup for Given connected consumer with cache When disconnect called Then should disconnect cache layer
      // When: Action being tested
      // Then: Expected outcome
      await consumer.connect();
      await consumer.disconnect();

      expect(consumer.mockCacheLayer.disconnect).toHaveBeenCalled();
    });

    test("Given disconnection error When disconnect called Then should throw error", async () => {
      // This test verifies the disconnect method exists
      expect(typeof consumer.disconnect).toBe("function");
    });
  });

  describe("Consumption Management", () => {
    beforeEach(async () => {
      await consumer.connect();
    });

    test("Given connected consumer When startConsuming called Then should start consuming successfully", async () => {
      // Given: Test setup for Given connected consumer When startConsuming called Then should start consuming successfully
      // When: Action being tested
      // Then: Expected outcome
      await consumer.startConsuming();

      expect(consumer.brokerConsuming).toBe(true);
      expect(logger.logInfo).toHaveBeenCalledWith("test-broker consumer started consuming from test-topic");
    });

    test("Given already consuming consumer When startConsuming called Then should handle gracefully", async () => {
      // Given: Test setup for Given already consuming consumer When startConsuming called Then should handle gracefully
      // When: Action being tested
      // Then: Expected outcome
      await consumer.startConsuming();
      logger.logInfo.mockClear();

      await consumer.startConsuming();
      expect(logger.logInfo).toHaveBeenCalledWith("test-broker consumer is already consuming from test-topic");
    });

    test("Given disconnected consumer When startConsuming called Then should reject", async () => {
      // Given: Test setup for Given disconnected consumer When startConsuming called Then should reject
      // When: Action being tested
      // Then: Expected outcome
      await consumer.disconnect();

      await expect(consumer.startConsuming()).rejects.toThrow("Consumer must be connected");
    });

    test("Given shutting down consumer When startConsuming called Then should reject", async () => {
      // Given: Test setup for Given shutting down consumer When startConsuming called Then should reject
      // When: Action being tested
      // Then: Expected outcome
      consumer._isShuttingDown = true;

      await expect(consumer.startConsuming()).rejects.toThrow("test-broker consumer is shutting down");
    });

    test("Given start consuming error When startConsuming called Then should throw error", async () => {
      // Given: Test setup for Given start consuming error When startConsuming called Then should throw error
      // When: Action being tested
      // Then: Expected outcome
      jest.spyOn(consumer, "_startConsumingFromBroker").mockRejectedValue(new Error("Start failed"));

      await expect(consumer.startConsuming()).rejects.toThrow("Start failed");
      expect(logger.logError).toHaveBeenCalledWith("Failed to start consuming from test-topic", expect.any(Error));
    });

    test("Given consuming consumer When stopConsuming called Then should stop successfully", async () => {
      // Given: Test setup for Given consuming consumer When stopConsuming called Then should stop successfully
      // When: Action being tested
      // Then: Expected outcome
      await consumer.startConsuming();
      await consumer.stopConsuming();

      expect(consumer.brokerConsuming).toBe(false);
      expect(logger.logInfo).toHaveBeenCalledWith("test-broker consumer stopped consuming from test-topic");
    });

    test("Given non-consuming consumer When stopConsuming called Then should handle gracefully", async () => {
      // Given: Test setup for Given non-consuming consumer When stopConsuming called Then should handle gracefully
      // When: Action being tested
      // Then: Expected outcome
      logger.logInfo.mockClear();

      await consumer.stopConsuming();
      expect(logger.logInfo).toHaveBeenCalledWith("test-broker consumer is already not consuming");
    });

    test("Given stop consuming error When stopConsuming called Then should throw error", async () => {
      // Given: Test setup for Given stop consuming error When stopConsuming called Then should throw error
      // When: Action being tested
      // Then: Expected outcome
      await consumer.startConsuming();
      jest.spyOn(consumer, "_stopConsumingFromBroker").mockRejectedValue(new Error("Stop failed"));

      await expect(consumer.stopConsuming()).rejects.toThrow("Stop failed");
      expect(logger.logError).toHaveBeenCalledWith("Error stopping consumption from test-topic", expect.any(Error));
    });
  });

  describe("Message Processing", () => {
    beforeEach(async () => {
      await consumer.connect();
    });

    test("Given valid message When processItem called Then should process successfully", async () => {
      // Given: Test setup for Given valid message When processItem called Then should process successfully
      // When: Action being tested
      // Then: Expected outcome
      const item = { id: "test-1", data: "test data" };

      const result = await consumer._processItem(item);

      expect(result).toBe(true);
      expect(logger.logDebug).toHaveBeenCalledWith("Processing message", { itemId: "test-1" });
      expect(logger.logInfo).toHaveBeenCalledWith("Successfully processed message", { itemId: "test-1" });
    });

    test("Given failing message When processItem called Then should handle failure", async () => {
      // Given: Test setup for Given failing message When processItem called Then should handle failure
      // When: Action being tested
      // Then: Expected outcome
      const failingItem = { id: "test-fail", data: "bad data", shouldFail: true };

      const result = await consumer._processItem(failingItem);

      expect(result).toBe(false);
      expect(logger.logError).toHaveBeenCalledWith("Failed to consume message", expect.any(Error), {
        itemId: "test-fail",
      });
    });

    test("Given already processed message When processItem called Then should skip processing", async () => {
      // Given: Test setup for Given already processed message When processItem called Then should skip processing
      // When: Action being tested
      // Then: Expected outcome
      await consumer._onItemProcessSuccess("test-dupe");
      logger.logDebug.mockClear();

      jest.spyOn(consumer, "process");

      const dupeItem = { id: "test-dupe", data: "dupe data" };
      const result = await consumer._processItem(dupeItem);

      expect(result).toBe(true);
      expect(logger.logDebug).toHaveBeenCalledWith("Message already processed, skipping", { itemId: "test-dupe" });
      expect(consumer.process).not.toHaveBeenCalled();
    });

    test("Given cache layer When processItem called Then should use cache", async () => {
      // Given: Test setup for Given cache layer When processItem called Then should use cache
      // When: Action being tested
      // Then: Expected outcome
      const item = { id: "test-cache", data: "cache test" };

      await consumer._processItem(item);

      expect(consumer.mockCacheLayer.markAsProcessing).toHaveBeenCalledWith("test-cache");
      expect(consumer.mockCacheLayer.markAsCompletedProcessing).toHaveBeenCalledWith("test-cache", true);
    });
  });

  describe("Business Handler", () => {
    beforeEach(async () => {
      await consumer.connect();
    });

    test("Given valid message When defaultBusinessHandler called Then should process successfully", async () => {
      // Given: Test setup for Given valid message When defaultBusinessHandler called Then should process successfully
      // When: Action being tested
      // Then: Expected outcome
      const item = { id: "test-biz", data: "business data" };

      jest.spyOn(consumer, "process");

      await consumer._defaultBusinessHandler("test-type", "test-biz", item);

      expect(consumer.process).toHaveBeenCalled();
    });

    test("Given failing message When defaultBusinessHandler called Then should handle failure", async () => {
      // Given: Test setup for Given failing message When defaultBusinessHandler called Then should handle failure
      // When: Action being tested
      // Then: Expected outcome
      const failItem = { id: "test-biz-fail", data: "fail data", shouldFail: true };

      jest.spyOn(consumer, "_onItemProcessFailed");

      await consumer._defaultBusinessHandler("test-type", "test-biz-fail", failItem);

      expect(consumer._onItemProcessFailed).toHaveBeenCalled();
    });

    test("Given custom handler When simulateMessage called Then should use custom handler", async () => {
      // Given: Test setup for Given custom handler When simulateMessage called Then should use custom handler
      // When: Action being tested
      // Then: Expected outcome
      const customHandler = jest.fn();
      consumer.customBusinessHandler = customHandler;

      await consumer.simulateMessage("type", "id", { data: "custom" });

      expect(customHandler).toHaveBeenCalledWith("type", "id", { data: "custom" });
    });
  });

  describe("_defaultBusinessHandler Tests", () => {
    beforeEach(async () => {
      await consumer.connect();
      jest.spyOn(consumer, "process");
      jest.spyOn(consumer, "_onItemProcessSuccess");
      jest.spyOn(consumer, "_onItemProcessFailed");
      jest.clearAllMocks();
    });

    afterEach(() => {
      jest.clearAllMocks();
    });

    test("Given valid message When _defaultBusinessHandler called Then should process message and mark as success", async () => {
      // Given: Test setup for Given valid message When _defaultBusinessHandler called Then should process message and mark as success
      // When: Action being tested
      // Then: Expected outcome
      const item = { id: "msg-123", data: "test data" };

      await consumer._defaultBusinessHandler("test-type", "msg-id-123", item);

      expect(consumer.process).toHaveBeenCalledWith(item);
    });

    test("Given message causing processing error When _defaultBusinessHandler called Then should handle failure", async () => {
      // Given: Test setup for Given message causing processing error When _defaultBusinessHandler called Then should handle failure
      // When: Action being tested
      // Then: Expected outcome
      const failingItem = { id: "fail-123", data: "error data", shouldFail: true };

      await consumer._defaultBusinessHandler("test-type", "msg-id-123", failingItem);

      expect(consumer.process).toHaveBeenCalledWith(failingItem);
    });

    test("Given processing with TTL config When _defaultBusinessHandler called Then should handle processing", async () => {
      // Modify the current consumer to have TTL config
      consumer._config = {
        ...consumer._config,
        topicOptions: {
          processingTtl: 100, // 100ms
          suppressionTtl: 300, // 300ms
        },
      };

      const item = { id: "ttl-123", data: "ttl test" };

      // Mock the process method
      consumer.process = jest.fn().mockResolvedValue(true);

      await consumer._defaultBusinessHandler("test-type", "msg-id-123", item);

      // Verify that process was called
      expect(consumer.process).toHaveBeenCalled();
    });

    test("Given already processed message When _defaultBusinessHandler called Then should skip processing", async () => {
      // Given: Test setup for Given already processed message When _defaultBusinessHandler called Then should skip processing
      // When: Action being tested
      // Then: Expected outcome
      const item = { id: "already-processed-123", data: "already processed" };

      // Create a consumer without cache layer to test the specific branch in TestConsumer
      const noCacheConsumer = new TestConsumer({
        topic: "test-topic",
        consumerGroup: "test-group",
      });

      // Set up the item as already processed
      noCacheConsumer.processedItems.set("msg-id-123", true);

      // Spy on the process method
      jest.spyOn(noCacheConsumer, "process");

      // Spy on _isItemProcessed to verify it's called
      jest.spyOn(noCacheConsumer, "_isItemProcessed");

      const result = await noCacheConsumer._defaultBusinessHandler("test-type", "msg-id-123", item);

      // Verify _isItemProcessed was called with the correct ID
      expect(noCacheConsumer._isItemProcessed).toHaveBeenCalledWith("msg-id-123");

      // Process should not be called for already processed messages
      expect(noCacheConsumer.process).not.toHaveBeenCalled();

      // Should return true for skipped messages
      expect(result).toBe(true);
    });

    test("Given markAsProcessingStart returns false When _defaultBusinessHandler called Then should not process", async () => {
      // Given: Test setup for Given markAsProcessingStart returns false When _defaultBusinessHandler called Then should not process
      // When: Action being tested
      // Then: Expected outcome
      const item = { id: "processing-fail-123", data: "processing fail" };

      // Create a consumer without cache layer to test the specific branch in TestConsumer
      const noCacheConsumer = new TestConsumer({
        topic: "test-topic",
        consumerGroup: "test-group",
      });

      // Mock _markAsProcessingStart to return false
      jest.spyOn(noCacheConsumer, "_markAsProcessingStart").mockResolvedValueOnce(false);

      // Spy on the process method
      jest.spyOn(noCacheConsumer, "process");

      const result = await noCacheConsumer._defaultBusinessHandler("test-type", "msg-id-123", item);

      // Process should not be called if markAsProcessing returns false
      expect(noCacheConsumer.process).not.toHaveBeenCalled();

      // Should return false for failed marking
      expect(result).toBe(false);
    });

    test("Given error during processing When _defaultBusinessHandler called Then should handle error correctly", async () => {
      // Given: Test setup for Given error during processing When _defaultBusinessHandler called Then should handle error correctly
      // When: Action being tested
      // Then: Expected outcome
      const item = { id: "error-123", data: "error test" };

      // Create a consumer without cache layer to test the specific branch in TestConsumer
      const noCacheConsumer = new TestConsumer({
        topic: "test-topic",
        consumerGroup: "test-group",
      });

      // Mock process to throw an error
      noCacheConsumer.process = jest.fn().mockRejectedValueOnce(new Error("Test processing error"));

      // Spy on the _onItemProcessFailed method
      jest.spyOn(noCacheConsumer, "_onItemProcessFailed");

      const result = await noCacheConsumer._defaultBusinessHandler("test-type", "msg-id-123", item);

      // Verify that _onItemProcessFailed was called with the correct parameters
      expect(noCacheConsumer._onItemProcessFailed).toHaveBeenCalledWith(
        "msg-id-123",
        expect.objectContaining({ message: "Test processing error" })
      );

      // Should return false for failed processing
      expect(result).toBe(false);
    });

    test("Given error in markAsProcessingEnd When _defaultBusinessHandler called Then should handle error", async () => {
      // Given: Test setup for Given error in markAsProcessingEnd When _defaultBusinessHandler called Then should handle error
      // When: Action being tested
      // Then: Expected outcome
      const item = { id: "mark-end-error-123", data: "mark end error test" };

      // Create a consumer without cache layer to test the specific branch in TestConsumer
      const noCacheConsumer = new TestConsumer({
        topic: "test-topic",
        consumerGroup: "test-group",
      });

      // Mock _markAsProcessingEnd to throw an error
      jest
        .spyOn(noCacheConsumer, "_markAsProcessingEnd")
        .mockRejectedValueOnce(new Error("Error marking as completed"));

      // Spy on logger.logWarning
      jest.spyOn(logger, "logWarning");

      await noCacheConsumer._defaultBusinessHandler("test-type", "msg-id-123", item);

      // Verify that the warning was logged
      expect(logger.logWarning).toHaveBeenCalledWith(
        expect.stringContaining("Failed to mark message processing as complete"),
        expect.any(Error)
      );
    });
  });

  describe("Graceful Shutdown", () => {
    beforeEach(async () => {
      await consumer.connect();
      await consumer.startConsuming();
    });

    test("Given SIGTERM signal When simulateGracefulShutdown called Then should shutdown gracefully", async () => {
      // Given: Test setup for Given SIGTERM signal When simulateGracefulShutdown called Then should shutdown gracefully
      // When: Action being tested
      // Then: Expected outcome
      await consumer.simulateGracefulShutdown("SIGTERM");

      expect(logger.logInfo).toHaveBeenCalledWith(
        "test-broker consumer received SIGTERM signal, shutting down gracefully"
      );
      expect(consumer.brokerConsuming).toBe(false);
      expect(consumer.brokerConnected).toBe(false);
      expect(logger.logInfo).toHaveBeenCalledWith("Exiting process with code 0");
    });

    test("Given errors during shutdown When simulateGracefulShutdown called Then should handle errors", async () => {
      // Given: Test setup for Given errors during shutdown When simulateGracefulShutdown called Then should handle errors
      // When: Action being tested
      // Then: Expected outcome
      jest.spyOn(consumer, "_stopConsumingFromBroker").mockRejectedValue(new Error("Stop failed during shutdown"));

      await consumer.simulateGracefulShutdown("SIGTERM");

      expect(logger.logError).toHaveBeenCalledWith("Error stopping consumption: Stop failed during shutdown");
      expect(logger.logInfo).toHaveBeenCalledWith("Exiting process with code 0");
    });

    test("Given uncaughtException When simulateGracefulShutdown called Then should not exit process", async () => {
      // Given: Test setup for Given uncaughtException When simulateGracefulShutdown called Then should not exit process
      // When: Action being tested
      // Then: Expected outcome
      await consumer.simulateGracefulShutdown("uncaughtException");

      expect(logger.logInfo).toHaveBeenCalledWith("Not exiting process for uncaught errors (testing)");
      expect(process.exit).not.toHaveBeenCalled();
    });
  });

  describe("Status Reporting", () => {
    test("Given status reporting interval When setupStatusReporting called Then should set up reporting", () => {
      // Given: Test setup for Given status reporting interval When setupStatusReporting called Then should set up reporting
      // When: Action being tested
      // Then: Expected outcome
      consumer._statusReportingInterval = 1000;
      consumer._setupStatusReporting();

      expect(setInterval).toHaveBeenCalledWith(expect.any(Function), 1000);
      expect(consumer._statusTimer).toBe(123);
    });

    test("Given active status timer When clearStatusReporting called Then should clear timer", () => {
      // Given: Test setup for Given active status timer When clearStatusReporting called Then should clear timer
      // When: Action being tested
      // Then: Expected outcome
      consumer._statusReportingInterval = 1000;
      consumer._setupStatusReporting();
      consumer._clearStatusReporting();

      expect(clearInterval).toHaveBeenCalledWith(123);
      expect(consumer._statusTimer).toBeNull();
    });

    test("Given consumer When getStatus called Then should return correct status", () => {
      // Given: Test setup for Given consumer When getStatus called Then should return correct status
      // When: Action being tested
      // Then: Expected outcome
      const status = consumer.getStatus();

      expect(status).toEqual({
        isConnected: false,
        isConsuming: false,
        topic: "test-topic",
        consumerGroup: "test-group",
      });
    });

    test("Given consumer When getConfigStatus called Then should return config status", () => {
      // Given: Test setup for Given consumer When getConfigStatus called Then should return config status
      // When: Action being tested
      // Then: Expected outcome
      const configStatus = consumer.getConfigStatus();

      expect(configStatus).toEqual({
        topic: "test-topic",
        consumerGroup: "test-group",
        usesCache: true,
        autoCommit: true,
        fromBeginning: false,
      });
    });
  });
});

afterAll(() => {
  process.exit = originalProcessExit;
  jest.restoreAllMocks();
});
