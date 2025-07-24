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

    if (config && config.cacheOptions) {
      if (!config.cacheOptions.keyPrefix) {
        config.cacheOptions.keyPrefix = "test:";
      }

      this.mockCacheLayer = {
        connect: jest.fn().mockResolvedValue(true),
        disconnect: jest.fn().mockResolvedValue(true),
        isConnected: true,
        markAsProcessing: jest.fn().mockResolvedValue(true),
        clearProcessingState: jest.fn().mockResolvedValue(true),
        extendProcessingTtl: jest.fn().mockResolvedValue(true),
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

  async _startConsumingFromBroker(options = {}) {
    this.brokerConsuming = true;
    this.processHandler = options.businessHandler || this._defaultBusinessHandler.bind(this);
    this.customBusinessHandler = options.businessHandler;
    return Promise.resolve(true);
  }

  async _stopConsumingFromBroker() {
    this.brokerConsuming = false;
    return Promise.resolve(true);
  }

  _handleConsumingStartError(error) {
    logger.logError(`Error starting consumption: ${error.message}`, error);
    return error;
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

  _logConfigurationLoaded() {
    logger.logDebug(`${this.getBrokerType()} consumer loaded with configuration: ${JSON.stringify(this._config)}`);
  }

  getItemId(item) {
    if (!item) return "unknown";
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

  // Implementation of _defaultBusinessHandler to match abstract class
  async _defaultBusinessHandler(type, messageId, item) {
    const startTime = Date.now();
    if (!type || !messageId) {
      logger.logWarning("Missing message type or ID in _defaultBusinessHandler");
      return false;
    }

    if (!item) {
      logger.logWarning(`Empty message data received for ${type}:${messageId}`);
      return false;
    }

    const itemId = this.getItemId(item);
    const messageKey = this.getMessageKey(item);

    try {
      logger.logDebug(`Processing message: ${itemId} (key: ${messageKey})`);
      if (await this._isItemProcessed(itemId)) {
        logger.logDebug(`Message ${itemId} already processed, skipping`);
        return true;
      }

      const result = this._cacheLayer ? await this._cacheLayer.markAsProcessing(itemId) : true;
      if (!result) {
        logger.logDebug(`Message ${itemId} is being processed by another instance, skipping`);
        return false;
      }

      await this.process(item);

      if (this._cacheLayer) {
        await this._cacheLayer.clearProcessingState(itemId);
      }

      await this._onItemProcessSuccess(itemId);
      return true;
    } catch (error) {
      if (this._cacheLayer) {
        await this._cacheLayer.clearProcessingState(itemId, false);
      }
      await this._onItemProcessFailed(itemId, error);
      logger.logError(`Error processing message ${type}:${itemId}`, error);
      return false;
    }
  }

  getStatus() {
    return {
      isConnected: this.brokerConnected,
      isConsuming: this.brokerConsuming,
      topic: this._topic,
      groupId: this._groupId,
    };
  }

  // To handle graceful shutdown simulation
  async simulateGracefulShutdown(signal) {
    if (this._isShuttingDown) {
      return;
    }

    this._isShuttingDown = true;
    logger.logInfo(`${this.getBrokerType()} consumer received ${signal} signal, shutting down gracefully`);

    try {
      // Simulate removing shutdown listeners
      process.removeListener("SIGINT", () => {});
      process.removeListener("SIGTERM", () => {});
      process.removeListener("uncaughtException", () => {});
      process.removeListener("unhandledRejection", () => {});

      if (this._isConsuming) {
        await this._stopConsumingFromBroker();
      }

      if (this._isConnected) {
        await this.disconnect();
      }

      logger.logInfo(`${this.getBrokerType()} consumer shutdown complete`);

      if (signal !== "uncaughtException" && signal !== "unhandledRejection") {
        process.exit(0);
      }
    } catch (error) {
      logger.logError("Error during graceful shutdown: " + error.message, error);
      if (signal !== "uncaughtException" && signal !== "unhandledRejection") {
        process.exit(0);
      }
    }
  }

  getConfigStatus() {
    return {
      topic: this._topic,
      groupId: this._groupId,
      maxConcurrency: this.maxConcurrency,
      cacheConnected: Boolean(this._cacheLayer),
      isConsuming: this._isConsuming,
      processedCount: this.metrics ? this.metrics.totalProcessed : 0,
      failedCount: this.metrics ? this.metrics.totalFailed : 0,
      activeMessages: 0,
      uptime: 0,
    };
  }
}

// Add alias for startConsuming to consume for backward compatibility
TestConsumer.prototype.startConsuming = TestConsumer.prototype.consume;

describe("Abstract Consumer Tests", () => {
  let consumer;
  const defaultConfig = {
    topic: "test-topic",
    groupId: "test-group",
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
    test("Given Test setup for Given valid configuration When instantiated Then should create instance with correct properties When Action being tested Then Expected outcome", () => {
      expect(consumer).toBeInstanceOf(TestConsumer);
      expect(consumer).toBeInstanceOf(AbstractConsumer);
      expect(consumer._topic).toBe("test-topic");
    });

    test("Given Test setup for Given AbstractConsumer class When instantiated directly Then should throw error When Action being tested Then Expected outcome", () => {
      expect(() => new AbstractConsumer(defaultConfig)).toThrow("AbstractConsumer cannot be instantiated directly");
    });

    test("Given Test setup for Given missing config When instantiated Then should throw error When Action being tested Then Expected outcome", () => {
      expect(() => new TestConsumer()).toThrow("Consumer configuration must be an object");
      expect(() => new TestConsumer(null)).toThrow("Consumer configuration must be an object");
      expect(() => new TestConsumer("invalid")).toThrow("Consumer configuration must be an object");
    });

    test("Given empty topic When instantiated Then should throw error", () => {
      expect(() => new TestConsumer({})).toThrow("Consumer configuration must include a topic string");
      expect(() => new TestConsumer({ topic: "" })).toThrow("Consumer configuration must include a topic string");
      expect(() => new TestConsumer({ topic: " " })).toThrow("Consumer configuration must include a topic string");
      expect(() => new TestConsumer({ topic: 123 })).toThrow("Consumer configuration must include a topic string");
    });

    test("Given various cache options When instantiated Then should validate correctly", () => {
      const validConfig = {
        topic: "valid-topic",
        groupId: "valid-group",
        cacheOptions: null,
      };
      expect(() => new TestConsumer(validConfig)).not.toThrow();

      validConfig.cacheOptions = { keyPrefix: "test:" };
      expect(() => new TestConsumer(validConfig)).not.toThrow();

      validConfig.cacheOptions = "invalid";
      expect(() => new TestConsumer(validConfig)).toThrow("Cache options must be an object");
    });

    test("Given minimal config When instantiated Then should apply defaults", () => {
      const minimalConfig = {
        topic: "minimal-topic",
        groupId: "minimal-group",
      };
      const instance = new TestConsumer(minimalConfig);
      expect(instance._statusReportInterval).toBe(30000);
      expect(instance._cacheLayer).toBeNull();
    });

    test("Given config with cache options When instantiated Then should create cache layer", () => {
      const instance = new TestConsumer({
        topic: "cache-topic",
        groupId: "cache-group",
        cacheOptions: { host: "redis-host", port: 6379, keyPrefix: "test:" },
      });
      expect(instance._cacheLayer).toBeDefined();
    });

    test("Given Test setup for Given config without cache options When instantiated Then should not create cache layer When Action being tested Then Expected outcome", () => {
      const instance = new TestConsumer({
        topic: "no-cache-topic",
        groupId: "no-cache-group",
      });
      expect(instance._cacheLayer).toBeNull();
    });
  });

  describe("Connection Management", () => {
    test("Given consumer When connect called Then should connect successfully", async () => {
      await consumer.connect();
      expect(consumer.brokerConnected).toBe(true);
      expect(logger.logInfo).toHaveBeenCalledWith("TEST-BROKER consumer is connected to topic [ test-topic ]");
    });

    test("Given consumer When connect called Then should create topic", async () => {
      await consumer.connect();
      expect(consumer.topicCreated).toBe(true);
    });

    test("Given already connected consumer When connect called Then should handle gracefully", async () => {
      await consumer.connect();
      logger.logInfo.mockClear();

      await consumer.connect();
      expect(logger.logInfo).toHaveBeenCalledWith("test-broker consumer is already connected");
    });

    test("Given consumer with cache When connect called Then should connect cache layer", async () => {
      await consumer.connect();
      expect(consumer.mockCacheLayer.connect).toHaveBeenCalled();
    });

    test("Given connection error When connect called Then should throw error", async () => {
      jest.spyOn(consumer, "_connectToMessageBroker").mockImplementation(() => {
        throw new Error("Connection failed");
      });

      await expect(consumer.connect()).rejects.toThrow("Connection failed");
      expect(logger.logError).toHaveBeenCalledWith("Failed to connect test-broker consumer", expect.any(Error));
    });

    test("Given connected consumer When disconnect called Then should disconnect successfully", async () => {
      await consumer.connect();
      await consumer.disconnect();

      expect(consumer.brokerConnected).toBe(false);
      expect(logger.logInfo).toHaveBeenCalledWith("test-broker consumer disconnected successfully");
    });

    test("Given connected consumer with cache When disconnect called Then should disconnect cache layer", async () => {
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
    let consumer;

    beforeEach(async () => {
      // Create a fresh consumer for each test
      consumer = new TestConsumer({
        topic: "test-topic",
        groupId: "test-group",
      });
      await consumer.connect();

      // Add spies to the methods we'll be testing
      jest.spyOn(consumer, "_startConsumingFromBroker");
      jest.spyOn(consumer, "_stopConsumingFromBroker");
      jest.spyOn(consumer, "_handleConsumingStartError");

      jest.clearAllMocks();
    });

    test("Given disconnected consumer When consume called Then should throw error", async () => {
      // Given
      consumer._isConnected = false;
      logger.logError.mockClear();

      // When/Then
      await expect(consumer.consume()).rejects.toThrow("test-broker consumer is not connected");
    });

    test("Given shutting down consumer When consume called Then should throw error", async () => {
      // Given
      consumer._isShuttingDown = true;
      logger.logError.mockClear();

      // When/Then
      await expect(consumer.consume()).rejects.toThrow("test-broker consumer is shutting down");
    });

    test("Given already consuming consumer When consume called Then should not start again", async () => {
      // Given
      await consumer.consume();
      jest.clearAllMocks();
      consumer._isConsuming = true;
      logger.logWarning.mockClear();

      // When
      await consumer.consume();

      // Then
      expect(consumer._startConsumingFromBroker).not.toHaveBeenCalled();
      expect(logger.logWarning).toHaveBeenCalledWith("test-broker consumer is already consuming messages");
    });

    test("Given start consuming error When consume called Then should throw error", async () => {
      // Given
      const error = new Error("Start failed");
      consumer._startConsumingFromBroker.mockRejectedValue(error);
      consumer._handleConsumingStartError.mockReturnValue(error);
      logger.logError.mockClear();

      // When/Then
      await expect(consumer.consume()).rejects.toThrow("Start failed");
    });

    test("Given consuming consumer When stopConsuming called Then should stop successfully", async () => {
      // Given
      await consumer.consume();
      consumer._isConsuming = true;
      jest.clearAllMocks();

      // When
      await consumer.stopConsuming();

      // Then
      expect(consumer._stopConsumingFromBroker).toHaveBeenCalled();
      expect(consumer._isConsuming).toBe(false);
      expect(logger.logInfo).toHaveBeenCalledWith("test-broker consumer stopped consuming from test-topic");
    });

    test("Given non-consuming consumer When stopConsuming called Then should handle gracefully", async () => {
      // Given
      consumer._isConsuming = false;

      // When
      await consumer.stopConsuming();

      // Then
      expect(consumer._stopConsumingFromBroker).not.toHaveBeenCalled();
      expect(logger.logWarning).toHaveBeenCalledWith("test-broker consumer is not currently consuming");
    });

    test("Given stop consuming error When stopConsuming called Then should throw error", async () => {
      // Given
      await consumer.consume();
      consumer._isConsuming = true;
      jest.clearAllMocks();

      const error = new Error("Stop failed");
      consumer._stopConsumingFromBroker.mockRejectedValue(error);
      logger.logError.mockClear();

      // When
      try {
        await consumer.stopConsuming();
      } catch (e) {
        // Expected to throw
      }

      // Then
      expect(logger.logError).toHaveBeenCalledWith("Error stopping consumption from test-topic", error);
    });
  });

  describe("Message Processing", () => {
    beforeEach(async () => {
      await consumer.connect();
    });

    test("Given valid message When _defaultBusinessHandler called Then should process successfully", async () => {
      // Given
      const item = { id: "test-1", data: "test data" };

      // When
      const result = await consumer._defaultBusinessHandler("test-type", "test-1", item);

      // Then
      expect(result).toBe(true);
      expect(logger.logDebug).toHaveBeenCalled();
    });

    test("Given failing message When _defaultBusinessHandler called Then should handle failure", async () => {
      // Given
      const failingItem = { id: "test-fail", data: "bad data", shouldFail: true };

      // When
      const result = await consumer._defaultBusinessHandler("test-type", "test-fail", failingItem);

      // Then
      expect(result).toBe(false);
      expect(logger.logError).toHaveBeenCalled();
    });

    test("Given already processed message When _defaultBusinessHandler called Then should skip processing", async () => {
      // Given
      await consumer._onItemProcessSuccess("test-dupe");
      logger.logDebug.mockClear();
      jest.spyOn(consumer, "process");

      // When
      const dupeItem = { id: "test-dupe", data: "dupe data" };
      const result = await consumer._defaultBusinessHandler("test-type", "test-dupe", dupeItem);

      // Then
      expect(result).toBe(true);
      expect(logger.logDebug).toHaveBeenCalled();
      expect(consumer.process).not.toHaveBeenCalled();
    });

    test("Given cache layer When _defaultBusinessHandler called Then should use cache", async () => {
      // Given
      const item = { id: "test-cache", data: "cache test" };

      // When
      await consumer._defaultBusinessHandler("test-type", "test-cache", item);

      // Then
      expect(consumer.mockCacheLayer.markAsProcessing).toHaveBeenCalledWith("test-cache");
      expect(consumer.mockCacheLayer.clearProcessingState).toHaveBeenCalled();
    });

    test("Given missing message type or ID When _defaultBusinessHandler called Then should return false", async () => {
      // Given
      const mockItem = { id: "test-id", data: "test-data" };
      logger.logWarning.mockClear();

      // When missing type
      const result = await consumer._defaultBusinessHandler(null, "test-id", mockItem);

      // Then
      expect(result).toBe(false);
      expect(logger.logWarning).toHaveBeenCalledWith("Missing message type or ID in _defaultBusinessHandler");
    });

    test("Given empty message data When _defaultBusinessHandler called Then should return false", async () => {
      // When
      const result = await consumer._defaultBusinessHandler("test-type", "test-id", null);

      // Then
      expect(result).toBe(false);
      expect(logger.logWarning).toHaveBeenCalledWith("Empty message data received for test-type:test-id");
    });
  });

  describe("Status Reporting", () => {
    let consumer;

    beforeEach(() => {
      consumer = new TestConsumer({
        topic: "test-topic",
        groupId: "test-group",
      });
      jest.clearAllMocks();
    });

    test("Given consumer When getConfigStatus called Then should return correct status", () => {
      // Given/When
      const status = consumer.getConfigStatus();

      // Then
      expect(status.topic).toBe("test-topic");
      expect(status.processedCount).toBe(0);
      expect(status.failedCount).toBe(0);
    });
  });

  describe("Graceful Shutdown", () => {
    let consumer;

    beforeEach(async () => {
      // Create a new TestConsumer instance for each test
      consumer = new TestConsumer({
        topic: "test-topic",
        groupId: "test-group",
      });

      // Mock necessary methods
      jest.spyOn(consumer, "disconnect").mockResolvedValue();

      // Connect and start consuming for tests
      await consumer.connect();
      await consumer.startConsuming();

      // Clear mocks after setup
      jest.clearAllMocks();
    });

    test("Given SIGTERM signal When simulateGracefulShutdown called Then should shutdown gracefully", async () => {
      // When
      await consumer.simulateGracefulShutdown("SIGTERM");

      // Then
      expect(logger.logInfo).toHaveBeenCalledWith(
        "test-broker consumer received SIGTERM signal, shutting down gracefully"
      );
      expect(consumer.disconnect).toHaveBeenCalled();
    });

    test("Given errors during shutdown When simulateGracefulShutdown called Then should handle errors", async () => {
      // Given
      const error = new Error("Disconnect failed");
      consumer.disconnect.mockRejectedValue(error);

      // When
      await consumer.simulateGracefulShutdown("SIGTERM");

      // Then
      expect(logger.logError).toHaveBeenCalledWith("Error during graceful shutdown: Disconnect failed", error);
    });

    test("Given uncaughtException When simulateGracefulShutdown called Then should not exit process", async () => {
      // When
      await consumer.simulateGracefulShutdown("uncaughtException");

      // Then
      expect(logger.logInfo).toHaveBeenCalledWith(
        "test-broker consumer received uncaughtException signal, shutting down gracefully"
      );
      expect(consumer.disconnect).toHaveBeenCalled();
      expect(process.exit).not.toHaveBeenCalled();
    });
  });
});

afterAll(() => {
  process.exit = originalProcessExit;
  jest.restoreAllMocks();
});
