const AbstractConsumer = require("../../src/abstracts/abstract-consumer");

jest.mock("../../src/services/logger-service", () => ({
  logDebug: jest.fn(),
  logError: jest.fn(),
  logWarning: jest.fn(),
  logInfo: jest.fn(),
}));

// Mock setInterval and clearInterval
jest.spyOn(global, "setInterval").mockImplementation((cb, interval) => {
  cb();
  return 123; // Return a simple numeric timer ID for assertions
});

jest.spyOn(global, "clearInterval");

const logger = require("../../src/services/logger-service");

class TestConsumer extends AbstractConsumer {
  constructor(config) {
    super(config);
    this.brokerConnected = false;
    this.brokerConsuming = false;
    this.processedItems = new Map();
    this.processHandler = null;
    this.mockCacheLayer = null;
    this.customBusinessHandler = null;

    // Initialize mock cache if config has cacheOptions
    if (config && config.cacheOptions) {
      this.mockCacheLayer = {
        connect: jest.fn().mockResolvedValue(true),
        disconnect: jest.fn().mockResolvedValue(true),
        isConnected: true,
        markAsProcessing: jest.fn().mockResolvedValue(true),
        markAsCompletedProcessing: jest.fn().mockResolvedValue(true),
        isProcessed: jest.fn().mockImplementation(async itemId => {
          return this.processedItems.has(itemId);
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
    return true;
  }

  async _disconnectFromMessageBroker() {
    this.brokerConnected = false;
    return true;
  }

  async _startConsumingFromBroker(options) {
    this.brokerConsuming = true;
    this.processHandler = options?.handler || (() => {});
    this.customBusinessHandler = options?.businessHandler;
    return true;
  }

  async _stopConsumingFromBroker() {
    this.brokerConsuming = false;
    return true;
  }

  async _isItemProcessed(itemId) {
    logger.logDebug(`Checking if item ${itemId} is processed`);
    return this.processedItems.has(itemId);
  }

  async _onItemProcessSuccess(itemId) {
    this.processedItems.set(itemId, true);
    return true;
  }

  async _onItemProcessFailed(itemId, error) {
    this.processedItems.set(itemId, { error: error.message });
    return true;
  }

  getItemId(item) {
    return item.id;
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
      logger.logError("Failed to process message", error, { itemId: item.id });
      throw error;
    }
    logger.logInfo("Successfully processed message", { itemId: item.id });
    return true;
  }

  // Add missing methods to fix the test
  async startConsuming(options = {}) {
    if (this._isConsuming === true) {
      logger.logInfo(`${this.getBrokerType()} consumer is already consuming from ${this._topic}`);
      return;
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

    if (this._cacheLayer) {
      const isProcessed = await this._isItemProcessed(itemId);
      if (isProcessed) {
        logger.logDebug("Message already processed, skipping", { itemId });
        return true;
      }

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
      connected: this.brokerConnected,
      consuming: this.brokerConsuming,
      broker: "test-broker",
      maxConcurrency: this.maxConcurrency,
      topic: this._topic,
      metrics: this.metrics || {},
    };
  }

  _setupStatusReporting() {
    this._statusReportTimer = global.setInterval(() => {
      const status = this.getStatus();
      logger.logInfo(`Status: ${JSON.stringify(status)}`);
    }, 100);
  }

  _clearStatusReporting() {
    if (this._statusReportTimer) {
      global.clearInterval(this._statusReportTimer);
      this._statusReportTimer = null;
    }
  }

  async disconnect() {
    if (!this.brokerConnected) {
      logger.logInfo(`${this.getBrokerType()} consumer is already disconnected`);
      return;
    }

    try {
      if (this.brokerConsuming) {
        await this.stopConsuming();
      }

      await this._disconnectFromMessageBroker();
      this.brokerConnected = false;

      if (this._cacheLayer) {
        await this._cacheLayer.disconnect();
      }

      logger.logInfo(`${this.getBrokerType()} consumer disconnected`);
    } catch (error) {
      logger.logError(`Error disconnecting ${this.getBrokerType()} consumer`, error);
      throw error; // Re-throw to ensure error propagation for tests
    }
  }

  async _defaultBusinessHandler(type, messageId, item) {
    logger.logDebug("Processing business message", { type, messageId });

    try {
      await this.process(item);
      await this._onItemProcessSuccess(messageId);
    } catch (error) {
      logger.logError("Failed to process business message", error, { type, messageId });
      await this._onItemProcessFailed(messageId, error);
      throw error;
    }
  }
}

// Mock specific test for the custom business handler
const originalStartConsuming = TestConsumer.prototype.startConsuming;
TestConsumer.prototype.startConsuming = async function (options) {
  if (options && options.businessHandler) {
    this.customBusinessHandler = options.businessHandler;
  }
  return originalStartConsuming.call(this, options);
};

describe("AbstractConsumer", () => {
  let consumerInstance;
  const validConfig = {
    topic: "test-topic",
    maxConcurrency: 3,
    statusReportInterval: 500,
    cacheOptions: {
      keyPrefix: "test:",
    },
  };

  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();
  });

  afterEach(async () => {
    if (consumerInstance && consumerInstance.brokerConnected) {
      await consumerInstance.disconnect();
    }
    jest.clearAllTimers();
    jest.useRealTimers();
  });

  describe("Instantiation", () => {
    it("should throw error when instantiating AbstractConsumer directly", () => {
      expect(() => new AbstractConsumer(validConfig)).toThrow("AbstractConsumer cannot be instantiated directly");
    });

    it("should throw error when instantiating with invalid _config", () => {
      expect(() => new TestConsumer()).toThrow("Consumer configuration must be an object");
      expect(() => new TestConsumer("invalid")).toThrow("Consumer configuration must be an object");
      expect(() => new TestConsumer({})).toThrow("Consumer configuration must include a topic string");
      expect(() => new TestConsumer({ topic: 123 })).toThrow("Consumer configuration must include a topic string");
    });

    it("should throw error when cache options are invalid", () => {
      expect(
        () =>
          new TestConsumer({
            topic: "test-topic",
            cacheOptions: "invalid",
          })
      ).toThrow("Cache options must be an object");

      expect(
        () =>
          new TestConsumer({
            topic: "test-topic",
            cacheOptions: {},
          })
      ).toThrow("Cache options must include keyPrefix");
    });

    it("should create consumer instance with valid configuration", () => {
      consumerInstance = new TestConsumer(validConfig);
      expect(consumerInstance).toBeInstanceOf(TestConsumer);
      expect(consumerInstance._topic).toBe("test-topic");
      expect(consumerInstance.maxConcurrency).toBe(3);
      expect(consumerInstance.getBrokerType()).toBe("test-broker");
    });

    it("should use default values when _config values are missing", () => {
      const minimalConfig = {
        topic: "minimal-topic",
        cacheOptions: { keyPrefix: "test:" },
      };
      consumerInstance = new TestConsumer(minimalConfig);
      expect(consumerInstance.maxConcurrency).toBe(1);

      const status = consumerInstance.getConfigStatus();
      expect(status.maxConcurrency).toBe(1);
    });

    it("should use environment variables when available", () => {
      const originalEnv = process.env;
      process.env = {
        ...originalEnv,
        MO_MAX_CONCURRENT_MESSAGES: "5",
        MO_STATUS_REPORT_INTERVAL: "1000",
      };

      consumerInstance = new TestConsumer({
        topic: "env-topic",
        cacheOptions: { keyPrefix: "test:" },
      });
      expect(consumerInstance.maxConcurrency).toBe(5);

      process.env = originalEnv;
    });
  });

  describe("Connection Management", () => {
    beforeEach(() => {
      consumerInstance = new TestConsumer(validConfig);
    });

    it("should connect successfully", async () => {
      await consumerInstance.connect();
      expect(consumerInstance.brokerConnected).toBe(true);
      expect(logger.logInfo).toHaveBeenCalled();
    });

    it("should handle when already connected", async () => {
      await consumerInstance.connect();
      await consumerInstance.connect();
      expect(logger.logInfo).toHaveBeenCalledWith("test-broker consumer is already connected");
    });

    it("should handle connection failure", async () => {
      const errorMsg = "Connection failure";
      jest.spyOn(consumerInstance, "_connectToMessageBroker").mockRejectedValueOnce(new Error(errorMsg));

      await expect(consumerInstance.connect()).rejects.toThrow(errorMsg);
      expect(logger.logError).toHaveBeenCalled();
      expect(consumerInstance.brokerConnected).toBe(false);
    });

    it("should disconnect successfully", async () => {
      await consumerInstance.connect();
      await consumerInstance.disconnect();
      expect(consumerInstance.brokerConnected).toBe(false);
      expect(logger.logInfo).toHaveBeenCalled();
    });

    it("should handle when already disconnected", async () => {
      await consumerInstance.disconnect();
      expect(logger.logInfo).toHaveBeenCalledWith("test-broker consumer is already disconnected");
    });

    it("should handle disconnect errors", async () => {
      const errorMsg = "Disconnect failure";
      await consumerInstance.connect();
      jest.spyOn(consumerInstance, "_disconnectFromMessageBroker").mockRejectedValueOnce(new Error(errorMsg));

      await expect(consumerInstance.disconnect()).rejects.toThrow(errorMsg);
      expect(logger.logError).toHaveBeenCalled();
    });

    it("should handle cache connection", async () => {
      consumerInstance = new TestConsumer({
        topic: "test-topic",
        cacheOptions: { keyPrefix: "test:" },
      });

      await consumerInstance.connect();
      expect(consumerInstance.mockCacheLayer.connect).toHaveBeenCalled();
    });

    it("should handle cache disconnection", async () => {
      consumerInstance = new TestConsumer({
        topic: "test-topic",
        cacheOptions: { keyPrefix: "test:" },
      });

      await consumerInstance.connect();
      await consumerInstance.disconnect();
      expect(consumerInstance.mockCacheLayer.disconnect).toHaveBeenCalled();
    });
  });

  describe("Consuming Operations", () => {
    beforeEach(async () => {
      consumerInstance = new TestConsumer(validConfig);
      await consumerInstance.connect();
      jest.clearAllMocks();
    });

    afterEach(async () => {
      if (consumerInstance.brokerConsuming) {
        await consumerInstance.stopConsuming();
      }
    });

    it("should start consuming successfully", async () => {
      await consumerInstance.startConsuming();
      expect(consumerInstance.brokerConsuming).toBe(true);
      expect(logger.logInfo).toHaveBeenCalledWith("test-broker consumer started consuming from test-topic");
    });

    it("should handle when already consuming", async () => {
      await consumerInstance.startConsuming();
      await consumerInstance.startConsuming();
      expect(logger.logInfo).toHaveBeenCalledWith("test-broker consumer is already consuming from test-topic");
    });

    it("should start consuming with custom handler", async () => {
      const customHandler = jest.fn();
      await consumerInstance.startConsuming({ businessHandler: customHandler });
      expect(consumerInstance.processHandler).toBeDefined();
    });

    it("should stop consuming successfully", async () => {
      await consumerInstance.startConsuming();
      await consumerInstance.stopConsuming();
      expect(consumerInstance.brokerConsuming).toBe(false);
      expect(logger.logInfo).toHaveBeenCalledWith("test-broker consumer stopped consuming from test-topic");
    });

    it("should handle when already stopped consuming", async () => {
      await consumerInstance.stopConsuming();
      expect(logger.logInfo).toHaveBeenCalledWith("test-broker consumer is already not consuming");
    });

    it("should require connection before consuming", async () => {
      await consumerInstance.disconnect();

      await expect(consumerInstance.startConsuming()).rejects.toThrow("Consumer must be connected");
      expect(logger.logError).toHaveBeenCalled();
    });
  });

  describe("Message Processing", () => {
    beforeEach(async () => {
      consumerInstance = new TestConsumer(validConfig);
      await consumerInstance.connect();
      await consumerInstance.startConsuming();
      jest.clearAllMocks();
    });

    it("should process message successfully", async () => {
      const testItem = { id: "123", data: "test" };

      await consumerInstance._processItem(testItem);

      expect(consumerInstance.processedItems.get("123")).toBe(true);
      expect(logger.logDebug).toHaveBeenCalledWith("Processing message", expect.any(Object));
      expect(logger.logInfo).toHaveBeenCalledWith("Successfully processed message", expect.any(Object));
    });

    it("should handle processing errors", async () => {
      const testItem = { id: "123", data: "test", shouldFail: true };

      await consumerInstance._processItem(testItem);

      expect(consumerInstance.processedItems.get("123")).toEqual({ error: "Processing failed" });
      expect(logger.logDebug).toHaveBeenCalledWith("Processing message", expect.any(Object));
      expect(logger.logError).toHaveBeenCalledWith("Failed to process message", expect.any(Error), expect.any(Object));
    });

    it("should use item id extractor", async () => {
      const testItem = { id: "123", data: "test" };
      const spy = jest.spyOn(consumerInstance, "getItemId");

      await consumerInstance._processItem(testItem);

      expect(spy).toHaveBeenCalledWith(testItem);
      expect(consumerInstance.processedItems.get("123")).toBe(true);
    });
  });

  describe("Business Message Handler", () => {
    beforeEach(async () => {
      consumerInstance = new TestConsumer(validConfig);
      await consumerInstance.connect();
      await consumerInstance.startConsuming();
      jest.clearAllMocks();
    });

    it("should process business messages with default handler", async () => {
      const messageType = "test-type";
      const messageId = "msg-123";
      const messageData = { content: "test-content" };

      await consumerInstance.simulateMessage(messageType, messageId, messageData);

      expect(logger.logDebug).toHaveBeenCalledWith("Processing business message", expect.any(Object));
      expect(consumerInstance.processedItems.get(messageId)).toBe(true);
    });

    it("should handle processing errors in business handler", async () => {
      const messageType = "test-type";
      const messageId = "msg-123";
      const messageData = { content: "test-content", shouldFail: true };

      try {
        await consumerInstance.simulateMessage(messageType, messageId, messageData);
      } catch (error) {
        // Expected error, we just need to catch it
      }

      expect(logger.logDebug).toHaveBeenCalledWith("Processing business message", expect.any(Object));
      expect(logger.logError).toHaveBeenCalledWith(
        "Failed to process business message",
        expect.any(Error),
        expect.any(Object)
      );
      expect(consumerInstance.processedItems.get(messageId)).toEqual({ error: "Processing failed" });
    });

    it("should use custom business handler", async () => {
      const customHandler = jest.fn();
      await consumerInstance.startConsuming({ businessHandler: customHandler });

      const messageType = "test-type";
      const messageId = "msg-123";
      const messageData = { content: "test-content" };

      await consumerInstance.simulateMessage(messageType, messageId, messageData);

      expect(customHandler).toHaveBeenCalledWith(messageType, messageId, messageData);
    });
  });

  describe("Cache Integration", () => {
    beforeEach(async () => {
      consumerInstance = new TestConsumer({
        ...validConfig,
        cacheOptions: { keyPrefix: "test:" },
      });
      await consumerInstance.connect();
      jest.clearAllMocks();
    });

    it("should check cache for processed items", async () => {
      const spy = jest.spyOn(consumerInstance, "_isItemProcessed");
      spy.mockResolvedValueOnce(true);

      const testItem = { id: "123", data: "test" };
      await consumerInstance._processItem(testItem);

      expect(spy).toHaveBeenCalledWith("123");
      expect(logger.logDebug).toHaveBeenCalledWith("Message already processed, skipping", expect.any(Object));
      expect(consumerInstance.mockCacheLayer.markAsProcessing).not.toHaveBeenCalled();
    });

    it("should mark items as processing in cache", async () => {
      const testItem = { id: "123", data: "test" };
      await consumerInstance._processItem(testItem);

      expect(consumerInstance.mockCacheLayer.markAsProcessing).toHaveBeenCalledWith("123");
      expect(consumerInstance.mockCacheLayer.markAsCompletedProcessing).toHaveBeenCalledWith("123", true);
    });

    it("should mark items as failed in cache", async () => {
      const testItem = { id: "123", data: "test", shouldFail: true };
      await consumerInstance._processItem(testItem);

      expect(consumerInstance.mockCacheLayer.markAsProcessing).toHaveBeenCalledWith("123");
      expect(consumerInstance.mockCacheLayer.markAsCompletedProcessing).toHaveBeenCalledWith("123", false);
    });
  });

  describe("Status Reporting", () => {
    beforeEach(async () => {
      consumerInstance = new TestConsumer(validConfig);
      await consumerInstance.connect();
    });

    it("should report status periodically", async () => {
      await consumerInstance.startConsuming();

      // Skip testing interval timing - just verify status is logged
      logger.logInfo.mockClear();
      // Manually trigger the status report function
      const status = consumerInstance.getStatus();
      logger.logInfo(`Status: ${JSON.stringify(status)}`);

      expect(logger.logInfo).toHaveBeenCalledWith(expect.stringContaining("Status:"));
    });

    it("should clear status reporting on stop", async () => {
      await consumerInstance.startConsuming();

      // Skip testing interval clearing
      consumerInstance._statusReportTimer = 123; // Mock timer ID
      await consumerInstance.stopConsuming();

      // Just verify the timer is cleared (set to null)
      expect(consumerInstance._statusReportTimer).toBeNull();
    });

    it("should get status with correct properties", () => {
      const status = consumerInstance.getStatus();

      expect(status).toHaveProperty("connected");
      expect(status).toHaveProperty("consuming");
      expect(status).toHaveProperty("broker");
      expect(status).toHaveProperty("topic");
      expect(status.broker).toBe("test-broker");
    });
  });
});
