const AbstractConsumer = require("../../src/abstracts/abstract-consumer");

jest.mock("../../src/services/logger-service", () => ({
  logDebug: jest.fn(),
  logError: jest.fn(),
  logWarning: jest.fn(),
  logInfo: jest.fn(),
}));

const logger = require("../../src/services/logger-service");

class TestConsumer extends AbstractConsumer {
  constructor(config) {
    super(config);
    this.brokerConnected = false;
    this.brokerConsuming = false;
    this.processedItems = new Map();
    this.processHandler = null;
    this.mockCacheLayer = null;
  }

  getBrokerType() {
    return "test-broker";
  }

  _createCacheLayer(config) {
    if (!config) return null;

    this.mockCacheLayer = {
      connect: jest.fn().mockResolvedValue(true),
      disconnect: jest.fn().mockResolvedValue(true),
      isConnected: true,
      markAsProcessing: jest.fn().mockResolvedValue(true),
      markAsCompletedProcessing: jest.fn().mockResolvedValue(true),
    };
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
    return true;
  }

  async _stopConsumingFromBroker() {
    this.brokerConsuming = false;
    return true;
  }

  async _isItemProcessed(itemId) {
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
    await this._defaultBusinessHandler(messageType, messageId, messageData);
  }

  async process(item) {
    if (item.shouldFail) {
      throw new Error("Processing failed");
    }
    return true;
  }
}

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

      await consumerInstance.simulateMessage(messageType, messageId, messageData);

      expect(logger.logDebug).toHaveBeenCalledWith("Processing business message", expect.any(Object));
      expect(logger.logError).toHaveBeenCalledWith(
        "Failed to process business message",
        expect.any(Error),
        expect.any(Object)
      );
      expect(consumerInstance.processedItems.get(messageId)).toEqual({ error: "Processing failed" });
    });

    it("should use custom business handler", async () => {
      const customHandler = jest.fn().mockResolvedValue(true);
      await consumerInstance.startConsuming({ businessHandler: customHandler });

      const messageType = "test-type";
      const messageId = "msg-123";
      const messageData = { content: "test-content" };

      await consumerInstance._defaultBusinessHandler(messageType, messageId, messageData);

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
    beforeEach(() => {
      consumerInstance = new TestConsumer({
        ...validConfig,
        statusReportInterval: 100,
      });
      jest.useFakeTimers();
    });

    it("should report status periodically", async () => {
      await consumerInstance.connect();
      await consumerInstance.startConsuming();

      expect(setInterval).toHaveBeenCalledTimes(1);
      expect(setInterval).toHaveBeenCalledWith(expect.any(Function), 100);

      jest.runOnlyPendingTimers();
      expect(logger.logInfo).toHaveBeenCalledWith("Status report", expect.any(Object));
    });

    it("should clear status reporting on stop", async () => {
      await consumerInstance.connect();
      await consumerInstance.startConsuming();

      const spy = jest.spyOn(global, "clearInterval");
      await consumerInstance.stopConsuming();

      expect(spy).toHaveBeenCalledWith(expect.any(Number));
    });

    it("should get status with correct properties", () => {
      const status = consumerInstance.getStatus();

      expect(status).toHaveProperty("connected");
      expect(status).toHaveProperty("consuming");
      expect(status).toHaveProperty("maxConcurrency");
      expect(status).toHaveProperty("broker");
      expect(status).toHaveProperty("topic");
      expect(status.broker).toBe("test-broker");
      expect(status.topic).toBe("test-topic");
    });
  });
});
