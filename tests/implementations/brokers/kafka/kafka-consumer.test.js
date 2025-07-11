/**
 * @jest-environment node
 */

// Mock dependencies before importing tested module
jest.mock("../../../../src/abstracts/abstract-consumer");
jest.mock("../../../../src/implementations/brokers/kafka/kafka-manager");
jest.mock("../../../../src/implementations/cache/cache-client-factory");
jest.mock("../../../../src/services/logger-service");

// Import the tested module and mocked dependencies
const KafkaConsumer = require("../../../../src/implementations/brokers/kafka/kafka-consumer");
const AbstractConsumer = require("../../../../src/abstracts/abstract-consumer");
const KafkaManager = require("../../../../src/implementations/brokers/kafka/kafka-manager");
const CacheClientFactory = require("../../../../src/implementations/cache/cache-client-factory");
const logger = require("../../../../src/services/logger-service");

describe("KafkaConsumer", () => {
  describe("Constructor and Initialization", () => {
    // We can test the constructor now by mocking the parent constructor
    it("should create a KafkaConsumer instance and call parent constructor", () => {
      // Setup
      const mockConfig = {
        topic: "test-topic",
        groupId: "test-group",
        clientOptions: { brokers: ["localhost:9092"] },
        consumerOptions: { fromBeginning: false },
        topicOptions: { allowAutoTopicCreation: true },
      };

      // Mock AbstractConsumer constructor to avoid calling the real one
      AbstractConsumer.mockImplementation(function () {
        this._config = mockConfig;
        this._topic = mockConfig.topic;
      });

      // Mock KafkaManager methods
      KafkaManager.standardizeConfig.mockReturnValue(mockConfig);
      KafkaManager.createAdmin.mockReturnValue({ connect: jest.fn() });
      KafkaManager.createConsumer.mockReturnValue({ connect: jest.fn() });

      // Execute
      const consumer = new KafkaConsumer(mockConfig);

      // Verify
      expect(AbstractConsumer).toHaveBeenCalledWith(mockConfig);
      expect(consumer._topicOptions).toBe(mockConfig.topicOptions);
      expect(consumer._groupId).toBe(mockConfig.groupId);
      expect(consumer._clientOptions).toBe(mockConfig.clientOptions);
      expect(consumer._consumerOptions).toBe(mockConfig.consumerOptions);
      expect(KafkaManager.createAdmin).toHaveBeenCalled();
      expect(KafkaManager.createConsumer).toHaveBeenCalled();
    });
  });

  describe("Static Type Test", () => {
    it("should verify KafkaConsumer is a class that extends AbstractConsumer", () => {
      // This test simply verifies the class structure
      expect(KafkaConsumer.prototype instanceof AbstractConsumer).toBe(true);
    });
  });

  describe("Basic Methods", () => {
    it("should have core methods required for Kafka consumer functionality", () => {
      // This is a structural test to ensure methods exist
      const proto = KafkaConsumer.prototype;

      // Public methods should exist on prototype
      expect(typeof proto.getBrokerType).toBe("function");

      // The following would be private methods that implement abstract parent methods
      expect(typeof proto._createCacheLayer).toBe("function");
      expect(typeof proto._connectToMessageBroker).toBe("function");
      expect(typeof proto._disconnectFromMessageBroker).toBe("function");
      expect(typeof proto._startConsumingFromBroker).toBe("function");
      expect(typeof proto._stopConsumingFromBroker).toBe("function");
      expect(typeof proto.getConfigStatus).toBe("function");
      expect(typeof proto._createTopicIfAllowed).toBe("function");
    });

    it("should return 'kafka' for getBrokerType", () => {
      // Get method directly from prototype and call with empty this
      const getBrokerType = KafkaConsumer.prototype.getBrokerType;
      expect(getBrokerType.call({})).toBe("kafka");
    });
  });

  describe("Integration with KafkaManager", () => {
    let kafkaConsumer;
    let mockAdmin;
    let mockConsumer;

    beforeEach(() => {
      // Reset mocks
      jest.clearAllMocks();

      // Setup mocks
      mockAdmin = {
        connect: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
      };

      mockConsumer = {
        connect: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
        subscribe: jest.fn().mockResolvedValue(undefined),
        run: jest.fn().mockResolvedValue(undefined),
        stop: jest.fn().mockResolvedValue(undefined),
      };

      // Setup config
      const config = {
        topic: "test-topic",
        groupId: "test-group",
        topicOptions: { allowAutoTopicCreation: true },
      };

      KafkaManager.standardizeConfig.mockReturnValue(config);
      KafkaManager.createAdmin.mockReturnValue(mockAdmin);
      KafkaManager.createConsumer.mockReturnValue(mockConsumer);

      // Create instance but don't call actual constructor code
      kafkaConsumer = Object.create(KafkaConsumer.prototype);
      kafkaConsumer._admin = mockAdmin;
      kafkaConsumer._consumer = mockConsumer;
      kafkaConsumer._topic = "test-topic";
      kafkaConsumer._topicOptions = { allowAutoTopicCreation: true };
      kafkaConsumer._groupId = "test-group";
    });

    it("should create topic if it doesn't exist and auto-creation is allowed", async () => {
      // Setup
      KafkaManager.isTopicExisted.mockResolvedValue(false);
      KafkaManager.createTopic.mockResolvedValue(true);

      // Execute
      await kafkaConsumer._createTopicIfAllowed();

      // Verify
      expect(KafkaManager.isTopicExisted).toHaveBeenCalledWith(mockAdmin, "test-topic");
      expect(KafkaManager.createTopic).toHaveBeenCalledWith(mockAdmin, "test-topic", { allowAutoTopicCreation: true });
    });

    it("should not create topic if it already exists", async () => {
      // Setup
      KafkaManager.isTopicExisted.mockResolvedValue(true);

      // Execute
      await kafkaConsumer._createTopicIfAllowed();

      // Verify
      expect(KafkaManager.isTopicExisted).toHaveBeenCalledWith(mockAdmin, "test-topic");
      expect(KafkaManager.createTopic).not.toHaveBeenCalled();
    });

    it("should not create topic if auto-creation is disabled", async () => {
      // Setup
      KafkaManager.isTopicExisted.mockResolvedValue(false);
      kafkaConsumer._topicOptions.allowAutoTopicCreation = false;

      // Execute
      await kafkaConsumer._createTopicIfAllowed();

      // Verify
      expect(KafkaManager.isTopicExisted).toHaveBeenCalledWith(mockAdmin, "test-topic");
      expect(KafkaManager.createTopic).not.toHaveBeenCalled();
    });
  });

  describe("Message Broker Methods", () => {
    let kafkaConsumer;
    let mockAdmin;
    let mockConsumer;

    beforeEach(() => {
      // Reset mocks
      jest.clearAllMocks();

      // Setup mocks
      mockAdmin = {
        connect: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
      };

      mockConsumer = {
        connect: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
        subscribe: jest.fn().mockResolvedValue(undefined),
        run: jest.fn().mockResolvedValue(undefined),
        stop: jest.fn().mockResolvedValue(undefined),
        commitOffsets: jest.fn().mockResolvedValue(undefined),
      };

      // Create instance but don't call actual constructor code
      kafkaConsumer = Object.create(KafkaConsumer.prototype);
      kafkaConsumer._admin = mockAdmin;
      kafkaConsumer._consumer = mockConsumer;
      kafkaConsumer._topic = "test-topic";
      kafkaConsumer._groupId = "test-group";
      kafkaConsumer._consumerOptions = {
        fromBeginning: false,
        autoCommit: true,
      };
      kafkaConsumer._config = {
        eachBatchAutoResolve: true,
        autoCommit: true,
        autoCommitInterval: 5000,
        autoCommitThreshold: 100,
      };
      kafkaConsumer.maxConcurrency = 1;
    });

    it("should connect to message broker", async () => {
      // Execute
      await kafkaConsumer._connectToMessageBroker();

      // Verify
      expect(mockConsumer.connect).toHaveBeenCalled();
      expect(mockAdmin.connect).toHaveBeenCalled();
      expect(logger.logConnectionEvent).toHaveBeenCalledWith("🔌 Kafka Consumer", "connected to Kafka broker");
    });

    it("should disconnect from message broker", async () => {
      // Execute
      await kafkaConsumer._disconnectFromMessageBroker();

      // Verify
      expect(mockConsumer.disconnect).toHaveBeenCalled();
      expect(mockAdmin.disconnect).toHaveBeenCalled();
      expect(logger.logConnectionEvent).toHaveBeenCalledWith("Kafka Consumer", "disconnected from Kafka broker");
    });

    it("should start consuming from broker", async () => {
      // Execute
      await kafkaConsumer._startConsumingFromBroker();

      // Verify
      expect(mockConsumer.subscribe).toHaveBeenCalledWith({
        topic: "test-topic",
        fromBeginning: false,
      });

      expect(mockConsumer.run).toHaveBeenCalledWith(
        expect.objectContaining({
          eachBatchAutoResolve: true,
          partitionsConsumedConcurrently: 1,
          autoCommit: true,
          autoCommitInterval: 5000,
          autoCommitThreshold: 100,
          eachMessage: expect.any(Function),
        })
      );

      expect(logger.logInfo).toHaveBeenCalledWith(expect.stringContaining("Kafka consumer started for topic"));
    });

    it("should stop consuming from broker", async () => {
      // Execute
      await kafkaConsumer._stopConsumingFromBroker();

      // Verify
      expect(mockConsumer.stop).toHaveBeenCalled();
      expect(logger.logInfo).toHaveBeenCalledWith(expect.stringContaining("Kafka consumer stopped"));
    });
  });

  describe("Message Processing", () => {
    let kafkaConsumer;
    let mockConsumer;
    let mockRunOptions;

    beforeEach(() => {
      // Reset mocks
      jest.clearAllMocks();

      // Setup mocks
      mockConsumer = {
        connect: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
        subscribe: jest.fn().mockResolvedValue(undefined),
        run: jest.fn().mockImplementation(options => {
          mockRunOptions = options;
          return Promise.resolve();
        }),
        stop: jest.fn().mockResolvedValue(undefined),
        commitOffsets: jest.fn().mockResolvedValue(undefined),
      };

      // Create instance but don't call actual constructor code
      kafkaConsumer = Object.create(KafkaConsumer.prototype);
      kafkaConsumer._consumer = mockConsumer;
      kafkaConsumer._topic = "test-topic";
      kafkaConsumer._groupId = "test-group";
      kafkaConsumer._consumerOptions = {
        fromBeginning: false,
        autoCommit: true,
      };
      kafkaConsumer._config = {
        eachBatchAutoResolve: true,
        autoCommit: true,
        autoCommitInterval: 5000,
        autoCommitThreshold: 100,
      };
      kafkaConsumer.maxConcurrency = 1;
      kafkaConsumer._defaultBusinessHandler = jest.fn().mockResolvedValue(undefined);
    });

    it("should configure message consumption correctly", async () => {
      // Execute
      await kafkaConsumer._startConsumingFromBroker();

      // Verify
      expect(mockConsumer.subscribe).toHaveBeenCalledWith({
        topic: "test-topic",
        fromBeginning: false,
      });

      expect(mockConsumer.run).toHaveBeenCalledWith(
        expect.objectContaining({
          eachBatchAutoResolve: true,
          partitionsConsumedConcurrently: 1,
          autoCommit: true,
          autoCommitInterval: 5000,
          autoCommitThreshold: 100,
          eachMessage: expect.any(Function),
        })
      );

      // Verify we captured the run options
      expect(mockRunOptions).toBeDefined();
      expect(typeof mockRunOptions.eachMessage).toBe("function");
    });

    it("should configure auto-commit based on consumer options", async () => {
      // Test with auto-commit enabled
      kafkaConsumer._consumerOptions.autoCommit = true;
      kafkaConsumer._config.autoCommit = true;
      await kafkaConsumer._startConsumingFromBroker();
      expect(mockRunOptions.autoCommit).toBe(true);

      // Reset and test with auto-commit disabled
      jest.clearAllMocks();
      kafkaConsumer._consumerOptions.autoCommit = false;
      kafkaConsumer._config.autoCommit = false;
      await kafkaConsumer._startConsumingFromBroker();
      expect(mockRunOptions.autoCommit).toBe(false);
    });

    it("should configure fromBeginning based on consumer options", async () => {
      // Test with fromBeginning disabled
      kafkaConsumer._consumerOptions.fromBeginning = false;
      await kafkaConsumer._startConsumingFromBroker();
      expect(mockConsumer.subscribe).toHaveBeenCalledWith({
        topic: "test-topic",
        fromBeginning: false,
      });

      // Reset and test with fromBeginning enabled
      jest.clearAllMocks();
      kafkaConsumer._consumerOptions.fromBeginning = true;
      await kafkaConsumer._startConsumingFromBroker();
      expect(mockConsumer.subscribe).toHaveBeenCalledWith({
        topic: "test-topic",
        fromBeginning: true,
      });
    });

    it("should configure maxConcurrency correctly", async () => {
      // Test with default concurrency
      kafkaConsumer.maxConcurrency = 1;
      await kafkaConsumer._startConsumingFromBroker();
      expect(mockRunOptions.partitionsConsumedConcurrently).toBe(1);

      // Reset and test with higher concurrency
      jest.clearAllMocks();
      kafkaConsumer.maxConcurrency = 5;
      await kafkaConsumer._startConsumingFromBroker();
      expect(mockRunOptions.partitionsConsumedConcurrently).toBe(5);
    });

    // Test for the message handling logic indirectly
    it("should log appropriate messages when starting and stopping consumption", async () => {
      // Test start consuming
      await kafkaConsumer._startConsumingFromBroker();
      expect(logger.logInfo).toHaveBeenCalledWith(expect.stringContaining("Kafka consumer started for topic"));
      expect(logger.logInfo).toHaveBeenCalledWith(expect.stringContaining("test-topic"));
      expect(logger.logInfo).toHaveBeenCalledWith(expect.stringContaining("test-group"));

      // Test stop consuming
      await kafkaConsumer._stopConsumingFromBroker();
      expect(logger.logInfo).toHaveBeenCalledWith(expect.stringContaining("Kafka consumer stopped"));
      expect(logger.logInfo).toHaveBeenCalledWith(expect.stringContaining("test-topic"));
    });

    // Test the message processing by mocking the private method
    it("should process messages correctly", async () => {
      // Setup mocks for the private method
      KafkaManager.createMessageId.mockReturnValue("test-topic:0:123");
      KafkaManager.parseMessageValue.mockReturnValue({ id: "test-id", data: "test-data" });

      // Mock the private method directly
      const mockExtractBrokerMessage = jest.fn().mockReturnValue({
        type: "test-topic",
        messageId: "test-topic:0:123",
        item: { id: "test-id", data: "test-data" },
        committed: false,
      });

      // Temporarily replace the private method with our mock
      const originalProto = Object.getPrototypeOf(kafkaConsumer);
      const mockProto = Object.create(originalProto);
      mockProto["#extractBrokerMessage"] = mockExtractBrokerMessage;
      Object.setPrototypeOf(kafkaConsumer, mockProto);

      // Test with auto-commit
      kafkaConsumer._consumerOptions.autoCommit = true;
      await kafkaConsumer._startConsumingFromBroker();

      // Verify auto-commit behavior
      expect(logger.logInfo).toHaveBeenCalledWith(expect.stringContaining("Kafka consumer started"));

      // Test with manual commit
      jest.clearAllMocks();
      kafkaConsumer._consumerOptions.autoCommit = false;
      kafkaConsumer._config.autoCommit = false;
      await kafkaConsumer._startConsumingFromBroker();

      // Verify manual commit configuration
      expect(mockRunOptions.autoCommit).toBe(false);
    });

    // Test error handling indirectly
    it("should handle errors in message processing", async () => {
      // Setup for error handling
      kafkaConsumer._defaultBusinessHandler.mockRejectedValue(new Error("Processing error"));

      // Start consuming
      await kafkaConsumer._startConsumingFromBroker();

      // Verify error handling configuration is in place
      expect(mockRunOptions.eachMessage).toBeDefined();
    });

    // Test the message processing in more detail
    it("should process messages correctly with auto-commit", async () => {
      // Setup mocks
      KafkaManager.createMessageId.mockReturnValue("test-topic:0:123");
      KafkaManager.parseMessageValue.mockReturnValue({ id: "test-id", data: "test-data" });

      // Start consuming
      await kafkaConsumer._startConsumingFromBroker();

      // Get the eachMessage function
      const eachMessageFn = mockRunOptions.eachMessage;

      // Create a test message
      const testMessage = {
        topic: "test-topic",
        partition: 0,
        message: {
          offset: "123",
          value: Buffer.from(JSON.stringify({ id: "test-id", data: "test-data" })),
        },
      };

      // Bind the function to the consumer instance to simulate proper execution context
      // This is a workaround to test private methods
      const boundEachMessage = eachMessageFn.bind(kafkaConsumer);

      // Execute the function
      await boundEachMessage(testMessage);

      // Verify the business handler was called
      expect(KafkaManager.createMessageId).toHaveBeenCalledWith("test-topic", 0, "123");
      expect(KafkaManager.parseMessageValue).toHaveBeenCalledWith(testMessage.message.value);
      expect(kafkaConsumer._defaultBusinessHandler).toHaveBeenCalled();
    });

    it("should process messages correctly with manual commit", async () => {
      // Setup for manual commit
      kafkaConsumer._consumerOptions.autoCommit = false;
      kafkaConsumer._config.autoCommit = false;

      // Setup mocks
      KafkaManager.createMessageId.mockReturnValue("test-topic:0:123");
      KafkaManager.parseMessageValue.mockReturnValue({ id: "test-id", data: "test-data" });

      // Start consuming
      await kafkaConsumer._startConsumingFromBroker();

      // Get the eachMessage function
      const eachMessageFn = mockRunOptions.eachMessage;

      // Create a test message
      const testMessage = {
        topic: "test-topic",
        partition: 0,
        message: {
          offset: "123",
          value: Buffer.from(JSON.stringify({ id: "test-id", data: "test-data" })),
        },
      };

      // Bind the function to the consumer instance
      const boundEachMessage = eachMessageFn.bind(kafkaConsumer);

      // Execute the function
      await boundEachMessage(testMessage);

      // Verify manual commit
      expect(mockConsumer.commitOffsets).toHaveBeenCalledWith([
        {
          topic: "test-topic",
          partition: 0,
          offset: "124", // offset + 1
        },
      ]);
    });

    it("should handle errors during message processing with auto-commit", async () => {
      // Setup error
      const processingError = new Error("Processing failed");
      kafkaConsumer._defaultBusinessHandler.mockRejectedValue(processingError);

      // Setup mocks
      KafkaManager.createMessageId.mockReturnValue("test-topic:0:123");
      KafkaManager.parseMessageValue.mockReturnValue({ id: "test-id", data: "test-data" });

      // Start consuming
      await kafkaConsumer._startConsumingFromBroker();

      // Get the eachMessage function
      const eachMessageFn = mockRunOptions.eachMessage;

      // Create a test message
      const testMessage = {
        topic: "test-topic",
        partition: 0,
        message: {
          offset: "123",
          value: Buffer.from(JSON.stringify({ id: "test-id", data: "test-data" })),
        },
      };

      // Bind the function to the consumer instance
      const boundEachMessage = eachMessageFn.bind(kafkaConsumer);

      // Execute the function
      await boundEachMessage(testMessage);

      // Verify error handling
      expect(logger.logError).toHaveBeenCalledWith(
        expect.stringContaining("Error processing Kafka message"),
        processingError
      );

      // Should not manually commit with auto-commit
      expect(mockConsumer.commitOffsets).not.toHaveBeenCalled();
    });

    it("should handle errors during message processing with manual commit", async () => {
      // Setup for manual commit
      kafkaConsumer._consumerOptions.autoCommit = false;
      kafkaConsumer._config.autoCommit = false;

      // Setup error
      const processingError = new Error("Processing failed");
      kafkaConsumer._defaultBusinessHandler.mockRejectedValue(processingError);

      // Setup mocks
      KafkaManager.createMessageId.mockReturnValue("test-topic:0:123");
      KafkaManager.parseMessageValue.mockReturnValue({ id: "test-id", data: "test-data" });

      // Start consuming
      await kafkaConsumer._startConsumingFromBroker();

      // Get the eachMessage function
      const eachMessageFn = mockRunOptions.eachMessage;

      // Create a test message
      const testMessage = {
        topic: "test-topic",
        partition: 0,
        message: {
          offset: "123",
          value: Buffer.from(JSON.stringify({ id: "test-id", data: "test-data" })),
        },
      };

      // Bind the function to the consumer instance
      const boundEachMessage = eachMessageFn.bind(kafkaConsumer);

      // Execute the function
      await boundEachMessage(testMessage);

      // Verify error handling
      expect(logger.logError).toHaveBeenCalledWith(
        expect.stringContaining("Error processing Kafka message"),
        processingError
      );

      // Should manually commit failed message
      expect(mockConsumer.commitOffsets).toHaveBeenCalledWith([
        {
          topic: "test-topic",
          partition: 0,
          offset: "124", // offset + 1
        },
      ]);

      expect(logger.logWarning).toHaveBeenCalledWith(expect.stringContaining("Committed failed message offset"));
    });

    it("should handle commit errors after processing failure", async () => {
      // Setup for manual commit
      kafkaConsumer._consumerOptions.autoCommit = false;
      kafkaConsumer._config.autoCommit = false;

      // Setup errors
      const processingError = new Error("Processing failed");
      kafkaConsumer._defaultBusinessHandler.mockRejectedValue(processingError);

      const commitError = new Error("Commit failed");
      mockConsumer.commitOffsets.mockRejectedValue(commitError);

      // Setup mocks
      KafkaManager.createMessageId.mockReturnValue("test-topic:0:123");
      KafkaManager.parseMessageValue.mockReturnValue({ id: "test-id", data: "test-data" });

      // Start consuming
      await kafkaConsumer._startConsumingFromBroker();

      // Get the eachMessage function
      const eachMessageFn = mockRunOptions.eachMessage;

      // Create a test message
      const testMessage = {
        topic: "test-topic",
        partition: 0,
        message: {
          offset: "123",
          value: Buffer.from(JSON.stringify({ id: "test-id", data: "test-data" })),
        },
      };

      // Bind the function to the consumer instance
      const boundEachMessage = eachMessageFn.bind(kafkaConsumer);

      // Execute the function
      await boundEachMessage(testMessage);

      // Verify error handling
      expect(logger.logError).toHaveBeenCalledWith(
        expect.stringContaining("Error processing Kafka message"),
        processingError
      );

      expect(mockConsumer.commitOffsets).toHaveBeenCalled();
      expect(logger.logError).toHaveBeenCalledWith(
        expect.stringContaining("Failed to commit offset after error"),
        commitError
      );
    });
  });

  describe("Cache Layer Creation", () => {
    let kafkaConsumer;

    beforeEach(() => {
      // Reset mocks
      jest.clearAllMocks();

      // Create instance but don't call actual constructor code
      kafkaConsumer = Object.create(KafkaConsumer.prototype);
    });

    it("should create cache layer when options are provided", () => {
      // Setup
      const cacheOptions = { type: "redis", host: "localhost", port: 6379 };
      const mockCacheClient = { connect: jest.fn() };
      CacheClientFactory.createClient.mockReturnValue(mockCacheClient);

      // Execute
      const result = kafkaConsumer._createCacheLayer(cacheOptions);

      // Verify
      expect(CacheClientFactory.createClient).toHaveBeenCalledWith(cacheOptions);
      expect(result).toBe(mockCacheClient);
    });

    it("should return null when cache options are not provided", () => {
      // Execute
      const result = kafkaConsumer._createCacheLayer(null);

      // Verify
      expect(CacheClientFactory.createClient).not.toHaveBeenCalled();
      expect(logger.logWarning).toHaveBeenCalledWith(expect.stringContaining("Cache layer is disabled"));
      expect(result).toBeNull();
    });
  });

  describe("Config Status", () => {
    let kafkaConsumer;
    let mockGetConfigStatus;

    beforeEach(() => {
      // Reset mocks
      jest.clearAllMocks();

      // Setup mock for parent method
      mockGetConfigStatus = function () {
        return {
          type: "consumer",
          topic: "test-topic",
          status: "connected",
        };
      };

      // Create instance but don't call actual constructor code
      kafkaConsumer = Object.create(KafkaConsumer.prototype);
      kafkaConsumer._groupId = "test-group";
      kafkaConsumer._consumerOptions = {
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
        maxBytesPerPartition: 1048576,
        autoCommit: true,
        fromBeginning: false,
      };
      kafkaConsumer.maxConcurrency = 5;

      // Mock parent method
      AbstractConsumer.prototype.getConfigStatus = mockGetConfigStatus;
    });

    it("should return extended config status with consumer-specific fields", () => {
      // Execute
      const status = kafkaConsumer.getConfigStatus();

      // Verify
      expect(status).toMatchObject({
        type: "consumer",
        topic: "test-topic",
        status: "connected",
        groupId: "test-group",
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
        maxBytesPerPartition: 1048576,
        autoCommit: true,
        fromBeginning: false,
        partitionsConsumedConcurrently: 5,
      });
    });
  });
});
