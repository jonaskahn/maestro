jest.mock("../../../../src/abstracts/abstract-consumer");
jest.mock("../../../../src/implementations/brokers/kafka/kafka-manager");
jest.mock("../../../../src/implementations/cache/cache-client-factory");
jest.mock("../../../../src/services/logger-service");

const mockLogInfo = jest.fn();
const mockLogDebug = jest.fn();
const mockLogWarning = jest.fn();
const mockLogError = jest.fn();
const mockLogConnectionEvent = jest.fn();
jest.mock("../../../../src/services/logger-service", () => ({
  logInfo: mockLogInfo,
  logDebug: mockLogDebug,
  logWarning: mockLogWarning,
  logError: mockLogError,
  logConnectionEvent: mockLogConnectionEvent,
}));

const mockCreateMessageId = jest.fn();
const mockParseMessageValue = jest.fn();
jest.mock("../../../../src/implementations/brokers/kafka/kafka-manager", () => ({
  standardizeConfig: jest.fn(),
  createAdmin: jest.fn(),
  createConsumer: jest.fn(),
  isTopicExisted: jest.fn(),
  createTopic: jest.fn(),
  createMessageId: mockCreateMessageId,
  parseMessageValue: mockParseMessageValue,
}));

const KafkaConsumer = require("../../../../src/implementations/brokers/kafka/kafka-consumer");
const AbstractConsumer = require("../../../../src/abstracts/abstract-consumer");
const KafkaManager = require("../../../../src/implementations/brokers/kafka/kafka-manager");
const CacheClientFactory = require("../../../../src/implementations/cache/cache-client-factory");
const logger = require("../../../../src/services/logger-service");

describe("KafkaConsumer", () => {
  describe("Constructor and Initialization", () => {
    test("Given the component When createing a KafkaConsumer instance and call parent constructor Then it should succeed", () => {
      const mockConfig = {
        topic: "test-topic",
        groupId: "test-group",
        clientOptions: { brokers: ["localhost:9092"] },
        consumerOptions: { fromBeginning: false },
        topicOptions: { allowAutoTopicCreation: true },
      };

      AbstractConsumer.mockImplementation(function () {
        this._config = mockConfig;
        this._topic = mockConfig.topic;
      });

      KafkaManager.standardizeConfig.mockReturnValue(mockConfig);
      KafkaManager.createAdmin.mockReturnValue({ connect: jest.fn() });
      KafkaManager.createConsumer.mockReturnValue({ connect: jest.fn() });

      const consumer = new KafkaConsumer(mockConfig);

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
    test("Given Test setup for should verify KafkaConsumer is a class that extends AbstractConsumer When Action being tested Then Expected outcome", () => {
      expect(KafkaConsumer.prototype instanceof AbstractConsumer).toBe(true);
    });
  });

  describe("Basic Methods", () => {
    test("Given Test setup for should have core methods required for Kafka consumer functionality When Action being tested Then Expected outcome", () => {
      const proto = KafkaConsumer.prototype;

      expect(typeof proto.getBrokerType).toBe("function");

      expect(typeof proto._createCacheLayer).toBe("function");
      expect(typeof proto._connectToMessageBroker).toBe("function");
      expect(typeof proto._disconnectFromMessageBroker).toBe("function");
      expect(typeof proto._startConsumingFromBroker).toBe("function");
      expect(typeof proto._stopConsumingFromBroker).toBe("function");
      expect(typeof proto.getConfigStatus).toBe("function");
      expect(typeof proto._createTopicIfAllowed).toBe("function");
    });

    test("Given the component When returning 'kafka' for getBrokerType Then it should succeed", () => {
      const getBrokerType = KafkaConsumer.prototype.getBrokerType;
      expect(getBrokerType.call({})).toBe("kafka");
    });
  });

  describe("Integration with KafkaManager", () => {
    let kafkaConsumer;
    let mockAdmin;
    let mockConsumer;

    beforeEach(() => {
      jest.clearAllMocks();

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

      const config = {
        topic: "test-topic",
        groupId: "test-group",
        topicOptions: { allowAutoTopicCreation: true },
      };

      KafkaManager.standardizeConfig.mockReturnValue(config);
      KafkaManager.createAdmin.mockReturnValue(mockAdmin);
      KafkaManager.createConsumer.mockReturnValue(mockConsumer);

      kafkaConsumer = Object.create(KafkaConsumer.prototype);
      kafkaConsumer._admin = mockAdmin;
      kafkaConsumer._consumer = mockConsumer;
      kafkaConsumer._topic = "test-topic";
      kafkaConsumer._topicOptions = { allowAutoTopicCreation: true };
      kafkaConsumer._groupId = "test-group";
    });

    test("should create topic if it doesn't exist and auto-creation is allowed", async () => {
      KafkaManager.isTopicExisted.mockResolvedValue(false);
      KafkaManager.createTopic.mockResolvedValue(true);

      await kafkaConsumer._createTopicIfAllowed();

      expect(KafkaManager.isTopicExisted).toHaveBeenCalledWith(mockAdmin, "test-topic");
      expect(KafkaManager.createTopic).toHaveBeenCalledWith(mockAdmin, "test-topic", { allowAutoTopicCreation: true });
    });

    test("should not create topic if it already exists", async () => {
      KafkaManager.isTopicExisted.mockResolvedValue(true);

      await kafkaConsumer._createTopicIfAllowed();

      expect(KafkaManager.isTopicExisted).toHaveBeenCalledWith(mockAdmin, "test-topic");
      expect(KafkaManager.createTopic).not.toHaveBeenCalled();
    });

    test("should not create topic if auto-creation is disabled", async () => {
      KafkaManager.isTopicExisted.mockResolvedValue(false);
      kafkaConsumer._topicOptions.allowAutoTopicCreation = false;

      await kafkaConsumer._createTopicIfAllowed();

      expect(KafkaManager.isTopicExisted).toHaveBeenCalledWith(mockAdmin, "test-topic");
      expect(KafkaManager.createTopic).not.toHaveBeenCalled();
    });
  });

  describe("Message Broker Methods", () => {
    let kafkaConsumer;
    let mockAdmin;
    let mockConsumer;

    beforeEach(() => {
      jest.clearAllMocks();

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

    test("should connect to message broker", async () => {
      await kafkaConsumer._connectToMessageBroker();

      expect(mockConsumer.connect).toHaveBeenCalled();
      expect(mockAdmin.connect).toHaveBeenCalled();
      expect(mockLogConnectionEvent).toHaveBeenCalledWith("Kafka Consumer", "connected to Kafka broker");
    });

    test("should disconnect from message broker", async () => {
      await kafkaConsumer._disconnectFromMessageBroker();

      expect(mockConsumer.disconnect).toHaveBeenCalled();
      expect(mockAdmin.disconnect).toHaveBeenCalled();
      expect(mockLogConnectionEvent).toHaveBeenCalledWith("Kafka Consumer", "disconnected from Kafka broker");
    });

    test("should start consuming from broker", async () => {
      await kafkaConsumer._startConsumingFromBroker();

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

      expect(mockLogInfo).toHaveBeenCalledWith(expect.stringContaining("Kafka consumer started for topic"));
    });

    test("should stop consuming from broker", async () => {
      await kafkaConsumer._stopConsumingFromBroker();

      expect(mockConsumer.stop).toHaveBeenCalled();
      expect(mockLogInfo).toHaveBeenCalledWith(expect.stringContaining("Kafka consumer stopped"));
    });
  });

  describe("Message Processing", () => {
    let kafkaConsumer;
    let mockConsumer;
    let mockRunOptions;
    let eachMessageHandler;

    beforeEach(() => {
      jest.clearAllMocks();

      mockCreateMessageId.mockImplementation((topic, partition, offset) => `${topic}:${partition}:${offset}`);
      mockParseMessageValue.mockImplementation(value => {
        try {
          return JSON.parse(value.toString());
        } catch (e) {
          return { data: value.toString() };
        }
      });

      mockConsumer = {
        connect: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
        subscribe: jest.fn().mockResolvedValue(undefined),
        run: jest.fn().mockImplementation(options => {
          mockRunOptions = options;
          eachMessageHandler = options.eachMessage;
          return Promise.resolve();
        }),
        stop: jest.fn().mockResolvedValue(undefined),
        commitOffsets: jest.fn().mockResolvedValue(undefined),
      };

      kafkaConsumer = new KafkaConsumer({
        topic: "test-topic",
        groupId: "test-group",
      });

      kafkaConsumer["#extractBrokerMessage"] = jest.fn().mockImplementation(({ topic, partition, message }) => {
        const messageId = `${topic}:${partition}:${message.offset}`;
        const content =
          typeof message.value === "string" ? { data: message.value } : JSON.parse(message.value.toString());

        return {
          type: topic,
          messageId,
          item: content,
          committed: false,
        };
      });

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

    test("should configure message consumption correctly", async () => {
      await kafkaConsumer._startConsumingFromBroker();

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

      expect(mockRunOptions).toBeDefined();
      expect(typeof mockRunOptions.eachMessage).toBe("function");
    });

    test("should configure auto-commit based on consumer options", async () => {
      kafkaConsumer._consumerOptions.autoCommit = true;
      kafkaConsumer._config.autoCommit = true;
      await kafkaConsumer._startConsumingFromBroker();
      expect(mockRunOptions.autoCommit).toBe(true);

      jest.clearAllMocks();
      kafkaConsumer._consumerOptions.autoCommit = false;
      kafkaConsumer._config.autoCommit = false;
      await kafkaConsumer._startConsumingFromBroker();
      expect(mockRunOptions.autoCommit).toBe(false);
    });

    test("should configure fromBeginning based on consumer options", async () => {
      kafkaConsumer._consumerOptions.fromBeginning = false;
      await kafkaConsumer._startConsumingFromBroker();
      expect(mockConsumer.subscribe).toHaveBeenCalledWith({
        topic: "test-topic",
        fromBeginning: false,
      });

      jest.clearAllMocks();
      kafkaConsumer._consumerOptions.fromBeginning = true;
      await kafkaConsumer._startConsumingFromBroker();
      expect(mockConsumer.subscribe).toHaveBeenCalledWith({
        topic: "test-topic",
        fromBeginning: true,
      });
    });

    test("should configure maxConcurrency correctly", async () => {
      kafkaConsumer.maxConcurrency = 1;
      await kafkaConsumer._startConsumingFromBroker();
      expect(mockRunOptions.partitionsConsumedConcurrently).toBe(1);

      jest.clearAllMocks();
      kafkaConsumer.maxConcurrency = 5;
      await kafkaConsumer._startConsumingFromBroker();
      expect(mockRunOptions.partitionsConsumedConcurrently).toBe(5);
    });

    test("should log appropriate messages when starting and stopping consumption", async () => {
      await kafkaConsumer._startConsumingFromBroker();
      expect(mockLogInfo).toHaveBeenCalledWith(expect.stringContaining("Kafka consumer started for topic"));
      expect(mockLogInfo).toHaveBeenCalledWith(expect.stringContaining("test-topic"));
      expect(mockLogInfo).toHaveBeenCalledWith(expect.stringContaining("test-group"));

      await kafkaConsumer._stopConsumingFromBroker();
      expect(mockLogInfo).toHaveBeenCalledWith(expect.stringContaining("Kafka consumer stopped"));
      expect(mockLogInfo).toHaveBeenCalledWith(expect.stringContaining("test-topic"));
    });

    test("should process messages correctly", async () => {
      mockCreateMessageId.mockReturnValue("test-topic:0:123");
      mockParseMessageValue.mockReturnValue({ id: "test-id", data: "test-data" });

      await kafkaConsumer._startConsumingFromBroker();

      kafkaConsumer._consumerOptions.autoCommit = true;

      expect(mockLogInfo).toHaveBeenCalledWith(expect.stringContaining("Kafka consumer started"));

      jest.clearAllMocks();
      kafkaConsumer._consumerOptions.autoCommit = false;
      kafkaConsumer._config.autoCommit = false;
      await kafkaConsumer._startConsumingFromBroker();

      expect(mockRunOptions.autoCommit).toBe(false);
    });

    test("should process messages correctly with auto-commit", async () => {
      await kafkaConsumer._startConsumingFromBroker();

      const testMessage = {
        topic: "test-topic",
        partition: 0,
        message: {
          offset: "123",
          value: Buffer.from(JSON.stringify({ id: "test-id", data: "test-data" })),
        },
      };

      const standardizedMessage = {
        type: "test-topic",
        messageId: "test-topic:0:123",
        item: { id: "test-id", data: "test-data" },
        committed: false,
      };

      kafkaConsumer["#extractBrokerMessage"].mockReturnValue(standardizedMessage);

      await eachMessageHandler(testMessage);

      expect(kafkaConsumer._defaultBusinessHandler).toHaveBeenCalledWith(
        standardizedMessage.type,
        standardizedMessage.messageId,
        standardizedMessage.item
      );
      expect(mockConsumer.commitOffsets).not.toHaveBeenCalled();
    });

    test("should process messages correctly with manual commit", async () => {
      kafkaConsumer._consumerOptions.autoCommit = false;
      kafkaConsumer._config.autoCommit = false;

      await kafkaConsumer._startConsumingFromBroker();

      const testMessage = {
        topic: "test-topic",
        partition: 0,
        message: {
          offset: "123",
          value: Buffer.from(JSON.stringify({ id: "test-id", data: "test-data" })),
        },
      };

      await eachMessageHandler(testMessage);

      expect(mockConsumer.commitOffsets).toHaveBeenCalledWith([
        {
          topic: "test-topic",
          partition: 0,
          offset: "124",
        },
      ]);
    });

    test("should handle errors during message processing with auto-commit", async () => {
      const processingError = new Error("Processing failed");
      kafkaConsumer._defaultBusinessHandler.mockRejectedValue(processingError);

      await kafkaConsumer._startConsumingFromBroker();

      const testMessage = {
        topic: "test-topic",
        partition: 0,
        message: {
          offset: "123",
          value: Buffer.from(JSON.stringify({ id: "test-id", data: "test-data" })),
        },
      };

      await eachMessageHandler(testMessage);

      expect(mockLogError).toHaveBeenCalledWith(
        expect.stringContaining("Error processing Kafka message"),
        expect.any(Error)
      );

      expect(mockConsumer.commitOffsets).not.toHaveBeenCalled();
    });

    test("should handle errors during message processing with manual commit", async () => {
      kafkaConsumer._consumerOptions.autoCommit = false;
      kafkaConsumer._config.autoCommit = false;

      const processingError = new Error("Processing failed");
      kafkaConsumer._defaultBusinessHandler.mockRejectedValue(processingError);

      const standardizedMessage = {
        type: "test-topic",
        messageId: "test-topic:0:123",
        item: { id: "test-id", data: "test-data" },
        committed: false,
      };

      kafkaConsumer["#extractBrokerMessage"].mockReturnValue(standardizedMessage);

      await kafkaConsumer._startConsumingFromBroker();

      const testMessage = {
        topic: "test-topic",
        partition: 0,
        message: {
          offset: "123",
          value: Buffer.from(JSON.stringify({ id: "test-id", data: "test-data" })),
        },
      };

      await eachMessageHandler(testMessage);

      expect(mockLogError).toHaveBeenCalledWith(
        expect.stringContaining("Error processing Kafka message"),
        expect.any(Error)
      );

      expect(mockConsumer.commitOffsets).toHaveBeenCalledWith([
        {
          topic: "test-topic",
          partition: 0,
          offset: "124",
        },
      ]);

      expect(mockLogWarning).toHaveBeenCalledWith(expect.stringContaining("Committed failed message offset"));
    });

    test("should handle commit errors after processing failure", async () => {
      kafkaConsumer._consumerOptions.autoCommit = false;
      kafkaConsumer._config.autoCommit = false;

      const processingError = new Error("Processing failed");
      kafkaConsumer._defaultBusinessHandler.mockRejectedValue(processingError);

      const commitError = new Error("Commit failed");
      mockConsumer.commitOffsets.mockRejectedValue(commitError);

      const standardizedMessage = {
        type: "test-topic",
        messageId: "test-topic:0:123",
        item: { id: "test-id", data: "test-data" },
        committed: false,
      };

      kafkaConsumer["#extractBrokerMessage"].mockReturnValue(standardizedMessage);

      await kafkaConsumer._startConsumingFromBroker();

      const testMessage = {
        topic: "test-topic",
        partition: 0,
        message: {
          offset: "123",
          value: Buffer.from(JSON.stringify({ id: "test-id", data: "test-data" })),
        },
      };

      await eachMessageHandler(testMessage);

      expect(mockLogError).toHaveBeenCalledWith(
        expect.stringContaining("Error processing Kafka message"),
        expect.any(Error)
      );

      expect(mockConsumer.commitOffsets).toHaveBeenCalled();
      expect(mockLogError).toHaveBeenCalledWith(
        expect.stringContaining("Failed to commit offset after error"),
        commitError
      );
    });
  });

  describe("Cache Layer Creation", () => {
    let kafkaConsumer;

    beforeEach(() => {
      jest.clearAllMocks();

      kafkaConsumer = Object.create(KafkaConsumer.prototype);
    });

    test("Given the component When createing cache layer when options are provided Then it should succeed", () => {
      const cacheOptions = { type: "redis", host: "localhost", port: 6379 };
      const mockCacheClient = { connect: jest.fn() };
      CacheClientFactory.createClient.mockReturnValue(mockCacheClient);

      const result = kafkaConsumer._createCacheLayer(cacheOptions);

      expect(CacheClientFactory.createClient).toHaveBeenCalledWith(cacheOptions);
      expect(result).toBe(mockCacheClient);
    });

    test("Given Test setup for should return null when cache options are not provided When Action being tested Then Expected outcome", () => {
      const result = kafkaConsumer._createCacheLayer(null);

      expect(CacheClientFactory.createClient).not.toHaveBeenCalled();
      expect(mockLogWarning).toHaveBeenCalledWith(expect.stringContaining("Cache layer is disabled"));
      expect(result).toBeNull();
    });
  });

  describe("Config Status", () => {
    let kafkaConsumer;
    let mockGetConfigStatus;

    beforeEach(() => {
      jest.clearAllMocks();

      mockGetConfigStatus = function () {
        return {
          type: "consumer",
          topic: "test-topic",
          status: "connected",
        };
      };

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

      AbstractConsumer.prototype.getConfigStatus = mockGetConfigStatus;
    });

    test("Given Test setup for should return extended config status with consumer-specific fields When Action being tested Then Expected outcome", () => {
      const status = kafkaConsumer.getConfigStatus();

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
