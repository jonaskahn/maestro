/**
 * @jest-environment node
 */

// Mock dependencies before importing KafkaManager
const mockKafka = jest.fn();
const mockAdmin = jest.fn();
const mockProducer = jest.fn();
const mockConsumer = jest.fn();
const mockLogDebug = jest.fn();
const mockLogWarning = jest.fn();
const mockLogError = jest.fn();
const mockLogInfo = jest.fn();
const mockLegacyPartitioner = jest.fn();

// Mock kafkajs
jest.mock("kafkajs", () => {
  return {
    Kafka: mockKafka,
    CompressionTypes: {
      None: 0,
      GZIP: 1,
      Snappy: 2,
      LZ4: 3,
      ZSTD: 4,
    },
    Partitioners: {
      LegacyPartitioner: mockLegacyPartitioner,
    },
  };
});

// Mock logger service
jest.mock("../../../../src/services/logger-service", () => ({
  logInfo: mockLogInfo,
  logDebug: mockLogDebug,
  logWarning: mockLogWarning,
  logError: mockLogError,
  logConnectionEvent: jest.fn(),
}));

// Mock TTL config
jest.mock("../../../../src/config/ttl-config", () => ({
  getKafkaConfig: jest.fn().mockReturnValue({
    connectionTimeout: 1000,
    requestTimeout: 10000,
  }),
  getAllTTLValues: jest.fn().mockReturnValue({
    TASK_PROCESSING_STATE_TTL: 60000,
  }),
}));

// Now import the module under test
const KafkaManager = require("../../../../src/implementations/brokers/kafka/kafka-manager");
const { CompressionTypes } = require("kafkajs");

describe("KafkaManager", () => {
  let mockKafkaInstance;
  let mockAdminInstance;

  beforeEach(() => {
    jest.clearAllMocks();

    // Setup mock instances
    mockAdminInstance = {
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      createTopics: jest.fn().mockResolvedValue(true),
      fetchTopicMetadata: jest.fn(),
      fetchTopicOffsets: jest.fn(),
      fetchOffsets: jest.fn(),
    };

    mockKafkaInstance = {
      admin: jest.fn().mockReturnValue(mockAdminInstance),
      producer: jest.fn().mockReturnValue({
        connect: jest.fn(),
        disconnect: jest.fn(),
        send: jest.fn(),
      }),
      consumer: jest.fn().mockReturnValue({
        connect: jest.fn(),
        disconnect: jest.fn(),
        subscribe: jest.fn(),
        run: jest.fn(),
        stop: jest.fn(),
      }),
    };

    mockKafka.mockReturnValue(mockKafkaInstance);
  });

  describe("Constants and Defaults", () => {
    it("should provide default topic configuration", () => {
      expect(KafkaManager.TOPIC_DEFAULTS).toBeDefined();
      expect(KafkaManager.TOPIC_DEFAULTS.NUM_PARTITIONS).toBe(3);
      expect(KafkaManager.TOPIC_DEFAULTS.REPLICATION_FACTOR).toBe(1);
    });

    it("should provide default client configuration", () => {
      expect(KafkaManager.CLIENT_DEFAULTS).toBeDefined();
      expect(KafkaManager.CLIENT_DEFAULTS.brokers).toContain("localhost:9092");
      expect(KafkaManager.CLIENT_DEFAULTS.connectionTimeout).toBe(1000);
    });

    it("should provide default consumer configuration", () => {
      expect(KafkaManager.CONSUMER_DEFAULTS).toBeDefined();
      expect(KafkaManager.CONSUMER_DEFAULTS.sessionTimeout).toBe(60000);
    });

    it("should provide default producer configuration", () => {
      expect(KafkaManager.PRODUCER_DEFAULTS).toBeDefined();
      expect(KafkaManager.PRODUCER_DEFAULTS.acks).toBe(-1);
      // Skip direct function comparison
      expect(typeof KafkaManager.PRODUCER_DEFAULTS.createPartitioner).toBe("function");
    });
  });

  describe("createClient", () => {
    it("should create a Kafka client with provided options", () => {
      // Arrange
      const clientOptions = {
        clientId: "test-client",
        brokers: ["kafka1:9092", "kafka2:9092"],
      };

      // Act
      const client = KafkaManager.createClient(clientOptions);

      // Assert
      expect(mockKafka).toHaveBeenCalledWith(clientOptions);
      expect(mockLogDebug).toHaveBeenCalledWith(expect.stringContaining("Kafka client created: test-client"));
    });

    it("should throw error if brokers are not provided", () => {
      expect(() => KafkaManager.createClient({})).toThrow(/must include 'brokers' array/);
    });
  });

  describe("createAdmin", () => {
    it("should create admin from existing client", () => {
      // Act
      const admin = KafkaManager.createAdmin(mockKafkaInstance);

      // Assert
      expect(mockKafkaInstance.admin).toHaveBeenCalled();
      expect(admin).toBe(mockAdminInstance);
    });

    it("should create admin with client options", () => {
      // Arrange
      const clientOptions = {
        clientId: "test-client",
        brokers: ["kafka:9092"],
      };

      // Act
      KafkaManager.createAdmin(null, clientOptions);

      // Assert
      expect(mockKafka).toHaveBeenCalledWith(clientOptions);
    });

    it("should throw error if no client or options provided", () => {
      expect(() => KafkaManager.createAdmin()).toThrow(/no client or clientOptions/);
    });
  });

  describe("createProducer", () => {
    it("should create producer from existing client", () => {
      // Arrange
      const mockProducerInstance = { connect: jest.fn() };
      const mockClient = {
        producer: jest.fn().mockReturnValue(mockProducerInstance),
      };
      const producerOptions = { acks: -1 };

      // Act
      const producer = KafkaManager.createProducer(mockClient, null, producerOptions);

      // Assert
      expect(mockClient.producer).toHaveBeenCalledWith(producerOptions);
      expect(producer).toBe(mockProducerInstance);
    });

    it("should create producer with client options", () => {
      // Arrange
      const clientOptions = {
        clientId: "test-client",
        brokers: ["kafka:9092"],
      };
      const producerOptions = { acks: -1 };

      // Act
      KafkaManager.createProducer(null, clientOptions, producerOptions);

      // Assert
      expect(mockKafka).toHaveBeenCalledWith(clientOptions);
    });

    it("should throw error if no client or options provided", () => {
      expect(() => KafkaManager.createProducer()).toThrow(/no client or clientOptions/);
    });
  });

  describe("createConsumer", () => {
    it("should create consumer from existing client", () => {
      // Arrange
      const mockConsumerInstance = { connect: jest.fn() };
      const mockClient = {
        consumer: jest.fn().mockReturnValue(mockConsumerInstance),
      };
      const consumerOptions = { groupId: "test-group" };

      // Act
      const consumer = KafkaManager.createConsumer(mockClient, null, consumerOptions);

      // Assert
      expect(mockClient.consumer).toHaveBeenCalledWith(consumerOptions);
      expect(consumer).toBe(mockConsumerInstance);
    });

    it("should create consumer with client options", () => {
      // Arrange
      const clientOptions = {
        clientId: "test-client",
        brokers: ["kafka:9092"],
      };
      const consumerOptions = { groupId: "test-group" };

      // Act
      KafkaManager.createConsumer(null, clientOptions, consumerOptions);

      // Assert
      expect(mockKafka).toHaveBeenCalledWith(clientOptions);
    });

    it("should throw error if no client or options provided", () => {
      expect(() => KafkaManager.createConsumer()).toThrow(/no client or clientOptions/);
    });
  });

  describe("createTopic", () => {
    it("should create topic successfully", async () => {
      // Arrange
      const mockAdmin = {
        createTopics: jest.fn().mockResolvedValue(true),
      };
      const topic = "test-topic";
      const topicOptions = {
        partitions: 3,
        replicationFactor: 1,
      };

      // Act
      const result = await KafkaManager.createTopic(mockAdmin, topic, topicOptions);

      // Assert
      expect(mockAdmin.createTopics).toHaveBeenCalledWith({
        topics: [
          {
            topic,
            numPartitions: topicOptions.partitions,
            replicationFactor: topicOptions.replicationFactor,
          },
        ],
      });
      expect(result).toBe(true);
    });

    it("should handle topic creation failure", async () => {
      // Arrange
      const mockAdmin = {
        createTopics: jest.fn().mockRejectedValue(new Error("Topic creation failed")),
      };
      const topic = "test-topic";
      const topicOptions = {
        partitions: 3,
        replicationFactor: 1,
      };

      // Act
      const result = await KafkaManager.createTopic(mockAdmin, topic, topicOptions);

      // Assert
      expect(mockAdmin.createTopics).toHaveBeenCalled();
      expect(result).toBe(false);
      expect(mockLogWarning).toHaveBeenCalledWith(expect.stringContaining(`Kafka topic [ ${topic} ] failed to create`));
    });
  });

  describe("isTopicExisted", () => {
    it("should return true when topic exists", async () => {
      // Arrange
      const mockAdmin = {
        fetchTopicMetadata: jest.fn().mockResolvedValue({
          topics: [{ name: "test-topic" }],
        }),
      };

      // Act
      const result = await KafkaManager.isTopicExisted(mockAdmin, "test-topic");

      // Assert
      expect(mockAdmin.fetchTopicMetadata).toHaveBeenCalledWith({
        topics: ["test-topic"],
      });
      expect(result).toBe(true);
    });

    it("should return false when topic doesn't exist", async () => {
      // Arrange
      const mockAdmin = {
        fetchTopicMetadata: jest.fn().mockResolvedValue({
          topics: [{ name: "other-topic" }],
        }),
      };

      // Act
      const result = await KafkaManager.isTopicExisted(mockAdmin, "test-topic");

      // Assert
      expect(result).toBe(false);
    });

    it("should return false and log warning on error", async () => {
      // Arrange
      const mockAdmin = {
        fetchTopicMetadata: jest.fn().mockRejectedValue(new Error("Topic not found")),
      };

      // Act
      const result = await KafkaManager.isTopicExisted(mockAdmin, "test-topic");

      // Assert
      expect(result).toBe(false);
      expect(mockLogWarning).toHaveBeenCalledWith(expect.stringContaining("Topic test-topic not found"));
    });
  });

  describe("getCompressionType", () => {
    it("should return GZIP compression type", () => {
      expect(KafkaManager.getCompressionType("gzip")).toBe(CompressionTypes.GZIP);
    });

    it("should return LZ4 compression type", () => {
      expect(KafkaManager.getCompressionType("lz4")).toBe(CompressionTypes.LZ4);
    });

    it("should return ZSTD compression type", () => {
      expect(KafkaManager.getCompressionType("zstd")).toBe(CompressionTypes.ZSTD);
    });

    it("should return None compression type for unknown types", () => {
      expect(KafkaManager.getCompressionType("unknown")).toBe(CompressionTypes.None);
      expect(KafkaManager.getCompressionType(null)).toBe(CompressionTypes.None);
      expect(KafkaManager.getCompressionType(undefined)).toBe(CompressionTypes.None);
    });
  });

  describe("standardizeConfig", () => {
    it("should standardize consumer configuration", () => {
      const userConfig = {
        topic: "test-topic",
        groupId: "test-group",
        brokerOptions: {
          clientOptions: {
            clientId: "test-client",
          },
        },
      };

      const result = KafkaManager.standardizeConfig(userConfig, "consumer");

      expect(result.topic).toBe("test-topic");
      expect(result.groupId).toBe("test-group");
      expect(result.clientOptions.clientId).toBe("test-client");
      expect(result.maxConcurrency).toBe(1);
      expect(result.consumerOptions).toBeDefined();
      expect(result.consumerOptions.groupId).toBe("test-group");
    });

    it("should standardize producer configuration", () => {
      const userConfig = {
        topic: "test-topic",
        brokerOptions: {
          producerOptions: {
            acks: 1,
          },
        },
      };

      const result = KafkaManager.standardizeConfig(userConfig, "producer");

      expect(result.topic).toBe("test-topic");
      expect(result.producerOptions).toBeDefined();
      expect(result.producerOptions.acks).toBe(1);
      expect(result.useSuppression).toBe(true);
      expect(result.useDistributedLock).toBe(true);
    });

    it("should throw error if freezing TTL is less than or equal to processing TTL", () => {
      const userConfig = {
        topic: "test-topic",
        cacheOptions: {
          processingTtl: 10000,
          suppressionTtl: 5000,
        },
      };

      expect(() => KafkaManager.standardizeConfig(userConfig, "consumer")).toThrow(
        /Processing time must be less then Freezing time/
      );
    });
  });

  describe("parseMessageValue", () => {
    it("should parse JSON message value", () => {
      const messageValue = Buffer.from('{"key":"value"}');

      const result = KafkaManager.parseMessageValue(messageValue);

      expect(result).toEqual({ key: "value" });
    });

    it("should return string for non-JSON content", () => {
      const messageValue = Buffer.from("plain text");

      const result = KafkaManager.parseMessageValue(messageValue);

      expect(result).toBe("plain text");
    });

    it("should return null for empty values", () => {
      expect(KafkaManager.parseMessageValue(null)).toBeNull();
      expect(KafkaManager.parseMessageValue(undefined)).toBeNull();
      expect(KafkaManager.parseMessageValue("")).toBeNull();
    });
  });

  describe("createMessageId", () => {
    it("should create message ID from topic, partition, and offset", () => {
      const messageId = KafkaManager.createMessageId("test-topic", 1, "100");

      expect(messageId).toBe("test-topic:1:100");
    });
  });

  describe("convertHeadersToBuffers", () => {
    it("should convert header values to buffers", () => {
      const headers = {
        textHeader: "text-value",
        numberHeader: 123,
        boolHeader: true,
        nullHeader: null,
      };

      const result = KafkaManager.convertHeadersToBuffers(headers);

      expect(result.textHeader).toBeInstanceOf(Buffer);
      expect(result.numberHeader).toBeInstanceOf(Buffer);
      expect(result.boolHeader).toBeInstanceOf(Buffer);
      expect(result.nullHeader).toBeUndefined();

      expect(result.textHeader.toString()).toBe("text-value");
      expect(result.numberHeader.toString()).toBe("123");
      expect(result.boolHeader.toString()).toBe("true");
    });

    it("should keep existing Buffer values", () => {
      const bufferValue = Buffer.from("already-buffer");
      const headers = {
        bufferHeader: bufferValue,
      };

      const result = KafkaManager.convertHeadersToBuffers(headers);

      expect(result.bufferHeader).toBe(bufferValue);
    });

    it("should handle invalid headers input", () => {
      expect(KafkaManager.convertHeadersToBuffers(null)).toEqual({});
      expect(KafkaManager.convertHeadersToBuffers("not-an-object")).toEqual({});
    });
  });

  describe("generateSequenceId", () => {
    it("should generate unique sequence IDs", () => {
      const id1 = KafkaManager.generateSequenceId();
      const id2 = KafkaManager.generateSequenceId();

      expect(id1).toMatch(/^seq_\d+_[a-z0-9]{7}$/);
      expect(id2).toMatch(/^seq_\d+_[a-z0-9]{7}$/);
      expect(id1).not.toBe(id2);
    });
  });

  describe("calculateConsumerLag", () => {
    it("should calculate consumer lag across partitions", async () => {
      // Arrange
      const mockAdmin = {
        fetchTopicOffsets: jest.fn().mockResolvedValue([
          { partition: 0, high: "100" },
          { partition: 1, high: "200" },
        ]),
        fetchOffsets: jest.fn().mockResolvedValue([
          {
            topic: "test-topic",
            partitions: [
              { partition: 0, offset: "50" },
              { partition: 1, offset: "150" },
            ],
          },
        ]),
      };

      // Act
      const lag = await KafkaManager.calculateConsumerLag("test-group", "test-topic", mockAdmin);

      // Assert
      expect(mockAdmin.fetchTopicOffsets).toHaveBeenCalledWith("test-topic");
      expect(mockAdmin.fetchOffsets).toHaveBeenCalledWith({
        groupId: "test-group",
        topics: ["test-topic"],
      });
      expect(lag).toBe(100); // (100-50) + (200-150) = 100
    });

    it("should return 0 when missing parameters", async () => {
      const lag = await KafkaManager.calculateConsumerLag("", "test-topic");
      expect(lag).toBe(0);
    });

    it("should handle group not found error", async () => {
      // Arrange
      const mockAdmin = {
        fetchTopicOffsets: jest.fn().mockResolvedValue([{ partition: 0, high: "100" }]),
        fetchOffsets: jest.fn().mockRejectedValue({
          type: "GROUP_ID_NOT_FOUND",
          message: "GroupIdNotFound",
        }),
      };

      // Act
      const lag = await KafkaManager.calculateConsumerLag("test-group", "test-topic", mockAdmin);

      // Assert
      expect(lag).toBe(100); // No committed offsets, so lag = latest offset
      expect(mockLogDebug).toHaveBeenCalledWith(expect.stringContaining("Consumer group 'test-group' not found"));
    });
  });

  describe("createMessages", () => {
    const originalCrypto = global.crypto;

    beforeEach(() => {
      global.crypto = { randomUUID: jest.fn().mockReturnValue("mock-uuid") };
    });

    afterEach(() => {
      global.crypto = originalCrypto;
    });

    it("should create formatted Kafka messages from items", () => {
      const items = [
        { id: "item1", data: "value1" },
        { id: "item2", data: "value2" },
      ];

      const messages = KafkaManager.createMessages(items, "test-type");

      expect(messages).toHaveLength(2);
      expect(messages[0].key).toBe("item1");
      expect(messages[1].key).toBe("item2");
      expect(JSON.parse(messages[0].value)).toEqual({ id: "item1", data: "value1" });
      expect(messages[0].headers).toBeDefined();
    });

    it("should use custom key function when provided", () => {
      const items = [{ id: "item1", customKey: "key1" }];
      const keyFn = item => item.customKey;

      const messages = KafkaManager.createMessages(items, "test-type", { key: keyFn });

      expect(messages[0].key).toBe("key1");
    });

    it("should use custom headers when provided", () => {
      const items = [{ id: "item1" }];
      const headers = { customHeader: "custom-value" };

      const messages = KafkaManager.createMessages(items, "test-type", { headers });

      expect(messages[0].headers.customHeader).toBeDefined();
    });

    it("should throw error for empty items array", () => {
      expect(() => KafkaManager.createMessages([], "test-type")).toThrow(/non-empty items array/);
    });

    it("should throw error for missing type", () => {
      expect(() => KafkaManager.createMessages([{ id: 1 }], "")).toThrow(/requires a type/);
    });

    it("should handle non-object message values", () => {
      const items = ["string-item", 123, true];

      const messages = KafkaManager.createMessages(items, "test-type");

      expect(messages).toHaveLength(3);
      expect(messages[0].value).toBe("string-item");
      expect(messages[1].value).toBe("123");
      expect(messages[2].value).toBe("true");
    });
  });
});
