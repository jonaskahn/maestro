/**
 * @jest-environment node
 */

const KafkaMonitorService = require("../../../../src/implementations/brokers/kafka/kafka-monitor-service");
const AbstractMonitorService = require("../../../../src/abstracts/abstract-monitor-service");
const KafkaManager = require("../../../../src/implementations/brokers/kafka/kafka-manager");
const logger = require("../../../../src/services/logger-service");

// Mock dependencies
jest.mock("../../../../src/abstracts/abstract-monitor-service");
jest.mock("../../../../src/services/logger-service", () => ({
  logDebug: jest.fn(),
  logError: jest.fn(),
  logWarning: jest.fn(),
  logInfo: jest.fn(),
}));
jest.mock("../../../../src/implementations/brokers/kafka/kafka-manager", () => ({
  createAdmin: jest.fn(),
  isTopicExisted: jest.fn(),
  calculateConsumerLag: jest.fn(),
}));

// Save original process.memoryUsage
const originalMemoryUsage = process.memoryUsage;

describe("KafkaMonitorService", () => {
  let monitorService;
  let mockAdmin;
  const testConfig = {
    topic: "test-topic",
    groupId: "test-group",
    clientOptions: { brokers: ["localhost:9092"] },
    lagThreshold: 100,
  };

  beforeEach(() => {
    jest.clearAllMocks();

    process.memoryUsage = jest.fn().mockReturnValue({
      heapUsed: 50,
      heapTotal: 100,
    });

    mockAdmin = {
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
    };

    KafkaManager.createAdmin.mockResolvedValue(mockAdmin);
    KafkaManager.isTopicExisted.mockResolvedValue(true);
    KafkaManager.calculateConsumerLag.mockResolvedValue(50);

    monitorService = new KafkaMonitorService(testConfig);

    monitorService.config = { lagThreshold: 100 };
  });

  afterEach(() => {
    if (monitorService._admin) {
      delete monitorService._admin;
    }
  });

  afterAll(() => {
    process.memoryUsage = originalMemoryUsage;
  });

  describe("Constructor", () => {
    it("should initialize with configuration", () => {
      expect(AbstractMonitorService).toHaveBeenCalledWith(testConfig);
      expect(monitorService._topic).toBe("test-topic");
      expect(monitorService._groupId).toBe("test-group");
      expect(monitorService._clientOptions).toBe(testConfig.clientOptions);
    });
  });

  describe("getBrokerType", () => {
    it("should return 'kafka'", () => {
      expect(monitorService.getBrokerType()).toBe("kafka");
    });
  });

  describe("connect", () => {
    it("should connect admin client", async () => {
      const superConnectSpy = jest.spyOn(AbstractMonitorService.prototype, "connect").mockResolvedValue();

      await monitorService.connect();

      expect(superConnectSpy).toHaveBeenCalled();
      expect(KafkaManager.createAdmin).toHaveBeenCalledWith(null, testConfig.clientOptions);
      expect(mockAdmin.connect).toHaveBeenCalled();
      expect(monitorService._admin).toBe(mockAdmin);

      superConnectSpy.mockRestore();
    });
  });

  describe("getConsumerLag", () => {
    beforeEach(() => {
      monitorService._admin = mockAdmin;
    });

    it("should return lag metrics successfully", async () => {
      const result = await monitorService.getConsumerLag();

      expect(KafkaManager.calculateConsumerLag).toHaveBeenCalledWith("test-group", "test-topic", mockAdmin);
      expect(result).toEqual({
        totalLag: 50,
        maxPartitionLag: 50,
        avgLag: 50,
        lagThreshold: 100,
      });
      expect(logger.logDebug).toHaveBeenCalledWith(
        expect.stringContaining("Start monitoring current consumer for topic test-topic lag")
      );
    });

    it("should handle errors during lag calculation", async () => {
      const testError = new Error("Lag calculation failed");
      KafkaManager.calculateConsumerLag.mockRejectedValueOnce(testError);

      await expect(monitorService.getConsumerLag()).rejects.toThrow("Lag calculation failed");

      expect(logger.logError).toHaveBeenCalledWith(
        expect.stringContaining("Failed monitoring current consumer"),
        testError
      );
    });
  });

  describe("getResourceMetrics", () => {
    it("should return resource metrics with memory usage", async () => {
      const result = await monitorService.getResourceMetrics();

      expect(result).toEqual({
        memoryUsage: 50,
        cpuUsage: 0,
        networkLatency: 0,
      });
    });
  });

  describe("_fetchCurrentLag", () => {
    beforeEach(() => {
      monitorService._admin = mockAdmin;
    });

    it("should calculate lag when topic exists", async () => {
      KafkaManager.isTopicExisted.mockResolvedValue(true);
      KafkaManager.calculateConsumerLag.mockResolvedValue(75);

      const lag = await monitorService._fetchCurrentLag();

      expect(KafkaManager.isTopicExisted).toHaveBeenCalledWith(mockAdmin, "test-topic");
      expect(KafkaManager.calculateConsumerLag).toHaveBeenCalledWith("test-group", "test-topic", mockAdmin);
      expect(lag).toBe(75);
    });

    it("should return 0 when topic doesn't exist", async () => {
      KafkaManager.isTopicExisted.mockResolvedValue(false);

      const lag = await monitorService._fetchCurrentLag();

      expect(KafkaManager.isTopicExisted).toHaveBeenCalledWith(mockAdmin, "test-topic");
      expect(KafkaManager.calculateConsumerLag).not.toHaveBeenCalled();
      expect(lag).toBe(0);
    });

    it("should return 0 and log warning when missing topic or group", async () => {
      monitorService._topic = null;
      monitorService._groupId = null;

      const lag = await monitorService._fetchCurrentLag();

      expect(logger.logWarning).toHaveBeenCalledWith(expect.stringContaining("Consumer group or topic not configured"));
      expect(lag).toBe(0);
    });
  });

  describe("disconnect", () => {
    beforeEach(() => {
      monitorService._admin = mockAdmin;
    });

    it("should disconnect admin client", async () => {
      const superDisconnectSpy = jest.spyOn(AbstractMonitorService.prototype, "disconnect").mockResolvedValue();

      await monitorService.disconnect();

      expect(mockAdmin.disconnect).toHaveBeenCalled();
      expect(superDisconnectSpy).toHaveBeenCalled();
      expect(monitorService._admin).toBeNull();

      superDisconnectSpy.mockRestore();
    });
  });
});
