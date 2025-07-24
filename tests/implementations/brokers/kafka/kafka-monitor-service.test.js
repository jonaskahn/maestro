/**
 * @jest-environment node
 */
// Define mock functions before importing modules
const mockCreateAdmin = jest.fn();
const mockIsTopicExisted = jest.fn();
const mockCalculateConsumerLag = jest.fn();
const mockLogDebug = jest.fn();
const mockLogError = jest.fn();
const mockLogWarning = jest.fn();
const mockLogInfo = jest.fn();
// Don't mock AbstractMonitorService, we need a real instance
// Just mock the connect and disconnect methods in our tests
jest.mock("../../../../src/services/logger-service", () => ({
  logDebug: mockLogDebug,
  logError: mockLogError,
  logWarning: mockLogWarning,
  logInfo: mockLogInfo,
}));
jest.mock("../../../../src/implementations/brokers/kafka/kafka-manager", () => ({
  createAdmin: mockCreateAdmin,
  isTopicExisted: mockIsTopicExisted,
  calculateConsumerLag: mockCalculateConsumerLag,
}));

const KafkaMonitorService = require("../../../../src/implementations/brokers/kafka/kafka-monitor-service");
const AbstractMonitorService = require("../../../../src/abstracts/abstract-monitor-service");
const KafkaManager = require("../../../../src/implementations/brokers/kafka/kafka-manager");
const logger = require("../../../../src/services/logger-service");
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

    mockCreateAdmin.mockResolvedValue(mockAdmin);
    mockIsTopicExisted.mockResolvedValue(true);
    mockCalculateConsumerLag.mockResolvedValue(50);

    monitorService = new KafkaMonitorService(testConfig);
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
    test("Given Test setup for should initialize with configuration When Action being tested Then Expected outcome", () => {
      expect(monitorService._groupId).toBe("test-group");
      expect(monitorService._clientOptions).toBe(testConfig.clientOptions);
      expect(monitorService._topic).toBe("test-topic");
    });
  });

  describe("getBrokerType", () => {
    test("Given Test setup for should return 'kafka' When Action being tested Then Expected outcome", () => {
      expect(monitorService.getBrokerType()).toBe("kafka");
    });
  });

  describe("connect", () => {
    test("should connect admin client", async () => {
      // Mock the parent class connect method
      const superConnectSpy = jest.spyOn(AbstractMonitorService.prototype, "connect").mockResolvedValue();

      await monitorService.connect();

      expect(superConnectSpy).toHaveBeenCalled();
      expect(mockCreateAdmin).toHaveBeenCalledWith(null, testConfig.clientOptions);
      expect(mockAdmin.connect).toHaveBeenCalled();
      expect(monitorService._admin).toBe(mockAdmin);

      superConnectSpy.mockRestore();
    });
  });

  describe("getConsumerLag", () => {
    beforeEach(() => {
      monitorService._admin = mockAdmin;
    });

    test("should return lag metrics successfully", async () => {
      // Mock internal method
      jest.spyOn(monitorService, "_fetchCurrentLag").mockResolvedValue(50);

      const result = await monitorService.getConsumerLag();

      expect(mockLogDebug).toHaveBeenCalledWith(
        expect.stringContaining("Start monitoring current consumer for topic test-topic lag")
      );

      expect(result).toEqual({
        totalLag: 50,
        maxPartitionLag: 50,
        avgLag: 50,
        lagThreshold: 100,
      });
    });

    test("should handle errors during lag calculation", async () => {
      const testError = new Error("Lag calculation failed");
      jest.spyOn(monitorService, "_fetchCurrentLag").mockRejectedValue(testError);

      await expect(monitorService.getConsumerLag()).rejects.toThrow("Lag calculation failed");

      expect(mockLogError).toHaveBeenCalledWith(
        expect.stringContaining("Failed monitoring current consumer"),
        testError
      );
    });
  });

  describe("getResourceMetrics", () => {
    test("should return resource metrics with memory usage", async () => {
      const result = monitorService.getResourceMetrics();

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

    test("should calculate lag when topic exists", async () => {
      mockIsTopicExisted.mockResolvedValue(true);
      mockCalculateConsumerLag.mockResolvedValue(75);

      const lag = await monitorService._fetchCurrentLag();

      expect(mockIsTopicExisted).toHaveBeenCalledWith(mockAdmin, "test-topic");
      expect(mockCalculateConsumerLag).toHaveBeenCalledWith("test-group", "test-topic", mockAdmin);
      expect(lag).toBe(75);
    });

    test("should return 0 when topic doesn't exist", async () => {
      mockIsTopicExisted.mockResolvedValue(false);

      const lag = await monitorService._fetchCurrentLag();

      expect(mockIsTopicExisted).toHaveBeenCalledWith(mockAdmin, "test-topic");
      expect(mockCalculateConsumerLag).not.toHaveBeenCalled();
      expect(lag).toBe(0);
    });

    test("should return 0 and log warning when missing topic or group", async () => {
      monitorService._topic = null;
      monitorService._groupId = null;

      const lag = await monitorService._fetchCurrentLag();

      expect(mockLogWarning).toHaveBeenCalledWith(expect.stringContaining("Consumer group or topic not configured"));
      expect(lag).toBe(0);
    });
  });

  describe("disconnect", () => {
    beforeEach(() => {
      monitorService._admin = mockAdmin;
    });

    test("should disconnect admin client", async () => {
      const superDisconnectSpy = jest.spyOn(AbstractMonitorService.prototype, "disconnect").mockResolvedValue();

      await monitorService.disconnect();

      expect(mockAdmin.disconnect).toHaveBeenCalled();
      expect(superDisconnectSpy).toHaveBeenCalled();
      expect(monitorService._admin).toBeNull();

      superDisconnectSpy.mockRestore();
    });

    test("should handle errors during disconnect", async () => {
      const disconnectError = new Error("Disconnect error");
      mockAdmin.disconnect.mockRejectedValueOnce(disconnectError);
      const superDisconnectSpy = jest.spyOn(AbstractMonitorService.prototype, "disconnect").mockResolvedValue();

      await monitorService.disconnect();

      expect(mockLogWarning).toHaveBeenCalledWith(
        "⚠️ Errors happened when Kafka Monitor Service disconnect its services",
        expect.any(Error)
      );
      expect(superDisconnectSpy).toHaveBeenCalled();

      superDisconnectSpy.mockRestore();
    });
  });
});
