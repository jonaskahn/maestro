/**
 * @jest-environment node
 */
jest.mock("../../src/services/logger-service", () => ({
  logInfo: jest.fn(),
  logDebug: jest.fn(),
  logWarning: jest.fn(),
  logError: jest.fn(),
}));
jest.mock("../../src/config/ttl-config", () => ({
  getBackpressureConfig: jest.fn().mockReturnValue({
    checkInterval: 15000,
    cacheTTL: 30000,
  }),
  getAllTtlValues: jest.fn().mockReturnValue({
    BACKOFF_MIN_DELAY: 50,
    BACKOFF_MAX_DELAY: 5000,
  }),
}));

const logger = require("../../src/services/logger-service");
const AbstractMonitorService = require("../../src/abstracts/abstract-monitor-service");

class TestMonitorService extends AbstractMonitorService {
  getBrokerType() {
    return "test-broker";
  }

  async getConsumerLag() {
    return Promise.resolve({
      totalLag: this._mockTotalLag || 0,
      maxPartitionLag: this._mockMaxPartitionLag || 0,
      avgLag: this._mockAvgLag || 0,
      lagThreshold: this.config.lagThreshold,
    });
  }
  getResourceMetrics() {
    return {
      memoryUsage: this._mockMemoryUsage || 0,
      cpuUsage: this._mockCpuUsage || 0,
      networkLatency: this._mockNetworkLatency || 0,
    };
  }
  setMockLagMetrics(totalLag, maxPartitionLag = 0, avgLag = 0) {
    this._mockTotalLag = totalLag;
    this._mockMaxPartitionLag = maxPartitionLag;
    this._mockAvgLag = avgLag;
  }
  setMockResourceMetrics(memoryUsage = 0, cpuUsage = 0, networkLatency = 0) {
    this._mockMemoryUsage = memoryUsage;
    this._mockCpuUsage = cpuUsage;
    this._mockNetworkLatency = networkLatency;
  }
}

describe("AbstractMonitorService", () => {
  let monitorService;
  const defaultConfig = {
    topic: "test-topic",
    maxLag: 100,
    enabledResourceLag: true,
    checkInterval: 1000,
    rateLimitThreshold: 100,
    cacheTTL: 5000,
    initialDelay: 50,
    maxDelay: 5000,
    exponentialFactor: 2,
  };
  beforeEach(() => {
    jest.clearAllMocks();
    monitorService = new TestMonitorService(defaultConfig);
  });

  afterEach(async () => {
    if (monitorService.monitoringInterval) {
      await monitorService.stopMonitoring();
    }
  });

  describe("Constructor and initialization", () => {
    test("Given Test setup for Given AbstractMonitorService class When instantiated directly Then should throw error When Action being tested Then Expected outcome", () => {
      expect(() => new AbstractMonitorService(defaultConfig)).toThrow(
        "AbstractMonitorService cannot be instantiated directly"
      );
    });

    test("Given Test setup for Given missing configuration When instantiated Then should throw error When Action being tested Then Expected outcome", () => {
      expect(() => new TestMonitorService()).toThrow("Backpressure monitor configuration must be an object");
    });

    test("Given minimal configuration When instantiated Then should initialize with defaults", () => {
      const minimalConfig = { topic: "minimal-topic" };
      const service = new TestMonitorService(minimalConfig);

      expect(service.config).toBeDefined();
      expect(service.config.lagThreshold).toBe(100);
      expect(service.config.enabledResourceLag).toBe(false);
      expect(service.config.checkInterval).toBe(15000);
      expect(service.config.rateLimitThreshold).toBe(100);
      expect(service._topic).toBe("minimal-topic");
    });

    test("Given Test setup for Given custom configuration When instantiated Then should use custom values When Action being tested Then Expected outcome", () => {
      expect(monitorService.config.lagThreshold).toBe(100);
      expect(monitorService.config.enabledResourceLag).toBe(defaultConfig.enabledResourceLag);
      expect(monitorService.config.checkInterval).toBe(defaultConfig.checkInterval);
      expect(monitorService.config.rateLimitThreshold).toBe(defaultConfig.rateLimitThreshold);
      expect(monitorService._topic).toBe(defaultConfig.topic);
    });

    test("Given Test setup for Given new instance When created Then should initialize monitoring as disabled When Action being tested Then Expected outcome", () => {
      expect(monitorService.isMonitoring).toBe(false);
    });

    test("Given new instance When created Then should log initialization info", () => {
      expect(logger.logDebug).toHaveBeenCalledWith(
        expect.stringContaining(
          `Backpressure monitor configured for broker test-broker on topic [ ${defaultConfig.topic} ]`
        )
      );
      expect(logger.logDebug).toHaveBeenCalledWith(
        expect.stringContaining(
          `Backpressure monitor configuration for broker test-broker on topic [ ${defaultConfig.topic} ]`
        ),
        expect.objectContaining({
          lagThreshold: expect.any(Number),
          rateLimitThreshold: expect.any(Number),
        })
      );
    });
  });

  describe("getEnvironmentValueOrDefault", () => {
    const originalEnv = process.env;

    beforeEach(() => {
      process.env = { ...originalEnv };
    });

    afterEach(() => {
      process.env = originalEnv;
    });

    test("Given Test setup for Given unset environment variables When getEnvironmentValueOrDefault called Then should return default value When Action being tested Then Expected outcome", () => {
      const result = monitorService.getEnvironmentValueOrDefault(["TEST_VAR"], 42);
      expect(result).toBe(42);
    });

    test("Given Test setup for Given set environment variable When getEnvironmentValueOrDefault called Then should return environment value When Action being tested Then Expected outcome", () => {
      process.env.TEST_VAR = "123";
      const result = monitorService.getEnvironmentValueOrDefault(["TEST_VAR"], 42);
      expect(result).toBe(123);
    });

    test("Given Test setup for Given multiple environment keys When getEnvironmentValueOrDefault called Then should check in order When Action being tested Then Expected outcome", () => {
      process.env.SECOND_VAR = "456";
      const result = monitorService.getEnvironmentValueOrDefault(["FIRST_VAR", "SECOND_VAR"], 42);
      expect(result).toBe(456);
    });
  });

  describe("getBrokerType", () => {
    test("Given Test setup for Given monitor service When getBrokerType called Then should return correct broker type When Action being tested Then Expected outcome", () => {
      expect(monitorService.getBrokerType()).toBe("test-broker");
    });
  });

  describe("Connect and Disconnect", () => {
    test("Given monitor service When connect called Then should start monitoring", async () => {
      const startSpy = jest.spyOn(monitorService, "startMonitoring");
      await monitorService.connect();

      expect(startSpy).toHaveBeenCalled();
      expect(monitorService.isMonitoring).toBe(true);
    });

    test("Given connected monitor service When disconnect called Then should stop monitoring", async () => {
      await monitorService.connect();
      const stopSpy = jest.spyOn(monitorService, "stopMonitoring");

      await monitorService.disconnect();

      expect(stopSpy).toHaveBeenCalled();
      expect(monitorService.isMonitoring).toBe(false);
    });
  });

  describe("startMonitoring", () => {
    test("Given inactive monitoring When startMonitoring called Then should start monitoring", async () => {
      await monitorService.startMonitoring();

      expect(monitorService.isMonitoring).toBe(true);
      expect(monitorService.monitoringInterval).toBeDefined();
      expect(logger.logDebug).toHaveBeenCalledWith(
        expect.stringContaining("Backpressure monitoring started for test-broker")
      );
    });

    test("Given active monitoring When startMonitoring called Then should do nothing", async () => {
      await monitorService.startMonitoring();
      logger.logInfo.mockClear();

      await monitorService.startMonitoring();

      expect(logger.logWarning).toHaveBeenCalledWith(
        expect.stringContaining("Backpressure monitor for test-broker is already active")
      );
    });
  });

  describe("stopMonitoring", () => {
    beforeEach(async () => {
      await monitorService.startMonitoring();
    });

    test("Given active monitoring When stopMonitoring called Then should stop monitoring", async () => {
      await monitorService.stopMonitoring();

      expect(monitorService.isMonitoring).toBe(false);
      expect(monitorService.monitoringInterval).toBeNull();
      expect(logger.logDebug).toHaveBeenCalledWith(
        expect.stringContaining("Backpressure monitoring stopped for test-broker")
      );
    });

    test("Given inactive monitoring When stopMonitoring called Then should do nothing", async () => {
      await monitorService.stopMonitoring();
      logger.logInfo.mockClear();

      await monitorService.stopMonitoring();

      expect(logger.logWarning).toHaveBeenCalledWith(
        expect.stringContaining("Backpressure monitor for test-broker is not active")
      );
    });
  });

  describe("performMonitoringCheck", () => {
    test("Given monitor service When performMonitoringCheck called Then should check lag and update status", async () => {
      const collectSpy = jest.spyOn(monitorService, "collectCurrentMetrics");
      const calculateSpy = jest.spyOn(monitorService, "calculateBackpressureLevel");

      await monitorService.performMonitoringCheck();

      expect(collectSpy).toHaveBeenCalled();
      expect(calculateSpy).toHaveBeenCalled();
    });

    test("Given high lag When performMonitoringCheck called Then should log warning", async () => {
      monitorService.setMockLagMetrics(200);

      await monitorService.performMonitoringCheck();

      expect(logger.logWarning).toHaveBeenCalledWith(expect.stringContaining("Backpressure detected"));
    });

    test("Given monitoring error When performMonitoringCheck called Then should handle errors", async () => {
      jest.spyOn(monitorService, "collectCurrentMetrics").mockRejectedValue(new Error("Test error"));

      await monitorService.performMonitoringCheck();

      expect(logger.logWarning).toHaveBeenCalledWith(
        expect.stringContaining("Error during backpressure monitoring check"),
        expect.any(Error)
      );
    });
  });

  describe("collectLagMetrics", () => {
    test("Given configured metrics When collectLagMetrics called Then should retrieve consumer lag metrics", async () => {
      monitorService.setMockLagMetrics(100, 50, 25);

      const metrics = await monitorService.collectLagMetrics();

      expect(metrics.totalLag).toBe(100);
      expect(metrics.maxPartitionLag).toBe(50);
      expect(metrics.avgLag).toBe(25);
    });

    test("Given error in getConsumerLag When collectLagMetrics called Then should return defaults and log warning", async () => {
      jest.spyOn(monitorService, "getConsumerLag").mockRejectedValue(new Error("Test error"));

      const metrics = await monitorService.collectLagMetrics();

      expect(metrics.totalLag).toBe(0);
      expect(logger.logWarning).toHaveBeenCalled();
    });
  });

  describe("collectResourceMetrics", () => {
    test("Given Test setup for Given enabled resource monitoring When collectResourceMetrics called Then should retrieve metrics When Action being tested Then Expected outcome", () => {
      monitorService.setMockResourceMetrics(60, 70, 30);

      const metrics = monitorService.collectResourceMetrics();

      expect(metrics.memoryUsage).toBe(60);
      expect(metrics.cpuUsage).toBe(70);
      expect(metrics.networkLatency).toBe(30);
    });

    test("Given Test setup for Given disabled resource monitoring When collectResourceMetrics called Then should return defaults When Action being tested Then Expected outcome", () => {
      monitorService.config.enabledResourceLag = false;

      const metrics = monitorService.collectResourceMetrics();

      expect(metrics.memoryUsage).toBe(0);
      expect(metrics.cpuUsage).toBe(0);
    });
  });

  describe("collectCurrentMetrics", () => {
    test("Given configured metrics When collectCurrentMetrics called Then should collect and combine metrics", async () => {
      monitorService.setMockLagMetrics(100, 50, 25);
      monitorService.setMockResourceMetrics(60, 70, 30);

      const metrics = await monitorService.collectCurrentMetrics();

      expect(metrics.totalLag).toBe(100);
      expect(metrics.memoryUsage).toBe(60);
      expect(metrics.cpuUsage).toBe(70);
      expect(metrics.timestamp).toBeDefined();
      expect(metrics.brokerType).toBe("test-broker");
    });
  });

  describe("calculateBackpressureLevel", () => {
    test("Given high metrics When calculateBackpressureLevel called Then should determine HIGH level", () => {
      const metrics = { totalLag: 200, cpuUsage: 50, memoryUsage: 40 };

      const level = monitorService.calculateBackpressureLevel(metrics);

      expect(level).toBe("CRITICAL");
    });

    test("Given low metrics When calculateBackpressureLevel called Then should determine NONE level", () => {
      const metrics = { totalLag: 10, cpuUsage: 20, memoryUsage: 30 };

      const level = monitorService.calculateBackpressureLevel(metrics);

      expect(level).toBe("NONE");
    });
  });

  describe("getHighestBackpressureLevel", () => {
    test("Given Test setup for Given two backpressure levels When getHighestBackpressureLevel called Then should return highest level When Action being tested Then Expected outcome", () => {
      expect(monitorService.getHighestBackpressureLevel("MEDIUM", "HIGH")).toBe("HIGH");
      expect(monitorService.getHighestBackpressureLevel("CRITICAL", "MEDIUM")).toBe("CRITICAL");
      expect(monitorService.getHighestBackpressureLevel("LOW", "LOW")).toBe("LOW");
    });
  });

  describe("getBackpressureStatus", () => {
    test("Given metrics When getBackpressureStatus called Then should return complete status", async () => {
      monitorService.setMockLagMetrics(150);
      monitorService.setMockResourceMetrics(70);
      await monitorService.performMonitoringCheck();

      const status = await monitorService.getBackpressureStatus();

      expect(status.backpressureLevel).toBeDefined();
      expect(status.metrics).toBeDefined();
      expect(status.metrics.lag).toBeDefined();
      expect(status.metrics.resources).toBeDefined();
    });

    test("Given error in collection When getBackpressureStatus called Then should handle errors", async () => {
      jest.spyOn(monitorService, "collectCurrentMetrics").mockRejectedValue(new Error("Test error"));

      const status = await monitorService.getBackpressureStatus();

      expect(status.backpressureLevel).toBe("NONE");
      expect(status.error).toBeDefined();
    });
  });

  describe("shouldPauseProcessing", () => {
    test("Given HIGH or CRITICAL backpressure When shouldPauseProcessing called Then should return true", async () => {
      jest.spyOn(monitorService, "getBackpressureStatus").mockResolvedValueOnce({
        backpressureLevel: "HIGH",
        shouldPause: true,
      });

      expect(await monitorService.shouldPauseProcessing()).toBe(true);

      jest.spyOn(monitorService, "getBackpressureStatus").mockResolvedValueOnce({
        backpressureLevel: "CRITICAL",
        shouldPause: true,
      });

      expect(await monitorService.shouldPauseProcessing()).toBe(true);
    });

    test("Given backpressure below HIGH When shouldPauseProcessing called Then should return false", async () => {
      jest.spyOn(monitorService, "getBackpressureStatus").mockResolvedValueOnce({
        backpressureLevel: "MEDIUM",
        shouldPause: false,
      });

      expect(await monitorService.shouldPauseProcessing()).toBe(false);

      jest.spyOn(monitorService, "getBackpressureStatus").mockResolvedValueOnce({
        backpressureLevel: "LOW",
        shouldPause: false,
      });

      expect(await monitorService.shouldPauseProcessing()).toBe(false);
    });
  });

  describe("getRecommendedDelay", () => {
    test("Given no backpressure When getRecommendedDelay called Then should return 0", async () => {
      jest.spyOn(monitorService, "getBackpressureStatus").mockResolvedValue({
        backpressureLevel: "NONE",
        recommendedDelay: 0,
      });

      const delay = await monitorService.getRecommendedDelay();
      expect(delay).toBe(0);
    });

    test("Given high backpressure When getRecommendedDelay called Then should return proportional delay", async () => {
      jest.spyOn(monitorService, "getBackpressureStatus").mockResolvedValue({
        backpressureLevel: "HIGH",
        recommendedDelay: 200,
      });

      const delay = await monitorService.getRecommendedDelay();
      expect(delay).toBe(200);
    });
  });
});
