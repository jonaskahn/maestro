/**
 * @jest-environment node
 */

const AbstractMonitorService = require("../../src/abstracts/abstract-monitor-service");
const logger = require("../../src/services/logger-service");

// Mock dependencies
jest.mock("../../src/services/logger-service", () => ({
  logInfo: jest.fn(),
  logDebug: jest.fn(),
  logWarning: jest.fn(),
  logError: jest.fn(),
}));

// Mock TtlConfig
jest.mock("../../src/config/ttl-config", () => ({
  getBackpressureConfig: jest.fn().mockReturnValue({
    checkInterval: 5000,
    cacheTTL: 30000,
  }),
  getAllTTLValues: jest.fn().mockReturnValue({
    BACKOFF_MIN_DELAY: 50,
    BACKOFF_MAX_DELAY: 5000,
  }),
}));

// Test implementation of AbstractMonitorService
class TestMonitorService extends AbstractMonitorService {
  getBrokerType() {
    return "test-broker";
  }

  async getConsumerLag() {
    return {
      totalLag: this._mockTotalLag || 0,
      maxPartitionLag: this._mockMaxPartitionLag || 0,
      avgLag: this._mockAvgLag || 0,
      lagThreshold: this.config.lagThreshold,
    };
  }

  async getResourceMetrics() {
    return {
      memoryUsage: this._mockMemoryUsage || 0,
      cpuUsage: this._mockCpuUsage || 0,
      networkLatency: this._mockNetworkLatency || 0,
    };
  }

  // Methods to set mock values for testing
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
    // Clean up any active monitoring
    if (monitorService.monitoringInterval) {
      await monitorService.stopMonitoring();
    }
  });

  describe("Constructor and initialization", () => {
    it("should throw an error when instantiated directly", () => {
      expect(() => new AbstractMonitorService(defaultConfig)).toThrow(
        "AbstractMonitorService cannot be instantiated directly"
      );
    });

    it("should throw an error when configuration is not provided", () => {
      expect(() => new TestMonitorService()).toThrow("Backpressure monitor configuration must be an object");
    });

    it("should initialize with default configuration values", () => {
      const minimalConfig = {};
      const service = new TestMonitorService(minimalConfig);

      expect(service.config).toBeDefined();
      expect(service.config.lagThreshold).toBe(100);
      expect(service.config.enabledResourceLag).toBe(false);
      expect(service.config.checkInterval).toBe(5000);
      expect(service.config.rateLimitThreshold).toBe(100);
    });

    it("should initialize with custom configuration values", () => {
      expect(monitorService.config.lagThreshold).toBe(100); // defaultConfig.maxLag gets mapped to lagThreshold internally
      expect(monitorService.config.enabledResourceLag).toBe(defaultConfig.enabledResourceLag);
      expect(monitorService.config.checkInterval).toBe(defaultConfig.checkInterval);
      expect(monitorService.config.rateLimitThreshold).toBe(defaultConfig.rateLimitThreshold);
    });

    it("should initialize monitoring as disabled", () => {
      expect(monitorService.isMonitoring).toBe(false);
    });

    it("should log initialization info", () => {
      expect(logger.logInfo).toHaveBeenCalledWith(
        expect.stringContaining("Backpressure monitor configured for test-broker")
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

    it("should return default value when environment variables are not set", () => {
      const result = monitorService.getEnvironmentValueOrDefault(["TEST_VAR"], 42);
      expect(result).toBe(42);
    });

    it("should return environment value when available", () => {
      process.env.TEST_VAR = "123";
      const result = monitorService.getEnvironmentValueOrDefault(["TEST_VAR"], 42);
      expect(result).toBe(123);
    });

    it("should check multiple environment keys in order", () => {
      process.env.SECOND_VAR = "456";
      const result = monitorService.getEnvironmentValueOrDefault(["FIRST_VAR", "SECOND_VAR"], 42);
      expect(result).toBe(456);
    });
  });

  describe("getBrokerType", () => {
    it("should return the correct broker type from implementation", () => {
      expect(monitorService.getBrokerType()).toBe("test-broker");
    });
  });

  describe("Connect and Disconnect", () => {
    it("should start monitoring on connect", async () => {
      const startSpy = jest.spyOn(monitorService, "startMonitoring");
      await monitorService.connect();

      expect(startSpy).toHaveBeenCalled();
      expect(monitorService.isMonitoring).toBe(true);
    });

    it("should stop monitoring on disconnect", async () => {
      await monitorService.connect();
      const stopSpy = jest.spyOn(monitorService, "stopMonitoring");

      await monitorService.disconnect();

      expect(stopSpy).toHaveBeenCalled();
      expect(monitorService.isMonitoring).toBe(false);
    });
  });

  describe("startMonitoring", () => {
    it("should start monitoring when not already monitoring", async () => {
      await monitorService.startMonitoring();

      expect(monitorService.isMonitoring).toBe(true);
      expect(monitorService.monitoringInterval).toBeDefined();
      expect(logger.logInfo).toHaveBeenCalledWith(
        expect.stringContaining("Backpressure monitoring started for test-broker")
      );
    });

    it("should not start monitoring when already monitoring", async () => {
      await monitorService.startMonitoring();
      logger.logInfo.mockClear();

      await monitorService.startMonitoring();

      expect(logger.logWarning).toHaveBeenCalledWith(
        expect.stringContaining("Backpressure monitor for test-broker is already active")
      );
    });
  });

  describe("stopMonitoring", () => {
    it("should stop monitoring when active", async () => {
      await monitorService.startMonitoring();
      await monitorService.stopMonitoring();

      expect(monitorService.isMonitoring).toBe(false);
      expect(monitorService.monitoringInterval).toBeNull();
      expect(logger.logInfo).toHaveBeenCalledWith(
        expect.stringContaining("Backpressure monitoring stopped for test-broker")
      );
    });

    it("should not stop monitoring when not active", async () => {
      await monitorService.stopMonitoring();

      expect(logger.logWarning).toHaveBeenCalledWith(
        expect.stringContaining("Backpressure monitor for test-broker is not active")
      );
    });
  });

  describe("performMonitoringCheck", () => {
    it("should collect metrics and calculate backpressure level", async () => {
      const collectSpy = jest.spyOn(monitorService, "collectCurrentMetrics");
      const calculateSpy = jest.spyOn(monitorService, "calculateBackpressureLevel");

      await monitorService.performMonitoringCheck();

      expect(collectSpy).toHaveBeenCalled();
      expect(calculateSpy).toHaveBeenCalled();
      expect(monitorService.lastMetrics).toBeDefined();
      expect(monitorService.lastMetricsTime).toBeDefined();
    });

    it("should log warning when backpressure is detected", async () => {
      monitorService.setMockLagMetrics(200);
      jest.spyOn(monitorService, "calculateBackpressureLevel").mockReturnValue("HIGH");

      await monitorService.performMonitoringCheck();

      expect(logger.logWarning).toHaveBeenCalledWith(expect.stringContaining("Backpressure detected (HIGH)"));
    });

    it("should handle errors during check", async () => {
      jest.spyOn(monitorService, "collectCurrentMetrics").mockRejectedValue(new Error("Check failed"));

      await monitorService.performMonitoringCheck();

      expect(logger.logWarning).toHaveBeenCalledWith("Error during backpressure monitoring check", expect.any(Error));
    });
  });

  describe("collectCurrentMetrics", () => {
    it("should collect and combine lag and resource metrics", async () => {
      monitorService.setMockLagMetrics(100);
      monitorService.setMockResourceMetrics(60, 70);

      const metrics = await monitorService.collectCurrentMetrics();

      expect(metrics.totalLag).toBe(100);
      expect(metrics.memoryUsage).toBe(60);
      expect(metrics.cpuUsage).toBe(70);
      expect(metrics.timestamp).toBeDefined();
      expect(metrics.brokerType).toBe("test-broker");
    });
  });

  describe("collectLagMetrics", () => {
    it("should return lag metrics", async () => {
      monitorService.setMockLagMetrics(50, 20, 15);

      const metrics = await monitorService.collectLagMetrics();

      expect(metrics.totalLag).toBe(50);
      expect(metrics.maxPartitionLag).toBe(20);
      expect(metrics.avgLag).toBe(15);
    });

    it("should handle errors and return default values", async () => {
      jest.spyOn(monitorService, "getConsumerLag").mockRejectedValue(new Error("Lag check failed"));

      const metrics = await monitorService.collectLagMetrics();

      expect(metrics.totalLag).toBe(0);
      expect(metrics.maxPartitionLag).toBe(0);
      expect(metrics.avgLag).toBe(0);
      expect(logger.logWarning).toHaveBeenCalledWith("Failed to collect consumer lag metrics", expect.any(Error));
    });
  });

  describe("collectResourceMetrics", () => {
    it("should return resource metrics", async () => {
      monitorService.setMockResourceMetrics(65, 75, 30);

      const metrics = await monitorService.collectResourceMetrics();

      expect(metrics.memoryUsage).toBe(65);
      expect(metrics.cpuUsage).toBe(75);
      expect(metrics.networkLatency).toBe(30);
    });

    it("should handle errors and return default values", async () => {
      jest.spyOn(monitorService, "getResourceMetrics").mockRejectedValue(new Error("Resource check failed"));

      const metrics = await monitorService.collectResourceMetrics();

      expect(metrics.memoryUsage).toBe(0);
      expect(metrics.cpuUsage).toBe(0);
      expect(metrics.networkLatency).toBe(0);
      expect(logger.logWarning).toHaveBeenCalledWith("Failed to collect resource metrics", expect.any(Error));
    });
  });

  describe("calculateBackpressureLevel", () => {
    it("should calculate NONE level when no backpressure", () => {
      const metrics = { totalLag: 10, cpuUsage: 20, memoryUsage: 30 };
      const level = monitorService.calculateBackpressureLevel(metrics);
      expect(level).toBe("NONE");
    });

    it("should calculate LOW level with moderate lag", () => {
      const metrics = { totalLag: 40, cpuUsage: 20, memoryUsage: 30 };
      const level = monitorService.calculateBackpressureLevel(metrics);
      expect(level).toBe("LOW");
    });

    it("should calculate MEDIUM level with higher lag", () => {
      const metrics = { totalLag: 70, cpuUsage: 20, memoryUsage: 30 };
      const level = monitorService.calculateBackpressureLevel(metrics);
      expect(level).toBe("MEDIUM");
    });

    it("should calculate HIGH level with high lag", () => {
      const metrics = { totalLag: 90, cpuUsage: 20, memoryUsage: 30 };
      const level = monitorService.calculateBackpressureLevel(metrics);
      expect(level).toBe("HIGH");
    });

    it("should calculate CRITICAL level with extreme lag", () => {
      const metrics = { totalLag: 110, cpuUsage: 20, memoryUsage: 30 };
      const level = monitorService.calculateBackpressureLevel(metrics);
      expect(level).toBe("CRITICAL");
    });

    it("should consider resource metrics when enabledResourceLag is true", () => {
      monitorService.config.enabledResourceLag = true;
      const metrics = { totalLag: 10, cpuUsage: 85, memoryUsage: 30 };

      const level = monitorService.calculateBackpressureLevel(metrics);
      expect(level).toBe("HIGH");
    });

    it("should ignore resource metrics when enabledResourceLag is false", () => {
      monitorService.config.enabledResourceLag = false;
      const metrics = { totalLag: 10, cpuUsage: 85, memoryUsage: 30 };

      const level = monitorService.calculateBackpressureLevel(metrics);
      expect(level).toBe("NONE");
    });
  });

  describe("getLagBackpressureLevel", () => {
    it("should return NONE for null metrics", () => {
      expect(monitorService.getLagBackpressureLevel(null)).toBe("NONE");
    });

    it("should return NONE for metrics without totalLag", () => {
      expect(monitorService.getLagBackpressureLevel({})).toBe("NONE");
    });

    it("should return levels based on lag ratio", () => {
      monitorService.config.lagThreshold = 100;

      expect(monitorService.getLagBackpressureLevel({ totalLag: 10 })).toBe("NONE");
      expect(monitorService.getLagBackpressureLevel({ totalLag: 40 })).toBe("LOW");
      expect(monitorService.getLagBackpressureLevel({ totalLag: 70 })).toBe("MEDIUM");
      expect(monitorService.getLagBackpressureLevel({ totalLag: 90 })).toBe("HIGH");
      expect(monitorService.getLagBackpressureLevel({ totalLag: 110 })).toBe("CRITICAL");
    });
  });

  describe("getResourceBackpressureLevel", () => {
    it("should return NONE when enabledResourceLag is false", () => {
      monitorService.config.enabledResourceLag = false;
      const metrics = { cpuUsage: 85, memoryUsage: 90 };

      const level = monitorService.getResourceBackpressureLevel(metrics);
      expect(level).toBe("NONE");
    });

    it("should return NONE for null metrics", () => {
      monitorService.config.enabledResourceLag = true;
      expect(monitorService.getResourceBackpressureLevel(null)).toBe("NONE");
    });

    it("should return levels based on maximum resource usage", () => {
      monitorService.config.enabledResourceLag = true;

      expect(monitorService.getResourceBackpressureLevel({ cpuUsage: 30, memoryUsage: 20 })).toBe("NONE");
      expect(monitorService.getResourceBackpressureLevel({ cpuUsage: 55, memoryUsage: 40 })).toBe("LOW");
      expect(monitorService.getResourceBackpressureLevel({ cpuUsage: 75, memoryUsage: 60 })).toBe("MEDIUM");
      expect(monitorService.getResourceBackpressureLevel({ cpuUsage: 85, memoryUsage: 75 })).toBe("HIGH");
      expect(monitorService.getResourceBackpressureLevel({ cpuUsage: 95, memoryUsage: 85 })).toBe("CRITICAL");
    });
  });

  describe("getHighestBackpressureLevel", () => {
    it("should return the highest level between two levels", () => {
      expect(monitorService.getHighestBackpressureLevel("NONE", "LOW")).toBe("LOW");
      expect(monitorService.getHighestBackpressureLevel("MEDIUM", "LOW")).toBe("MEDIUM");
      expect(monitorService.getHighestBackpressureLevel("HIGH", "CRITICAL")).toBe("CRITICAL");
      expect(monitorService.getHighestBackpressureLevel("CRITICAL", "NONE")).toBe("CRITICAL");
    });
  });

  describe("getBackpressureStatus", () => {
    it("should return complete status with metrics and recommendations", async () => {
      monitorService.setMockLagMetrics(50, 20, 15);
      monitorService.setMockResourceMetrics(60, 70, 10);

      // Mock the implementation to return a structure matching the actual implementation
      jest.spyOn(monitorService, "collectCurrentMetrics").mockResolvedValue({
        totalLag: 50,
        maxPartitionLag: 20,
        avgLag: 15,
        lagThreshold: 100,
        memoryUsage: 60,
        cpuUsage: 70,
        networkLatency: 10,
        timestamp: Date.now(),
        brokerType: "test-broker",
      });

      const status = await monitorService.getBackpressureStatus();

      expect(status.backpressureLevel).toBeDefined();
      expect(status.recommendedDelay).toBeDefined();
      expect(status.metrics).toBeDefined();
      expect(status.metrics.lag).toBeDefined();
      expect(status.metrics.lag.total).toBe(50);
      expect(status.metrics.resources).toBeDefined();
      expect(status.metrics.resources.cpu).toBe(70);
      expect(status.timestamp).toBeDefined();
      expect(status.brokerType).toBe("test-broker");
    });

    it("should handle errors during status collection", async () => {
      jest.spyOn(monitorService, "collectCurrentMetrics").mockRejectedValue(new Error("Status check failed"));

      const status = await monitorService.getBackpressureStatus();

      expect(status.backpressureLevel).toBe("NONE");
      expect(status.error).toBeDefined();
      expect(logger.logError).toHaveBeenCalledWith("Error getting backpressure status", expect.any(Error));
    });
  });

  describe("shouldPauseProcessing", () => {
    it("should return true when backpressure is HIGH or CRITICAL", async () => {
      // Mock the implementation to match the expected behavior
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

    it("should return false when backpressure is below HIGH", async () => {
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
    it("should return 0 when no backpressure", async () => {
      jest.spyOn(monitorService, "getBackpressureStatus").mockResolvedValue({
        backpressureLevel: "NONE",
        recommendedDelay: 0,
      });

      const delay = await monitorService.getRecommendedDelay();
      expect(delay).toBe(0);
    });

    it("should return delay proportional to backpressure level", async () => {
      jest.spyOn(monitorService, "getBackpressureStatus").mockResolvedValue({
        backpressureLevel: "HIGH",
        recommendedDelay: 200,
      });

      const delay = await monitorService.getRecommendedDelay();
      expect(delay).toBe(200);
    });
  });
});
