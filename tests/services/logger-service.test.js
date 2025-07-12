/**
 * @jest-environment node
 */

// Create all the mock functions that will be used
const mockTimestamp = jest.fn().mockReturnThis();
const mockColorize = jest.fn().mockReturnThis();
const mockPrintf = jest.fn().mockReturnThis();
const mockJson = jest.fn().mockReturnThis();
const mockErrors = jest.fn().mockReturnThis();
const mockCombine = jest.fn().mockReturnThis();

const mockInfo = jest.fn();
const mockDebug = jest.fn();
const mockWarn = jest.fn();
const mockError = jest.fn();
const mockIsDebugEnabled = jest.fn().mockReturnValue(true);
const mockIsLevelEnabled = jest.fn().mockImplementation(level => level === "debug");

const mockConsoleTransport = jest.fn();

// Mock Winston before requiring any modules
jest.mock("winston", () => ({
  format: {
    timestamp: mockTimestamp,
    colorize: mockColorize,
    printf: mockPrintf,
    json: mockJson,
    combine: mockCombine,
    errors: mockErrors,
  },
  createLogger: jest.fn().mockReturnValue({
    info: mockInfo,
    debug: mockDebug,
    warn: mockWarn,
    error: mockError,
    isDebugEnabled: mockIsDebugEnabled,
    isLevelEnabled: mockIsLevelEnabled,
  }),
  transports: {
    Console: mockConsoleTransport,
  },
}));

// Now require modules that use winston
const winston = require("winston");
const {
  LoggerService,
  logInfo,
  logDebug,
  logWarning,
  logError,
  logBatchOperation,
  logTaskOperation,
  logConnectionEvent,
  logConcurrencyStatus,
  logProcessingStatistics,
} = require("../../src/services/logger-service");

describe("LoggerService", () => {
  let loggerService;

  beforeEach(() => {
    jest.clearAllMocks();
    loggerService = new LoggerService();
  });

  describe("Constructor", () => {
    it("should create a logger instance with winston", () => {
      expect(winston.createLogger).toHaveBeenCalled();
      expect(loggerService.logger).toBeDefined();
    });

    it("should use default log level when environment variable is not set", () => {
      const originalEnv = process.env;
      process.env = { ...originalEnv };
      delete process.env.LOG_LEVEL;

      loggerService = new LoggerService();

      expect(loggerService._getLogLevel()).toBe("info");
      process.env = originalEnv;
    });

    it("should use environment variable for log level when set", () => {
      const originalEnv = process.env;
      process.env = { ...originalEnv, LOG_LEVEL: "debug" };

      loggerService = new LoggerService();

      expect(loggerService._getLogLevel()).toBe("debug");
      process.env = originalEnv;
    });

    it("should use default log format when environment variable is not set", () => {
      const originalEnv = process.env;
      process.env = { ...originalEnv };
      delete process.env.LOG_FORMAT;

      loggerService = new LoggerService();

      expect(loggerService._getLogFormat()).toBe("simple");
      process.env = originalEnv;
    });

    it("should use environment variable for log format when set", () => {
      const originalEnv = process.env;
      process.env = { ...originalEnv, LOG_FORMAT: "json" };

      loggerService = new LoggerService();

      expect(loggerService._getLogFormat()).toBe("json");
      process.env = originalEnv;
    });
  });

  describe("Format Creation", () => {
    it("should create simple format", () => {
      loggerService._createFormat("simple");

      expect(mockTimestamp).toHaveBeenCalledWith({ format: "YYYY-MM-DD HH:mm:ss" });
      expect(mockColorize).toHaveBeenCalled();
      expect(mockPrintf).toHaveBeenCalled();
      expect(mockCombine).toHaveBeenCalled();
    });

    it("should create json format", () => {
      loggerService._createFormat("json");

      expect(mockTimestamp).toHaveBeenCalledWith({ format: "YYYY-MM-DD HH:mm:ss" });
      expect(mockJson).toHaveBeenCalled();
      expect(mockCombine).toHaveBeenCalled();
    });

    it("should create detailed format", () => {
      loggerService._createFormat("detailed");

      expect(mockTimestamp).toHaveBeenCalledWith({ format: "YYYY-MM-DD HH:mm:ss" });
      expect(mockColorize).toHaveBeenCalled();
      expect(mockErrors).toHaveBeenCalledWith({ stack: true });
      expect(mockPrintf).toHaveBeenCalled();
      expect(mockCombine).toHaveBeenCalled();
    });

    it("should default to simple format for unknown format type", () => {
      loggerService._createFormat("unknown");

      expect(mockTimestamp).toHaveBeenCalledWith({ format: "YYYY-MM-DD HH:mm:ss" });
      expect(mockColorize).toHaveBeenCalled();
      expect(mockPrintf).toHaveBeenCalled();
      expect(mockCombine).toHaveBeenCalled();
    });
  });

  describe("Transport Creation", () => {
    it("should create console transport with exception handling", () => {
      const transports = loggerService._createTransports();

      expect(mockConsoleTransport).toHaveBeenCalledWith({
        handleExceptions: true,
        handleRejections: true,
      });
      expect(transports).toHaveLength(1);
    });
  });

  describe("Logging Methods", () => {
    it("should log info messages", () => {
      const message = "Test info message";
      const metadata = { test: "data" };

      loggerService.logInfo(message, metadata);

      expect(mockInfo).toHaveBeenCalled();
    });

    it("should log debug messages", () => {
      const message = "Test debug message";
      const metadata = { test: "data" };

      loggerService.logDebug(message, metadata);

      expect(mockDebug).toHaveBeenCalled();
    });

    it("should log warning messages", () => {
      const message = "Test warning message";
      const metadata = { test: "data" };

      loggerService.logWarning(message, metadata);

      expect(mockWarn).toHaveBeenCalled();
    });

    it("should log error messages with error object", () => {
      const message = "Test error message";
      const error = new Error("Test error");
      const metadata = { test: "data" };

      loggerService.logError(message, error, metadata);

      expect(mockError).toHaveBeenCalled();
    });

    it("should log error messages without error object", () => {
      const message = "Test error message";
      const metadata = { test: "data" };

      loggerService.logError(message, null, metadata);

      expect(mockError).toHaveBeenCalled();
    });
  });

  describe("Specialized Logging Methods", () => {
    it("should log batch operation", () => {
      const operation = "process";
      const batchId = "batch123";
      const count = { count: 5 };

      loggerService.logBatchOperation(batchId, operation, count);

      expect(mockInfo).toHaveBeenCalled();
    });

    it("should log task operation", () => {
      const operation = "complete";
      const taskId = "task123";
      const duration = { duration: 100 };

      loggerService.logTaskOperation(taskId, operation, duration);

      expect(mockInfo).toHaveBeenCalled();
    });

    it("should log connection event", () => {
      const event = "connect";
      const service = "kafka";

      loggerService.logConnectionEvent(service, event);

      expect(mockInfo).toHaveBeenCalled();
    });

    it("should log concurrency status", () => {
      const active = 3;
      const max = 5;
      const queued = 2;

      loggerService.logConcurrencyStatus(active, max, queued);

      expect(mockInfo).toHaveBeenCalled();
    });

    it("should log processing statistics", () => {
      const stats = {
        total: 100,
        success: 90,
        failed: 10,
        duration: 1000,
      };

      loggerService.logProcessingStatistics(stats);

      expect(mockInfo).toHaveBeenCalled();
    });
  });

  describe("Utility Methods", () => {
    it("should check if debug is enabled", () => {
      expect(loggerService.isDebugEnabled()).toBe(true);
      expect(mockIsDebugEnabled).toHaveBeenCalled();
    });

    it("should check if specific log level is enabled", () => {
      expect(loggerService.isLevelEnabled("debug")).toBe(true);
      expect(mockIsLevelEnabled).toHaveBeenCalledWith("debug");
    });
  });

  describe("Exported Functions", () => {
    it("should export working wrapper functions", () => {
      // These are exported functions from the singleton instance
      // We can't test them directly with mocks, so we just verify they don't throw errors
      logInfo("test info");
      logDebug("test debug");
      logWarning("test warning");
      logError("test error");
      logBatchOperation("batch1", "test", 10);
      logTaskOperation("task1", "test", 100);
      logConnectionEvent("kafka", "test");
      logConcurrencyStatus(1, 10, 5);
      logProcessingStatistics({ total: 10 });
    });
  });
});
