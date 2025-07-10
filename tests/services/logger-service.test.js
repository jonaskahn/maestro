/**
 * @jest-environment node
 */

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

// Mock winston
jest.mock("winston", () => {
  const mockFormat = {
    timestamp: jest.fn().mockReturnValue("timestamp-format"),
    colorize: jest.fn().mockReturnValue("colorize-format"),
    printf: jest.fn().mockReturnValue("printf-format"),
    json: jest.fn().mockReturnValue("json-format"),
    combine: jest.fn().mockReturnValue("combined-format"),
    errors: jest.fn().mockReturnValue("errors-format"),
  };

  const mockLogger = {
    info: jest.fn(),
    debug: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    isDebugEnabled: jest.fn().mockReturnValue(true),
    isLevelEnabled: jest.fn().mockImplementation(level => level === "debug"),
  };

  return {
    format: mockFormat,
    createLogger: jest.fn().mockReturnValue(mockLogger),
    transports: {
      Console: jest.fn().mockImplementation(() => ({})),
    },
  };
});

describe("LoggerService", () => {
  let loggerService;
  let mockLogger;

  beforeEach(() => {
    jest.clearAllMocks();
    loggerService = new LoggerService();
    mockLogger = winston.createLogger();
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

      expect(winston.format.timestamp).toHaveBeenCalledWith({ format: "YYYY-MM-DD HH:mm:ss" });
      expect(winston.format.colorize).toHaveBeenCalled();
      expect(winston.format.printf).toHaveBeenCalled();
      expect(winston.format.combine).toHaveBeenCalledWith("timestamp-format", "colorize-format", "printf-format");
    });

    it("should create json format", () => {
      loggerService._createFormat("json");

      expect(winston.format.timestamp).toHaveBeenCalledWith({ format: "YYYY-MM-DD HH:mm:ss" });
      expect(winston.format.json).toHaveBeenCalled();
      expect(winston.format.combine).toHaveBeenCalledWith("timestamp-format", "json-format");
    });

    it("should create detailed format", () => {
      loggerService._createFormat("detailed");

      expect(winston.format.timestamp).toHaveBeenCalledWith({ format: "YYYY-MM-DD HH:mm:ss" });
      expect(winston.format.colorize).toHaveBeenCalled();
      expect(winston.format.errors).toHaveBeenCalledWith({ stack: true });
      expect(winston.format.printf).toHaveBeenCalled();
      expect(winston.format.combine).toHaveBeenCalledWith(
        "timestamp-format",
        "colorize-format",
        "errors-format",
        "printf-format"
      );
    });

    it("should default to simple format for unknown format type", () => {
      loggerService._createFormat("unknown");

      expect(winston.format.combine).toHaveBeenCalledWith("timestamp-format", "colorize-format", "printf-format");
    });
  });

  describe("Transport Creation", () => {
    it("should create console transport with exception handling", () => {
      const transports = loggerService._createTransports();

      expect(winston.transports.Console).toHaveBeenCalledWith({
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

      expect(mockLogger.info).toHaveBeenCalledWith(message, metadata);
    });

    it("should log debug messages", () => {
      const message = "Test debug message";
      const metadata = { test: "data" };

      loggerService.logDebug(message, metadata);

      expect(mockLogger.debug).toHaveBeenCalledWith(message, metadata);
    });

    it("should log warning messages", () => {
      const message = "Test warning message";
      const metadata = { test: "data" };

      loggerService.logWarning(message, metadata);

      expect(mockLogger.warn).toHaveBeenCalledWith(message, metadata);
    });

    it("should log error messages with error object", () => {
      const message = "Test error message";
      const error = new Error("Test error");
      const metadata = { test: "data" };

      loggerService.logError(message, error, metadata);

      expect(mockLogger.error).toHaveBeenCalledWith(message, {
        error: error.message,
        stack: error.stack,
        ...metadata,
      });
    });

    it("should log error messages without error object", () => {
      const message = "Test error message";
      const metadata = { test: "data" };

      loggerService.logError(message, null, metadata);

      expect(mockLogger.error).toHaveBeenCalledWith(message, metadata);
    });

    it("should log batch operations", () => {
      const batchId = "batch-123";
      const operation = "processing";
      const details = { items: 10 };

      loggerService.logBatchOperation(batchId, operation, details);

      expect(mockLogger.info).toHaveBeenCalledWith(`[Batch ${batchId}] ${operation}`, details);
    });

    it("should log task operations", () => {
      const taskId = "task-456";
      const operation = "completed";
      const details = { duration: 500 };

      loggerService.logTaskOperation(taskId, operation, details);

      expect(mockLogger.info).toHaveBeenCalledWith(`[${taskId}] ${operation}`, details);
    });

    it("should log connection events", () => {
      const service = "Kafka";
      const event = "connected";
      const details = { host: "localhost:9092" };

      loggerService.logConnectionEvent(service, event, details);

      expect(mockLogger.info).toHaveBeenCalledWith(`${service} ${event}`, details);
    });

    it("should log concurrency status", () => {
      const active = 5;
      const max = 10;
      const queued = 3;
      const additionalInfo = { avgProcessingTime: 200 };

      loggerService.logConcurrencyStatus(active, max, queued, additionalInfo);

      expect(mockLogger.info).toHaveBeenCalledWith(
        `Concurrency Status: ${active}/${max} active, ${queued} queued`,
        additionalInfo
      );
    });

    it("should log processing statistics", () => {
      const statistics = {
        processed: 100,
        failed: 2,
        avgTime: 150,
      };

      loggerService.logProcessingStatistics(statistics);

      expect(mockLogger.info).toHaveBeenCalledWith("Processing Statistics", statistics);
    });
  });

  describe("Helper Methods", () => {
    it("should check if debug is enabled", () => {
      const result = loggerService.isDebugEnabled();

      expect(mockLogger.isDebugEnabled).toHaveBeenCalled();
      expect(result).toBe(true);
    });

    it("should check if specific level is enabled", () => {
      const debugResult = loggerService.isLevelEnabled("debug");
      const errorResult = loggerService.isLevelEnabled("error");

      expect(mockLogger.isLevelEnabled).toHaveBeenCalledWith("debug");
      expect(mockLogger.isLevelEnabled).toHaveBeenCalledWith("error");
      expect(debugResult).toBe(true);
      expect(errorResult).toBe(false);
    });
  });

  describe("Module Exports", () => {
    it("should export bound logger methods", () => {
      const testMessage = "Test message";

      logInfo(testMessage);
      logDebug(testMessage);
      logWarning(testMessage);
      logError(testMessage);
      logBatchOperation("batch-1", "test");
      logTaskOperation("task-1", "test");
      logConnectionEvent("Redis", "connected");
      logConcurrencyStatus(1, 5, 2);
      logProcessingStatistics({ count: 10 });

      expect(mockLogger.info).toHaveBeenCalledWith(testMessage, {});
      expect(mockLogger.debug).toHaveBeenCalledWith(testMessage, {});
      expect(mockLogger.warn).toHaveBeenCalledWith(testMessage, {});
      expect(mockLogger.error).toHaveBeenCalledWith(testMessage, {});
    });
  });
});
