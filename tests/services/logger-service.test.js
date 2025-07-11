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
  });

  describe("Specialized Logging Methods", () => {
    it("should log batch operation", () => {
      const operation = "start";
      const batchId = "batch123";
      const count = 5;
      const logSpy = jest.spyOn(loggerService, "logInfo");

      loggerService.logBatchOperation(operation, batchId, count);

      expect(logSpy).toHaveBeenCalledWith(`[Batch ${operation}] ${batchId}`, count);
    });

    it("should log task operation", () => {
      const operation = "complete";
      const taskId = "task123";
      const duration = 100;
      const logSpy = jest.spyOn(loggerService, "logInfo");

      loggerService.logTaskOperation(operation, taskId, duration);

      expect(logSpy).toHaveBeenCalledWith(`[${operation}] ${taskId}`, duration);
    });

    it("should log connection event", () => {
      const event = "connect";
      const service = "kafka";
      const logSpy = jest.spyOn(loggerService, "logInfo");

      loggerService.logConnectionEvent(event, service);

      expect(logSpy).toHaveBeenCalledWith(`${event} ${service}`, {});
    });

    it("should log concurrency status", () => {
      const active = 3;
      const max = 5;
      const logSpy = jest.spyOn(loggerService, "logInfo");

      loggerService.logConcurrencyStatus(active, max);

      expect(logSpy).toHaveBeenCalledWith(`Concurrency Status: ${active}/${max} active, undefined queued`, {});
    });

    it("should log processing statistics", () => {
      const stats = {
        total: 100,
        success: 90,
        failed: 10,
        duration: 1000,
      };
      const logSpy = jest.spyOn(loggerService, "logInfo");

      loggerService.logProcessingStatistics(stats);

      expect(logSpy).toHaveBeenCalledWith("Processing Statistics", stats);
    });
  });

  describe("Exported Functions", () => {
    it("should export working wrapper functions", () => {
      logInfo("test info");
      logDebug("test debug");
      logWarning("test warning");
      logError("test error");
      logBatchOperation("test", "batch1", 10);
      logTaskOperation("test", "task1", 100);
      logConnectionEvent("test", "kafka");
      logConcurrencyStatus(1, 10);
      logProcessingStatistics({ total: 10 });

      expect(mockLogger.info).toHaveBeenCalledWith("test info", {});
      expect(mockLogger.debug).toHaveBeenCalledWith("test debug", {});
      expect(mockLogger.warn).toHaveBeenCalledWith("test warning", {});
      expect(mockLogger.error).toHaveBeenCalledWith("test error", {});
    });
  });
});
