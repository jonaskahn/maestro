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
    test("Given The LoggerService class is available When Creating a new LoggerService instance Then Winston createLogger should be called and logger should be defined", () => {
      expect(winston.createLogger).toHaveBeenCalled();
      expect(loggerService.logger).toBeDefined();
    });

    test("should use default log level when environment variable is not set", () => {
      // Given: No LOG_LEVEL environment variable is set
      const originalEnv = process.env;
      process.env = { ...originalEnv };
      delete process.env.LOG_LEVEL;

      // When: Creating a new LoggerService instance
      loggerService = new LoggerService();

      // Then: The log level should be the default value "info"
      expect(loggerService._getLogLevel()).toBe("info");
      process.env = originalEnv;
    });

    test("should use environment variable for log level when set", () => {
      // Given: LOG_LEVEL environment variable is set to "debug"
      const originalEnv = process.env;
      process.env = { ...originalEnv, LOG_LEVEL: "debug" };

      // When: Creating a new LoggerService instance
      loggerService = new LoggerService();

      // Then: The log level should match the environment variable value
      expect(loggerService._getLogLevel()).toBe("debug");
      process.env = originalEnv;
    });

    test("should use default log format when environment variable is not set", () => {
      // Given: No LOG_FORMAT environment variable is set
      const originalEnv = process.env;
      process.env = { ...originalEnv };
      delete process.env.LOG_FORMAT;

      // When: Creating a new LoggerService instance
      loggerService = new LoggerService();

      // Then: The log format should be the default value "simple"
      expect(loggerService._getLogFormat()).toBe("simple");
      process.env = originalEnv;
    });

    test("should use environment variable for log format when set", () => {
      // Given: LOG_FORMAT environment variable is set to "json"
      const originalEnv = process.env;
      process.env = { ...originalEnv, LOG_FORMAT: "json" };

      // When: Creating a new LoggerService instance
      loggerService = new LoggerService();

      // Then: The log format should match the environment variable value
      expect(loggerService._getLogFormat()).toBe("json");
      process.env = originalEnv;
    });
  });

  describe("Format Creation", () => {
    test("should create simple format", () => {
      // Given: A LoggerService instance
      // When: Creating a simple format
      loggerService._createFormat("simple");

      // Then: The appropriate Winston format methods should be called
      expect(mockTimestamp).toHaveBeenCalledWith({ format: "YYYY-MM-DD HH:mm:ss" });
      expect(mockColorize).toHaveBeenCalled();
      expect(mockPrintf).toHaveBeenCalled();
      expect(mockCombine).toHaveBeenCalled();
    });

    test("should create json format", () => {
      // Given: A LoggerService instance
      // When: Creating a json format
      loggerService._createFormat("json");

      // Then: The appropriate Winston format methods should be called
      expect(mockTimestamp).toHaveBeenCalledWith({ format: "YYYY-MM-DD HH:mm:ss" });
      expect(mockJson).toHaveBeenCalled();
      expect(mockCombine).toHaveBeenCalled();
    });

    test("should create detailed format", () => {
      // Given: A LoggerService instance
      // When: Creating a detailed format
      loggerService._createFormat("detailed");

      // Then: The appropriate Winston format methods should be called
      expect(mockTimestamp).toHaveBeenCalledWith({ format: "YYYY-MM-DD HH:mm:ss" });
      expect(mockColorize).toHaveBeenCalled();
      expect(mockErrors).toHaveBeenCalledWith({ stack: true });
      expect(mockPrintf).toHaveBeenCalled();
      expect(mockCombine).toHaveBeenCalled();
    });

    test("should default to simple format for unknown format type", () => {
      // Given: A LoggerService instance
      // When: Creating an unknown format
      loggerService._createFormat("unknown");

      // Then: The simple format methods should be called
      expect(mockTimestamp).toHaveBeenCalledWith({ format: "YYYY-MM-DD HH:mm:ss" });
      expect(mockColorize).toHaveBeenCalled();
      expect(mockPrintf).toHaveBeenCalled();
      expect(mockCombine).toHaveBeenCalled();
    });
  });

  describe("Transport Creation", () => {
    test("should create console transport with exception handling", () => {
      // Given: A LoggerService instance
      // When: Creating transports
      const transports = loggerService._createTransports();

      // Then: Console transport should be created with exception handling
      expect(mockConsoleTransport).toHaveBeenCalledWith({
        handleExceptions: true,
        handleRejections: true,
      });
      expect(transports).toHaveLength(1);
    });
  });

  describe("Logging Methods", () => {
    test("should log info messages", () => {
      // Given: A message and metadata
      const message = "Test info message";
      const metadata = { test: "data" };

      // When: Logging an info message
      loggerService.logInfo(message, metadata);

      // Then: The info method should be called
      expect(mockInfo).toHaveBeenCalled();
    });

    test("should log debug messages", () => {
      // Given: A message and metadata
      const message = "Test debug message";
      const metadata = { test: "data" };

      // When: Logging a debug message
      loggerService.logDebug(message, metadata);

      // Then: The debug method should be called
      expect(mockDebug).toHaveBeenCalled();
    });

    test("should log warning messages", () => {
      // Given: A message and metadata
      const message = "Test warning message";
      const metadata = { test: "data" };

      // When: Logging a warning message
      loggerService.logWarning(message, metadata);

      // Then: The warn method should be called
      expect(mockWarn).toHaveBeenCalled();
    });

    test("should log error messages with error object", () => {
      // Given: A message, error object, and metadata
      const message = "Test error message";
      const error = new Error("Test error");
      const metadata = { test: "data" };

      // When: Logging an error message with an error object
      loggerService.logError(message, error, metadata);

      // Then: The error method should be called
      expect(mockError).toHaveBeenCalled();
    });

    test("should log error messages without error object", () => {
      // Given: A message and metadata but no error object
      const message = "Test error message";
      const metadata = { test: "data" };

      // When: Logging an error message without an error object
      loggerService.logError(message, null, metadata);

      // Then: The error method should be called
      expect(mockError).toHaveBeenCalled();
    });
  });

  describe("Specialized Logging Methods", () => {
    test("should log batch operation", () => {
      // Given: Batch operation details
      const operation = "process";
      const batchId = "batch123";
      const count = { count: 5 };

      // When: Logging a batch operation
      loggerService.logBatchOperation(batchId, operation, count);

      // Then: The info method should be called
      expect(mockInfo).toHaveBeenCalled();
    });

    test("should log task operation", () => {
      // Given: Task operation details
      const operation = "complete";
      const taskId = "task123";
      const duration = { duration: 100 };

      // When: Logging a task operation
      loggerService.logTaskOperation(taskId, operation, duration);

      // Then: The info method should be called
      expect(mockInfo).toHaveBeenCalled();
    });

    test("should log connection event", () => {
      // Given: Connection event details
      const event = "connect";
      const service = "kafka";

      // When: Logging a connection event
      loggerService.logConnectionEvent(service, event);

      // Then: The debug method should be called
      expect(mockDebug).toHaveBeenCalled();
    });

    test("should log concurrency status", () => {
      // Given: Concurrency status details
      const active = 3;
      const max = 5;
      const queued = 2;

      // When: Logging concurrency status
      loggerService.logConcurrencyStatus(active, max, queued);

      // Then: The info method should be called
      expect(mockInfo).toHaveBeenCalled();
    });

    test("should log processing statistics", () => {
      // Given: Processing statistics
      const stats = {
        processed: 100,
        failed: 5,
        duration: 1000,
      };

      // When: Logging processing statistics
      loggerService.logProcessingStatistics(stats);

      // Then: The info method should be called
      expect(mockInfo).toHaveBeenCalled();
    });
  });

  describe("Utility Methods", () => {
    test("should check if debug is enabled", () => {
      // Given: A LoggerService instance
      // When: Checking if debug is enabled
      const result = loggerService.isDebugEnabled();

      // Then: It should return the mock value and call the mock function
      expect(result).toBe(true);
      expect(mockIsDebugEnabled).toHaveBeenCalled();
    });

    test("should check if specific log level is enabled", () => {
      // Given: A LoggerService instance
      // When: Checking if a specific log level is enabled
      const result = loggerService.isLevelEnabled("debug");

      // Then: It should return the mock value and call the mock function
      expect(result).toBe(true);
      expect(mockIsLevelEnabled).toHaveBeenCalledWith("debug");
    });
  });

  describe("Exported Functions", () => {
    test("Given The exported wrapper functions When Calling the wrapper functions Then They should not throw errors", () => {
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
