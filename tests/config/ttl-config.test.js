/**
 * @jest-environment node
 */

const mockLogWarning = jest.fn();
const mockLogInfo = jest.fn();
const mockLogDebug = jest.fn();
jest.mock("../../src/services/logger-service", () => ({
  logWarning: mockLogWarning,
  logInfo: mockLogInfo,
  logDebug: mockLogDebug,
}));
// Store the original env variables
let originalEnv;

describe("TtlConfig", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    // Store original env and reset for tests
    originalEnv = { ...process.env };
  });

  afterEach(() => {
    // Restore original env variables
    process.env = originalEnv;
  });

  describe("getPrimaryTTL", () => {
    test("should return default value when no env variable is set", () => {
      // Arrange
      const TtlConfig = require("../../src/config/ttl-config");

      // Act
      const delayBaseTimeout = TtlConfig.getPrimaryTTL("DELAY_BASE_TIMEOUT_MS");

      // Assert
      expect(delayBaseTimeout).toBe(0);
    });

    test("should return value from environment variable when set", () => {
      // Arrange
      process.env.MO_DELAY_BASE_TIMEOUT_MS = "5000";
      const TtlConfig = require("../../src/config/ttl-config");

      // Act
      const delayBaseTimeout = TtlConfig.getPrimaryTTL("DELAY_BASE_TIMEOUT_MS");

      // Assert
      expect(delayBaseTimeout).toBe(5000);
    });

    test("should use default when env variable is not a valid number", () => {
      // Arrange
      process.env.MO_DELAY_BASE_TIMEOUT_MS = "invalid";
      const TtlConfig = require("../../src/config/ttl-config");

      // Act
      const delayBaseTimeout = TtlConfig.getPrimaryTTL("DELAY_BASE_TIMEOUT_MS");

      // Assert
      expect(delayBaseTimeout).toBe(1000);
      expect(mockLogWarning).toHaveBeenCalledWith(expect.stringContaining("Invalid environment value"));
    });

    test("should throw error for unknown primary TTL key", () => {
      // Arrange
      const TtlConfig = require("../../src/config/ttl-config");

      // Act & Assert
      expect(() => TtlConfig.getPrimaryTTL("UNKNOWN_KEY")).toThrow("Unknown primary TTL key");
    });
  });

  describe("getDerivedTTL", () => {
    test("should calculate derived value when no env variable is set", () => {
      // Arrange
      const TtlConfig = require("../../src/config/ttl-config");
      const derivationFn = jest.fn().mockReturnValue(15000);

      // Act
      const derivedValue = TtlConfig.getDerivedTTL("DISTRIBUTED_LOCK_TTL", derivationFn);

      // Assert
      expect(derivationFn).toHaveBeenCalled();
      expect(derivedValue).toBe(15000);
    });

    test("should use environment variable value when available", () => {
      // Arrange
      process.env.MO_DISTRIBUTED_LOCK_TTL_MS = "20000";
      const TtlConfig = require("../../src/config/ttl-config");
      const derivationFn = jest.fn().mockReturnValue(15000);

      // Act
      const derivedValue = TtlConfig.getDerivedTTL("DISTRIBUTED_LOCK_TTL", derivationFn);

      // Assert
      expect(derivationFn).not.toHaveBeenCalled();
      expect(derivedValue).toBe(20000);
    });

    test("should use calculated value when env variable is invalid", () => {
      // Arrange
      process.env.MO_DISTRIBUTED_LOCK_TTL_MS = "invalid";
      const TtlConfig = require("../../src/config/ttl-config");
      const derivationFn = jest.fn().mockReturnValue(15000);

      // Act
      const derivedValue = TtlConfig.getDerivedTTL("DISTRIBUTED_LOCK_TTL", derivationFn);

      // Assert
      expect(derivationFn).toHaveBeenCalled();
      expect(derivedValue).toBe(15000);
      expect(mockLogWarning).toHaveBeenCalledWith(expect.stringContaining("Invalid environment value"));
    });
  });

  describe("getAllTtlValues", () => {
    test("should return both primary and derived TTL values", () => {
      // Arrange
      const TtlConfig = require("../../src/config/ttl-config");

      // Act
      const allValues = TtlConfig.getAllTtlValues();

      // Assert
      expect(allValues.DELAY_BASE_TIMEOUT_MS).toBeDefined();
      expect(allValues.TASK_PROCESSING_BASE_TTL).toBeDefined();
      expect(allValues.DISTRIBUTED_LOCK_TTL).toBeDefined();
      expect(allValues.DISTRIBUTED_LOCK_REFRESH_INTERVAL).toBeDefined();
      expect(allValues.KAFKA_CONNECTION_TIMEOUT).toBeDefined();
    });

    test("should accept overrides for specific values", () => {
      // Arrange
      const TtlConfig = require("../../src/config/ttl-config");
      const overrides = {
        DELAY_BASE_TIMEOUT_MS: 10000,
        DISTRIBUTED_LOCK_TTL: 60000,
      };

      // Act
      const allValues = TtlConfig.getAllTtlValues(overrides);

      // Assert
      expect(allValues.DELAY_BASE_TIMEOUT_MS).toBe(10000);
      expect(allValues.DISTRIBUTED_LOCK_TTL).toBe(60000);
    });

    test("should validate TTL relationships", () => {
      // Arrange
      const TtlConfig = require("../../src/config/ttl-config");
      jest.spyOn(TtlConfig, "validateTTLRelationships");

      // Act
      TtlConfig.getAllTtlValues();

      // Assert
      expect(TtlConfig.validateTTLRelationships).toHaveBeenCalled();
    });
  });

  describe("getSpecificConfigs", () => {
    test("should return lock configuration", () => {
      // Arrange
      const TtlConfig = require("../../src/config/ttl-config");

      // Act
      const lockConfig = TtlConfig.getLockConfig();

      // Assert
      expect(lockConfig.ttlMs).toBeDefined();
      expect(lockConfig.maxWaitTimeMs).toBeDefined();
      expect(lockConfig.retryDelayMs).toBeDefined();
      expect(lockConfig.refreshInterval).toBeDefined();
    });

    test("should return topic configuration", () => {
      // Arrange
      const TtlConfig = require("../../src/config/ttl-config");

      // Act
      const topicConfig = TtlConfig.getTopicConfig();

      // Assert
      expect(topicConfig.processingTtl).toBeDefined();
      expect(topicConfig.suppressionTtl).toBeDefined();
    });

    test("should return backpressure configuration", () => {
      // Arrange
      const TtlConfig = require("../../src/config/ttl-config");

      // Act
      const backpressureConfig = TtlConfig.getBackpressureConfig();

      // Assert
      expect(backpressureConfig.checkInterval).toBeDefined();
      expect(backpressureConfig.cacheTTL).toBeDefined();
      expect(backpressureConfig.backoffMinDelay).toBeDefined();
      expect(backpressureConfig.backoffMaxDelay).toBeDefined();
    });

    test("should return kafka configuration", () => {
      // Arrange
      const TtlConfig = require("../../src/config/ttl-config");

      // Act
      const kafkaConfig = TtlConfig.getKafkaConfig();

      // Assert
      expect(kafkaConfig.connectionTimeout).toBeDefined();
      expect(kafkaConfig.requestTimeout).toBeDefined();
    });
  });

  describe("_getEnvValue", () => {
    test("should return env value when available", () => {
      // Arrange
      process.env.TEST_ENV_VALUE = "test-value";
      const TtlConfig = require("../../src/config/ttl-config");

      // Act
      const value = TtlConfig._getEnvValue(["TEST_ENV_VALUE", "FALLBACK_VALUE"]);

      // Assert
      expect(value).toBe("test-value");
    });

    test("should return first found env value", () => {
      // Arrange
      process.env.SECOND_ENV_VALUE = "second-value";
      const TtlConfig = require("../../src/config/ttl-config");

      // Act
      const value = TtlConfig._getEnvValue(["FIRST_ENV_VALUE", "SECOND_ENV_VALUE"]);

      // Assert
      expect(value).toBe("second-value");
    });

    test("should return null when no env value is found", () => {
      // Arrange
      const TtlConfig = require("../../src/config/ttl-config");

      // Act
      const value = TtlConfig._getEnvValue(["NONEXISTENT_VALUE"]);

      // Assert
      expect(value).toBeNull();
    });
  });
});
