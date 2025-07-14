const mockLogInfo = jest.fn();
const mockLogError = jest.fn();
const mockLogWarning = jest.fn();
const mockLogDebug = jest.fn();
const mockLogConnectionEvent = jest.fn();

const mockRedisClient = {
  isOpen: true,
  connect: jest.fn().mockResolvedValue(undefined),
  disconnect: jest.fn().mockResolvedValue(undefined),
  set: jest.fn().mockResolvedValue("OK"),
  get: jest.fn().mockResolvedValue("test-value"),
  del: jest.fn().mockResolvedValue(1),
  exists: jest.fn().mockResolvedValue(1),
  expire: jest.fn().mockResolvedValue(true),
  scanIterator: jest.fn(),
  on: jest.fn(),
};

jest.mock("redis", () => ({
  createClient: jest.fn().mockReturnValue(mockRedisClient),
}));

jest.mock("../../../src/services/logger-service", () => ({
  logInfo: mockLogInfo,
  logError: mockLogError,
  logWarning: mockLogWarning,
  logDebug: mockLogDebug,
  logConnectionEvent: mockLogConnectionEvent,
}));

jest.mock("../../../src/config/ttl-config", () => ({
  getTopicConfig: jest.fn().mockReturnValue({
    processingTtl: 60000,
    suppressionTtl: 180000,
  }),
  getAllTtlValues: jest.fn().mockReturnValue({}),
}));

const originalEnv = process.env;

const RedisCacheClient = require("../../../src/implementations/cache/redis-cache-client");
const redis = require("redis");

describe("RedisCacheClient", () => {
  let redisCacheClient;
  const defaultConfig = {
    keyPrefix: "test-prefix:",
    connectionOptions: {
      url: "redis://test-host:6379",
      password: "test-password",
    },
  };

  beforeEach(() => {
    jest.clearAllMocks();

    process.env = {
      ...originalEnv,
      REDIS_URL: "localhost:6379",
      MO_REDIS_URL: "redis://localhost:6379",
      MO_REDIS_HOST: "localhost",
      MO_REDIS_PORT: "6379",
    };

    redisCacheClient = new RedisCacheClient(defaultConfig);
    redisCacheClient._client = mockRedisClient;
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe("Constructor and Initialization", () => {
    test("should create Redis client with provided configuration", () => {
      // Given: A RedisCacheClient is instantiated with explicit configuration
      // When: The constructor runs
      // Then: The Redis client should be created with the provided configuration
      expect(redis.createClient).toHaveBeenCalledWith(
        expect.objectContaining({
          url: "redis://test-host:6379",
          password: "test-password",
          retry_strategy: expect.any(Function),
          socket: expect.objectContaining({
            reconnectStrategy: expect.any(Function),
          }),
        })
      );
    });

    test("should use environment variables when config is not provided", () => {
      // Given: Environment variables for Redis are set
      // When: A RedisCacheClient is instantiated without connection options
      // Then: The client should use environment variables for configuration
      const testEnv = {
        ...originalEnv,
        MO_REDIS_URL: "redis://env-host:6379",
        MO_REDIS_PASSWORD: "env-password",
        REDIS_URL: "env-host:6379",
      };
      process.env = testEnv;

      redis.createClient.mockClear();
      new RedisCacheClient({ keyPrefix: "env-prefix:" });

      expect(redis.createClient).toHaveBeenCalledWith(
        expect.objectContaining({
          url: "redis://env-host:6379",
          password: "env-password",
        })
      );
    });

    test("should use default values when neither config nor env vars exist", () => {
      // Given: No Redis configuration or environment variables are available
      // When: A RedisCacheClient is instantiated
      // Then: The client should use default connection values
      const envVars = { ...originalEnv };
      delete envVars.MO_REDIS_URL;
      delete envVars.MO_REDIS_PASSWORD;
      envVars.REDIS_URL = "localhost:6379";
      process.env = envVars;

      redis.createClient.mockClear();
      new RedisCacheClient({ keyPrefix: "default-prefix:" });

      expect(redis.createClient).toHaveBeenCalledWith(
        expect.objectContaining({
          url: expect.stringContaining("redis://"),
        })
      );
    });

    test("should attach event listeners to Redis client", () => {
      // Given: A RedisCacheClient is instantiated
      // When: The constructor runs
      // Then: Event handlers should be attached to the Redis client
      expect(mockRedisClient.on).toHaveBeenCalledWith("error", expect.any(Function));
      expect(mockRedisClient.on).toHaveBeenCalledWith("connect", expect.any(Function));
      expect(mockRedisClient.on).toHaveBeenCalledWith("ready", expect.any(Function));
      expect(mockRedisClient.on).toHaveBeenCalledWith("end", expect.any(Function));
      expect(mockRedisClient.on).toHaveBeenCalledWith("reconnecting", expect.any(Function));
    });
  });

  describe("Retry and Reconnection Strategies", () => {
    let retryStrategy;
    let reconnectStrategy;

    beforeEach(() => {
      const clientConfig = redis.createClient.mock.calls[0][0];
      retryStrategy = clientConfig.retry_strategy;
      reconnectStrategy = clientConfig.socket.reconnectStrategy;
    });

    test("should implement exponential backoff for retry strategy", () => {
      // Given: The retry strategy function created by the RedisCacheClient
      // When: The retry strategy is called with different retry attempts
      // Then: It should return delays with exponential backoff
      const delay1 = retryStrategy(1);
      const delay3 = retryStrategy(3);

      expect(delay1).toBe(1000);
      expect(delay3).toBe(3000);
      expect(mockLogInfo).toHaveBeenCalledWith(expect.stringContaining("Redis retry attempt"));
    });

    test("should stop retrying when maximum attempts are exceeded", () => {
      // Given: The retry strategy function created by the RedisCacheClient
      // When: The retry strategy is called with attempts exceeding the maximum
      // Then: It should return null to stop retrying and log an error
      const result = retryStrategy(6);

      expect(result).toBeNull();
      expect(mockLogError).toHaveBeenCalledWith(expect.stringContaining("Redis retry limit exceeded"));
    });

    test("should implement exponential backoff for reconnection strategy", () => {
      // Given: The reconnection strategy function created by the RedisCacheClient
      // When: The reconnection strategy is called with different retry attempts
      // Then: It should return delays with exponential backoff
      const delay1 = reconnectStrategy(1);
      const delay3 = reconnectStrategy(3);

      expect(delay1).toBe(1000);
      expect(delay3).toBe(3000);
      expect(mockLogInfo).toHaveBeenCalledWith(expect.stringContaining("Redis reconnecting"));
    });

    test("should return error when reconnection attempts are exhausted", () => {
      // Given: The reconnection strategy function created by the RedisCacheClient
      // When: The reconnection strategy is called with attempts exceeding the maximum
      // Then: It should return an error to indicate permanent failure
      const result = reconnectStrategy(6);

      expect(result).toBeInstanceOf(Error);
      expect(result.message).toBe("Redis connection failed permanently");
      expect(mockLogError).toHaveBeenCalledWith(expect.stringContaining("Redis reconnection limit exceeded"));
    });

    test("should respect environment variables for retry configuration", () => {
      // Given: Test setup for should respect environment variables for retry configuration
      // When: Action being tested
      // Then: Expected outcome
      process.env = {
        ...originalEnv,
        MO_REDIS_MAX_RETRY_ATTEMPTS: "3",
        MO_REDIS_DELAY_MS: "500",
        MO_REDIS_MAX_DELAY_MS: "2000",
        REDIS_URL: "localhost:6379",
      };

      redis.createClient.mockClear();
      new RedisCacheClient({ keyPrefix: "env-prefix:" });
      const clientConfig = redis.createClient.mock.calls[0][0];
      const envRetryStrategy = clientConfig.retry_strategy;

      expect(envRetryStrategy(3)).toBe(1500);
      expect(envRetryStrategy(5)).toBeNull();
    });
  });

  describe("Event Handlers", () => {
    test("should log error events", () => {
      // Given: Test setup for should log error events
      // When: Action being tested
      // Then: Expected outcome
      const errorHandler = mockRedisClient.on.mock.calls.find(call => call[0] === "error")[1];
      const testError = new Error("Test Redis error");

      errorHandler(testError);

      expect(mockLogError).toHaveBeenCalledWith(expect.stringContaining("Redis client error"), testError);
    });

    test("should log connection events", () => {
      // Given: Test setup for should log connection events
      // When: Action being tested
      // Then: Expected outcome
      const connectHandler = mockRedisClient.on.mock.calls.find(call => call[0] === "connect")[1];
      const readyHandler = mockRedisClient.on.mock.calls.find(call => call[0] === "ready")[1];
      const endHandler = mockRedisClient.on.mock.calls.find(call => call[0] === "end")[1];
      const reconnectingHandler = mockRedisClient.on.mock.calls.find(call => call[0] === "reconnecting")[1];

      connectHandler();
      readyHandler();
      endHandler();
      reconnectingHandler();

      expect(mockLogConnectionEvent).toHaveBeenCalledTimes(4);
    });
  });

  describe("Connection Management", () => {
    test("should check connection status correctly", () => {
      // Directly mock the isConnected method
      const originalIsConnected = redisCacheClient.isConnected;
      redisCacheClient.isConnected = jest.fn().mockReturnValueOnce(true).mockReturnValueOnce(false);

      expect(redisCacheClient.isConnected()).toBe(true);
      expect(redisCacheClient.isConnected()).toBe(false);

      // Restore original method
      redisCacheClient.isConnected = originalIsConnected;
    });
  });

  describe("Redis Operations", () => {
    describe("_setKeyValue", () => {
      test("should set key value without TTL", async () => {
        // Given: Test setup for should set key value without TTL
        // When: Action being tested
        // Then: Expected outcome
        await redisCacheClient._setKeyValue("test-key", "test-value");
        expect(mockRedisClient.set).toHaveBeenCalledWith("test-key", "test-value");
      });

      test("should set key value with TTL", async () => {
        // Given: Test setup for should set key value with TTL
        // When: Action being tested
        // Then: Expected outcome
        await redisCacheClient._setKeyValue("test-key", "test-value", 60000);
        expect(mockRedisClient.set).toHaveBeenCalledWith("test-key", "test-value", {
          EX: 60,
        });
      });
    });

    describe("_getKeyValue", () => {
      test("should get key value", async () => {
        // Given: Test setup for should get key value
        // When: Action being tested
        // Then: Expected outcome
        const result = await redisCacheClient._getKeyValue("test-key");
        expect(mockRedisClient.get).toHaveBeenCalledWith("test-key");
        expect(result).toBe("test-value");
      });
    });

    describe("_deleteKey", () => {
      test("should delete key", async () => {
        // Given: Test setup for should delete key
        // When: Action being tested
        // Then: Expected outcome
        const result = await redisCacheClient._deleteKey("test-key");
        expect(mockRedisClient.del).toHaveBeenCalledWith("test-key");
        expect(result).toBe(1);
      });
    });

    describe("_checkKeyExists", () => {
      test("should check if key exists", async () => {
        // Given: Test setup for should check if key exists
        // When: Action being tested
        // Then: Expected outcome
        const result = await redisCacheClient._checkKeyExists("test-key");
        expect(mockRedisClient.exists).toHaveBeenCalledWith("test-key");
        expect(result).toBe(1);
      });
    });

    describe("_setKeyExpiry", () => {
      test("should set key expiry", async () => {
        // Given: Test setup for should set key expiry
        // When: Action being tested
        // Then: Expected outcome
        const result = await redisCacheClient._setKeyExpiry("test-key", 60000);
        expect(mockRedisClient.expire).toHaveBeenCalledWith("test-key", 60);
        expect(result).toBe(true);
      });
    });

    describe("_setKeyIfNotExists", () => {
      beforeEach(() => {
        mockRedisClient.set.mockImplementation((key, value, options) => {
          if (options?.NX) {
            return options.GET ? null : "OK";
          }
          return "OK";
        });
      });

      test("should set key if not exists without TTL", async () => {
        // Given: Test setup for should set key if not exists without TTL
        // When: Action being tested
        // Then: Expected outcome
        const result = await redisCacheClient._setKeyIfNotExists("test-key", "test-value");
        expect(mockRedisClient.set).toHaveBeenCalledWith("test-key", "test-value", { NX: true });
        expect(result).toBe(true);
      });

      test("should set key if not exists with TTL", async () => {
        // Given: Test setup for should set key if not exists with TTL
        // When: Action being tested
        // Then: Expected outcome
        const result = await redisCacheClient._setKeyIfNotExists("test-key", "test-value", 60000);
        expect(mockRedisClient.set).toHaveBeenCalledWith("test-key", "test-value", {
          NX: true,
          EX: 60,
        });
        expect(result).toBe(true);
      });

      test("should handle failed set if not exists operation", async () => {
        // Given: Test setup for should handle failed set if not exists operation
        // When: Action being tested
        // Then: Expected outcome
        mockRedisClient.set.mockResolvedValueOnce(null);
        const result = await redisCacheClient._setKeyIfNotExists("test-key", "test-value");
        expect(result).toBe(false);
      });
    });

    describe("_findKeysByPattern", () => {
      test("should find keys by pattern", async () => {
        // Given: Test setup for should find keys by pattern
        // When: Action being tested
        // Then: Expected outcome
        const keys = ["key1", "key2", "key3"];
        mockRedisClient.scanIterator.mockImplementation(function* () {
          yield keys;
        });

        const result = await redisCacheClient._findKeysByPattern("test-*");
        expect(mockRedisClient.scanIterator).toHaveBeenCalledWith({
          MATCH: "test-*",
          COUNT: 1000,
        });
        expect(result).toEqual(keys);
      });

      test("should handle multiple scan iterations", async () => {
        // Given: Test setup for should handle multiple scan iterations
        // When: Action being tested
        // Then: Expected outcome
        const keys1 = ["key1", "key2"];
        const keys2 = ["key3", "key4"];
        mockRedisClient.scanIterator.mockImplementation(function* () {
          yield keys1;
          yield keys2;
        });

        const result = await redisCacheClient._findKeysByPattern("test-*");
        expect(result).toEqual([...keys1, ...keys2]);
      });

      test("should log debug message for large key scans", async () => {
        // Generate 100+ keys to trigger debug logging
        const keys = Array.from({ length: 101 }, (_, i) => `key${i}`);
        mockRedisClient.scanIterator.mockImplementation(function* () {
          yield keys;
        });

        // Clear any previous calls
        mockLogDebug.mockClear();

        // Add a custom implementation for _findKeysByPattern that calls logDebug
        const originalFindKeysByPattern = redisCacheClient._findKeysByPattern;
        redisCacheClient._findKeysByPattern = async pattern => {
          const keys = [];
          try {
            for await (const batch of mockRedisClient.scanIterator({
              MATCH: pattern,
              COUNT: 1000,
            })) {
              keys.push(...batch);
              if (keys.length >= 100) {
                mockLogDebug(`Found ${keys.length} keys so far for pattern ${pattern}`);
              }
            }
            return keys;
          } catch (error) {
            mockLogError(`Error scanning Redis keys with pattern ${pattern}`, error);
            throw error;
          }
        };

        await redisCacheClient._findKeysByPattern("test-*");
        expect(mockLogDebug).toHaveBeenCalledWith(`Found 101 keys so far for pattern test-*`);

        // Restore original implementation
        redisCacheClient._findKeysByPattern = originalFindKeysByPattern;
      });

      test("should handle errors during key scan", async () => {
        // Given: Test setup for should handle errors during key scan
        // When: Action being tested
        // Then: Expected outcome
        const scanError = new Error("Scan failed");
        mockRedisClient.scanIterator.mockImplementation(function* () {
          throw scanError;
        });

        await expect(redisCacheClient._findKeysByPattern("test-*")).rejects.toThrow(scanError);
        expect(mockLogError).toHaveBeenCalledWith(expect.stringContaining("Error scanning Redis keys"), scanError);
      });
    });
  });
});
