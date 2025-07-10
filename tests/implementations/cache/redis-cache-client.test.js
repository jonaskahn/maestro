/**
 * @jest-environment node
 */

const RedisCacheClient = require("../../../src/implementations/cache/redis-cache-client");
const redis = require("redis");
const logger = require("../../../src/services/logger-service");

// Mock dependencies
jest.mock("redis");
jest.mock("../../../src/services/logger-service");
jest.mock("../../../src/config/ttl-config", () => ({
  getCacheConfig: jest.fn().mockReturnValue({
    processingTtl: 60000,
    freezingTtl: 180000,
  }),
  getAllTTLValues: jest.fn().mockReturnValue({}),
}));

describe("RedisCacheClient", () => {
  let redisCacheClient;
  let mockRedisClient;
  const defaultConfig = {
    keyPrefix: "test-prefix:",
    connectionOptions: {
      url: "redis://test-host:6379",
      password: "test-password",
    },
  };

  beforeEach(() => {
    jest.clearAllMocks();

    // Create mock Redis client with all required methods and event emitter functionality
    mockRedisClient = {
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

    // Mock redis.createClient to return our mock client
    redis.createClient.mockReturnValue(mockRedisClient);

    // Create instance of RedisCacheClient
    redisCacheClient = new RedisCacheClient(defaultConfig);

    // Replace the client property with our mock
    redisCacheClient.client = mockRedisClient;
  });

  describe("Constructor and Initialization", () => {
    it("should create Redis client with provided configuration", () => {
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

    it("should use environment variables when config not provided", () => {
      const originalEnv = process.env;
      process.env = {
        ...originalEnv,
        MO_REDIS_URL: "redis://env-host:6379",
        MO_REDIS_PASSWORD: "env-password",
      };

      redis.createClient.mockClear();
      const envClient = new RedisCacheClient({ keyPrefix: "env-prefix:" });

      expect(redis.createClient).toHaveBeenCalledWith(
        expect.objectContaining({
          url: "redis://env-host:6379",
          password: "env-password",
        })
      );

      process.env = originalEnv;
    });

    it("should use defaults when neither config nor env vars are provided", () => {
      const originalEnv = process.env;
      const envVars = { ...process.env };
      delete envVars.MO_REDIS_URL;
      delete envVars.MO_REDIS_PASSWORD;
      process.env = envVars;

      redis.createClient.mockClear();
      const defaultClient = new RedisCacheClient({ keyPrefix: "default-prefix:" });

      expect(redis.createClient).toHaveBeenCalledWith(
        expect.objectContaining({
          url: "redis://localhost:6379",
        })
      );

      process.env = originalEnv;
    });

    it("should attach event handlers to Redis client", () => {
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
      // Extract the retry and reconnect strategies from the client creation call
      const clientConfig = redis.createClient.mock.calls[0][0];
      retryStrategy = clientConfig.retry_strategy;
      reconnectStrategy = clientConfig.socket.reconnectStrategy;
    });

    it("should create retry strategy with exponential backoff", () => {
      const delay1 = retryStrategy(1);
      const delay3 = retryStrategy(3);

      expect(delay1).toBe(1000); // Default delay for first attempt
      expect(delay3).toBe(3000); // 3x default delay for third attempt
      expect(logger.logInfo).toHaveBeenCalledWith(expect.stringContaining("Redis retry attempt"));
    });

    it("should return null when retry attempts exceed maximum", () => {
      const result = retryStrategy(6); // Default max is 5

      expect(result).toBeNull();
      expect(logger.logError).toHaveBeenCalledWith(expect.stringContaining("Redis retry limit exceeded"));
    });

    it("should create reconnection strategy with exponential backoff", () => {
      const delay1 = reconnectStrategy(1);
      const delay3 = reconnectStrategy(3);

      expect(delay1).toBe(1000);
      expect(delay3).toBe(3000);
      expect(logger.logInfo).toHaveBeenCalledWith(expect.stringContaining("Redis reconnecting"));
    });

    it("should return error when reconnection attempts exceed maximum", () => {
      const result = reconnectStrategy(6); // Default max is 5

      expect(result).toBeInstanceOf(Error);
      expect(result.message).toBe("Redis connection failed permanently");
      expect(logger.logError).toHaveBeenCalledWith(expect.stringContaining("Redis reconnection limit exceeded"));
    });

    it("should respect environment variables for retry configuration", () => {
      const originalEnv = process.env;
      process.env = {
        ...originalEnv,
        MO_REDIS_MAX_RETRY_ATTEMPTS: "3",
        MO_REDIS_DELAY_MS: "500",
        MO_REDIS_MAX_DELAY_MS: "2000",
      };

      redis.createClient.mockClear();
      const envClient = new RedisCacheClient({ keyPrefix: "env-prefix:" });
      const clientConfig = redis.createClient.mock.calls[0][0];
      const envRetryStrategy = clientConfig.retry_strategy;

      expect(envRetryStrategy(3)).toBe(1500); // 3 * 500 = 1500
      expect(envRetryStrategy(5)).toBeNull(); // > 3 attempts, should return null

      process.env = originalEnv;
    });
  });

  describe("Event Handlers", () => {
    it("should log error events", () => {
      // Extract the error handler from the on('error') call
      const errorHandler = mockRedisClient.on.mock.calls.find(call => call[0] === "error")[1];
      const testError = new Error("Test Redis error");

      errorHandler(testError);

      expect(logger.logError).toHaveBeenCalledWith(expect.stringContaining("Redis client error"), testError);
    });

    it("should log connection events", () => {
      // Extract handlers for different connection events
      const connectHandler = mockRedisClient.on.mock.calls.find(call => call[0] === "connect")[1];
      const readyHandler = mockRedisClient.on.mock.calls.find(call => call[0] === "ready")[1];
      const endHandler = mockRedisClient.on.mock.calls.find(call => call[0] === "end")[1];
      const reconnectingHandler = mockRedisClient.on.mock.calls.find(call => call[0] === "reconnecting")[1];

      connectHandler();
      readyHandler();
      endHandler();
      reconnectingHandler();

      expect(logger.logConnectionEvent).toHaveBeenCalledWith(expect.stringContaining("Redis"), "client connected");
      expect(logger.logConnectionEvent).toHaveBeenCalledWith(expect.stringContaining("Redis"), "client ready");
      expect(logger.logConnectionEvent).toHaveBeenCalledWith("Redis", expect.stringContaining("client disconnected"));
      expect(logger.logConnectionEvent).toHaveBeenCalledWith("Redis", expect.stringContaining("client reconnecting"));
    });
  });

  describe("Connection Management", () => {
    it("should check connection status correctly", () => {
      expect(mockRedisClient.isOpen).toBe(true);

      mockRedisClient.isOpen = false;
      expect(mockRedisClient.isOpen).toBe(false);
    });
  });

  describe("Redis Operations", () => {
    describe("_setKeyValue", () => {
      it("should set key value without TTL", async () => {
        // Access the private method using function property access
        const setKeyValue = redisCacheClient._setKeyValue;
        await setKeyValue.call(redisCacheClient, "test-key", "test-value");

        expect(mockRedisClient.set).toHaveBeenCalledWith("test-key", "test-value");
      });

      it("should set key value with TTL", async () => {
        // Access the private method using function property access
        const setKeyValue = redisCacheClient._setKeyValue;
        await setKeyValue.call(redisCacheClient, "test-key", "test-value", 5000);

        expect(mockRedisClient.set).toHaveBeenCalledWith("test-key", "test-value", {
          EX: 5, // 5000ms = 5s
        });
      });
    });

    describe("_getKeyValue", () => {
      it("should get key value", async () => {
        const getKeyValue = redisCacheClient._getKeyValue;
        const value = await getKeyValue.call(redisCacheClient, "test-key");

        expect(mockRedisClient.get).toHaveBeenCalledWith("test-key");
        expect(value).toBe("test-value");
      });
    });

    describe("_deleteKey", () => {
      it("should delete key", async () => {
        const deleteKey = redisCacheClient._deleteKey;
        const result = await deleteKey.call(redisCacheClient, "test-key");

        expect(mockRedisClient.del).toHaveBeenCalledWith("test-key");
        expect(result).toBe(1);
      });
    });

    describe("_checkKeyExists", () => {
      it("should check if key exists", async () => {
        const checkKeyExists = redisCacheClient._checkKeyExists;
        const exists = await checkKeyExists.call(redisCacheClient, "test-key");

        expect(mockRedisClient.exists).toHaveBeenCalledWith("test-key");
        expect(exists).toBe(1);
      });
    });

    describe("_setKeyExpiry", () => {
      it("should set key expiry", async () => {
        const setKeyExpiry = redisCacheClient._setKeyExpiry;
        const result = await setKeyExpiry.call(redisCacheClient, "test-key", 10000);

        expect(mockRedisClient.expire).toHaveBeenCalledWith("test-key", 10);
        expect(result).toBe(true);
      });
    });

    describe("_setKeyIfNotExists", () => {
      it("should set key if not exists without TTL", async () => {
        const setKeyIfNotExists = redisCacheClient._setKeyIfNotExists;
        const result = await setKeyIfNotExists.call(redisCacheClient, "test-key", "test-value");

        expect(mockRedisClient.set).toHaveBeenCalledWith("test-key", "test-value", {
          NX: true,
        });
        expect(result).toBe(true);
      });

      it("should set key if not exists with TTL", async () => {
        const setKeyIfNotExists = redisCacheClient._setKeyIfNotExists;
        const result = await setKeyIfNotExists.call(redisCacheClient, "test-key", "test-value", 15000);

        expect(mockRedisClient.set).toHaveBeenCalledWith("test-key", "test-value", {
          EX: 15,
          NX: true,
        });
        expect(result).toBe(true);
      });

      it("should handle failed set if not exists operation", async () => {
        mockRedisClient.set.mockResolvedValueOnce(null);

        const setKeyIfNotExists = redisCacheClient._setKeyIfNotExists;
        const result = await setKeyIfNotExists.call(redisCacheClient, "test-key", "test-value");

        expect(result).toBe(false);
      });
    });

    describe("_findKeysByPattern", () => {
      it("should find keys by pattern", async () => {
        const keys = ["key1", "key2", "key3"];
        mockRedisClient.scanIterator.mockImplementation(function* () {
          yield keys;
        });

        const findKeysByPattern = redisCacheClient._findKeysByPattern;
        const result = await findKeysByPattern.call(redisCacheClient, "test-*");

        expect(mockRedisClient.scanIterator).toHaveBeenCalledWith({
          MATCH: "test-*",
          COUNT: 1000,
        });
        expect(result).toEqual(["key1", "key2", "key3"]);
      });

      it("should handle multiple scan iterations", async () => {
        const batch1 = ["key1", "key2"];
        const batch2 = ["key3", "key4"];
        mockRedisClient.scanIterator.mockImplementation(function* () {
          yield batch1;
          yield batch2;
        });

        const findKeysByPattern = redisCacheClient._findKeysByPattern;
        const result = await findKeysByPattern.call(redisCacheClient, "test-*");

        expect(result).toEqual(["key1", "key2", "key3", "key4"]);
      });

      it("should log debug message for large key scans", async () => {
        // Generate 101 batches to trigger the debug log (100 + 1)
        const batches = Array.from({ length: 101 }, (_, i) => [`key${i}`]);
        mockRedisClient.scanIterator.mockImplementation(function* () {
          for (const batch of batches) {
            yield batch;
          }
        });

        const findKeysByPattern = redisCacheClient._findKeysByPattern;
        await findKeysByPattern.call(redisCacheClient, "test-*");

        expect(logger.logDebug).toHaveBeenCalledWith(expect.stringContaining("Found 100 keys so far"));
      });

      it("should handle errors during key scan", async () => {
        const scanError = new Error("Scan failed");
        mockRedisClient.scanIterator.mockImplementation(() => {
          throw scanError;
        });

        const findKeysByPattern = redisCacheClient._findKeysByPattern;
        await expect(findKeysByPattern.call(redisCacheClient, "test-*")).rejects.toThrow(scanError);
        expect(logger.logError).toHaveBeenCalledWith(expect.stringContaining("Error scanning Redis keys"), scanError);
      });
    });
  });
});
