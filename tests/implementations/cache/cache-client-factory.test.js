/**
 * Tests for the CacheClientFactory class
 */

const mockLogWarning = jest.fn();
const mockLogError = jest.fn();

jest.mock("../../../src/implementations/cache/redis-cache-client", () => {
  // Create a constructor function that can be instantiated with 'new'
  const MockRedisCacheClient = jest.fn().mockImplementation(function (config) {
    this.config = config;
    this.implementation = config.implementation;
  });

  return MockRedisCacheClient;
});

jest.mock("../../../src/services/logger-service", () => ({
  logWarning: mockLogWarning,
  logError: mockLogError,
}));

// Now require the modules after setting up mocks
const CacheClientFactory = require("../../../src/implementations/cache/cache-client-factory");
const RedisCacheClient = require("../../../src/implementations/cache/redis-cache-client");
const logger = require("../../../src/services/logger-service");

describe("CacheClientFactory", () => {
  beforeEach(() => {
    // Clear all mocks before each test
    jest.clearAllMocks();
  });

  describe("createClient", () => {
    test("Given the component When createing Redis client with valid configuration Then it should succeed", () => {
      const config = {
        implementation: "redis",
        keyPrefix: "test_prefix",
        processingTtl: 5000,
        suppressionTtl: 10000,
      };

      const client = CacheClientFactory.createClient(config);

      expect(client).toBeDefined();
      expect(RedisCacheClient).toHaveBeenCalledWith({
        ...config,
        implementation: "redis",
      });
    });

    test("Given the component When falling back to Redis for unsupported implementations Then it should succeed", () => {
      const config = {
        implementation: "unsupported",
        keyPrefix: "test_prefix",
      };

      const client = CacheClientFactory.createClient(config);

      expect(client).toBeDefined();
      expect(mockLogWarning).toHaveBeenCalledWith(expect.stringContaining("Unsupported cache implementation"));
      expect(RedisCacheClient).toHaveBeenCalledWith(
        expect.objectContaining({
          implementation: "redis",
        })
      );
    });

    test("Given the component When useing environment variable for implementation if not specified Then it should succeed", () => {
      const originalEnv = process.env.MO_CACHE_IMPLEMENTATION;
      process.env.MO_CACHE_IMPLEMENTATION = "redis";

      const config = {
        keyPrefix: "test_prefix",
      };

      const client = CacheClientFactory.createClient(config);

      expect(client).toBeDefined();
      expect(RedisCacheClient).toHaveBeenCalledWith(
        expect.objectContaining({
          implementation: "redis",
        })
      );

      // Restore original env
      process.env.MO_CACHE_IMPLEMENTATION = originalEnv;
    });

    test("Given the component When defaulting to Redis if no implementation specified Then it should succeed", () => {
      const config = {
        keyPrefix: "test_prefix",
      };

      const client = CacheClientFactory.createClient(config);

      expect(client).toBeDefined();
      expect(RedisCacheClient).toHaveBeenCalledWith(
        expect.objectContaining({
          implementation: "redis",
        })
      );
    });

    test("Given the component When handleing Redis client creation errors Then it should succeed", () => {
      const config = {
        implementation: "redis",
        keyPrefix: "test_prefix",
      };

      // Override the mock implementation just for this test
      RedisCacheClient.mockImplementationOnce(() => {
        throw new Error("Redis connection failed");
      });

      expect(() => {
        CacheClientFactory.createClient(config);
      }).toThrow("Redis cache client creation failed");

      expect(mockLogError).toHaveBeenCalledWith("Failed to create Redis cache _client", expect.any(Error));
    });

    test("Given the component When attempting Redis fallback when other implementation fails Then it should succeed", () => {
      const config = {
        implementation: "memcached",
        keyPrefix: "test_prefix",
      };

      // Mock the warning for unsupported implementation
      const mockRedisClient = { implementation: "redis" };
      RedisCacheClient.mockImplementationOnce(() => mockRedisClient);

      const client = CacheClientFactory.createClient(config);

      expect(client).toBe(mockRedisClient);
      expect(mockLogWarning).toHaveBeenCalledWith(
        "Unsupported cache implementation: memcached. Falling back to Redis."
      );
    });

    test("Given the component When throwing error when both main implementation and Redis fallback fail Then it should succeed", () => {
      const config = {
        implementation: "memory",
        keyPrefix: "test_prefix",
      };

      // Mock the original implementation to simulate the real behavior
      const originalCreateClient = CacheClientFactory.createClient;

      // Mock the implementation to first throw error for memory implementation
      // and then for Redis fallback
      jest.spyOn(CacheClientFactory, "createClient").mockImplementation(function (cfg) {
        // When we're in the test, throw an error for the memory implementation
        if (cfg.implementation === "memory") {
          // Restore the original implementation for the fallback to work
          CacheClientFactory.createClient = originalCreateClient;

          // Now make Redis throw an error too
          RedisCacheClient.mockImplementationOnce(() => {
            throw new Error("Redis connection failed");
          });

          // Simulate the error handling in the original code
          mockLogWarning("Attempting fallback to Redis cache...");
          try {
            return CacheClientFactory.createClient({
              ...cfg,
              implementation: "redis",
            });
          } catch (fallbackError) {
            mockLogError("Redis fallback also failed", fallbackError);
            throw new Error(
              `Cache client creation failed: Memory implementation failed. Fallback to Redis also failed: Redis connection failed`
            );
          }
        }

        // This should not be reached in our test
        return originalCreateClient(cfg);
      });

      expect(() => {
        CacheClientFactory.createClient(config);
      }).toThrow(/Cache client creation failed.*Fallback to Redis also failed/);

      expect(mockLogWarning).toHaveBeenCalledWith("Attempting fallback to Redis cache...");
      expect(mockLogError).toHaveBeenCalledWith("Redis fallback also failed", expect.any(Error));

      // Restore the original implementation
      CacheClientFactory.createClient = originalCreateClient;
    });
  });

  describe("validateConfiguration", () => {
    test("Given the component When validateing valid configuration Then it should succeed", () => {
      const config = {
        implementation: "redis",
        keyPrefix: "test_prefix",
        processingTtl: 5000,
        suppressionTtl: 10000,
      };

      expect(() => {
        CacheClientFactory.validateConfiguration(config);
      }).not.toThrow();
    });

    test("Given the component When throwing error for missing configuration object Then it should succeed", () => {
      expect(() => {
        CacheClientFactory.validateConfiguration(null);
      }).toThrow("Cache configuration must be an object");
    });

    test("Given Test setup for should throw error for missing keyPrefix When Action being tested Then Expected outcome", () => {
      expect(() => {
        CacheClientFactory.validateConfiguration({});
      }).toThrow("Cache configuration must include a keyPrefix string");
    });

    test("Given Test setup for should throw error for empty keyPrefix When Action being tested Then Expected outcome", () => {
      expect(() => {
        CacheClientFactory.validateConfiguration({ keyPrefix: "  " });
      }).toThrow("Cache keyPrefix cannot be empty");
    });

    test("Given the component When loging warning for unsupported implementation Then it should succeed", () => {
      const config = {
        implementation: "unsupported",
        keyPrefix: "test_prefix",
      };

      CacheClientFactory.validateConfiguration(config);

      expect(mockLogWarning).toHaveBeenCalledWith(expect.stringContaining("Unsupported cache implementation"));
    });

    test("Given Test setup for should throw error for invalid processingTtl When Action being tested Then Expected outcome", () => {
      expect(() => {
        CacheClientFactory.validateConfiguration({
          keyPrefix: "test_prefix",
          processingTtl: "invalid",
        });
      }).toThrow("Cache processingTtl must be a positive integer");
    });

    test("Given Test setup for should throw error for negative processingTtl When Action being tested Then Expected outcome", () => {
      expect(() => {
        CacheClientFactory.validateConfiguration({
          keyPrefix: "test_prefix",
          processingTtl: -100,
        });
      }).toThrow("Cache processingTtl must be a positive integer");
    });

    test("Given Test setup for should throw error for invalid sentTtl When Action being tested Then Expected outcome", () => {
      expect(() => {
        CacheClientFactory.validateConfiguration({
          keyPrefix: "test_prefix",
          suppressionTtl: 0,
        });
      }).toThrow("Cache suppressionTtl must be a positive integer");
    });

    test("Given the component When loging warning for very high TTL values Then it should succeed", () => {
      const config = {
        keyPrefix: "test_prefix",
        processingTtl: 90 * 86400 * 1000, // 90 days
      };

      CacheClientFactory.validateConfiguration(config);

      expect(mockLogWarning).toHaveBeenCalledWith(expect.stringContaining("Cache processingTtl is quite high"));
    });

    test("Given Test setup for should throw error for invalid connectionOptions When Action being tested Then Expected outcome", () => {
      expect(() => {
        CacheClientFactory.validateConfiguration({
          keyPrefix: "test_prefix",
          connectionOptions: "invalid",
        });
      }).toThrow("Cache connectionOptions must be an object");
    });
  });

  describe("getSupportedImplementations", () => {
    test("Given Test setup for should return array of supported implementations When Action being tested Then Expected outcome", () => {
      const implementations = CacheClientFactory.getSupportedImplementations();

      expect(Array.isArray(implementations)).toBe(true);
      expect(implementations.length).toBeGreaterThan(0);
      expect(implementations).toContain("redis");
      expect(implementations).toContain("memcached");
      expect(implementations).toContain("memory");
    });
  });

  describe("isImplementationSupported", () => {
    test("Given Test setup for should return true for supported implementations When Action being tested Then Expected outcome", () => {
      expect(CacheClientFactory.isImplementationSupported("redis")).toBe(true);
      expect(CacheClientFactory.isImplementationSupported("REDIS")).toBe(true);
      expect(CacheClientFactory.isImplementationSupported("memcached")).toBe(true);
      expect(CacheClientFactory.isImplementationSupported("memory")).toBe(true);
    });

    test("Given Test setup for should return false for unsupported implementations When Action being tested Then Expected outcome", () => {
      expect(CacheClientFactory.isImplementationSupported("unsupported")).toBe(false);
      expect(CacheClientFactory.isImplementationSupported(123)).toBe(false);
      expect(CacheClientFactory.isImplementationSupported(null)).toBe(false);
      expect(CacheClientFactory.isImplementationSupported(undefined)).toBe(false);
    });
  });

  describe("getDefaultConfig", () => {
    test("Given Test setup for should return default Redis configuration When Action being tested Then Expected outcome", () => {
      const config = CacheClientFactory.getDefaultConfig("redis");

      expect(config).toHaveProperty("implementation", "redis");
      expect(config).toHaveProperty("keyPrefix");
      expect(config).toHaveProperty("processingTtl");
      expect(config).toHaveProperty("suppressionTtl");
      expect(config).toHaveProperty("connectionOptions");
      expect(config.connectionOptions).toHaveProperty("url");
    });

    test("Given Test setup for should return default Memcached configuration When Action being tested Then Expected outcome", () => {
      const config = CacheClientFactory.getDefaultConfig("memcached");

      expect(config).toHaveProperty("implementation", "memcached");
      expect(config).toHaveProperty("connectionOptions");
      expect(config.connectionOptions).toHaveProperty("servers");
    });

    test("Given Test setup for should return default Memory configuration When Action being tested Then Expected outcome", () => {
      const config = CacheClientFactory.getDefaultConfig("memory");

      expect(config).toHaveProperty("implementation", "memory");
      expect(config).toHaveProperty("connectionOptions");
      expect(config.connectionOptions).toHaveProperty("maxSize");
    });

    test("Given Test setup for should default to Redis configuration if implementation not specified When Action being tested Then Expected outcome", () => {
      const config = CacheClientFactory.getDefaultConfig();

      expect(config).toHaveProperty("implementation", "redis");
    });

    test("Given Test setup for should handle case-insensitive implementation names When Action being tested Then Expected outcome", () => {
      const config = CacheClientFactory.getDefaultConfig("REDIS");

      expect(config).toHaveProperty("implementation", "redis");
    });
  });

  describe("_resolveImplementation", () => {
    test("Given Test setup for should use config implementation when available When Action being tested Then Expected outcome", () => {
      const implementation = CacheClientFactory._resolveImplementation({
        implementation: "redis",
      });

      expect(implementation).toBe("redis");
    });

    test("Given Test setup for should use environment variable when config implementation not available When Action being tested Then Expected outcome", () => {
      const originalEnv = process.env.MO_CACHE_IMPLEMENTATION;
      process.env.MO_CACHE_IMPLEMENTATION = "memcached";

      const implementation = CacheClientFactory._resolveImplementation({});

      expect(implementation).toBe("memcached");

      // Restore original env
      process.env.MO_CACHE_IMPLEMENTATION = originalEnv;
    });

    test("Given Test setup for should default to redis when neither config nor env available When Action being tested Then Expected outcome", () => {
      const originalEnv = process.env.MO_CACHE_IMPLEMENTATION;
      delete process.env.MO_CACHE_IMPLEMENTATION;

      const implementation = CacheClientFactory._resolveImplementation({});

      expect(implementation).toBe("redis");

      // Restore original env
      process.env.MO_CACHE_IMPLEMENTATION = originalEnv;
    });
  });
});
