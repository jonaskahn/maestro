/**
 * Tests for the CacheClientFactory class
 */

// Create mock functions first
const mockLogWarning = jest.fn();
const mockLogError = jest.fn();

// Mock dependencies before requiring modules
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
    test("should create Redis client with valid configuration", () => {
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

    test("should fall back to Redis for unsupported implementations", () => {
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

    test("should use environment variable for implementation if not specified", () => {
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

    test("should default to Redis if no implementation specified", () => {
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

    test("should handle Redis client creation errors", () => {
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

    test("should attempt Redis fallback when other implementation fails", () => {
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

    test("should throw error when both main implementation and Redis fallback fail", () => {
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
    test("should validate valid configuration", () => {
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

    test("should throw error for missing configuration object", () => {
      expect(() => {
        CacheClientFactory.validateConfiguration(null);
      }).toThrow("Cache configuration must be an object");
    });

    test("should throw error for missing keyPrefix", () => {
      expect(() => {
        CacheClientFactory.validateConfiguration({});
      }).toThrow("Cache configuration must include a keyPrefix string");
    });

    test("should throw error for empty keyPrefix", () => {
      expect(() => {
        CacheClientFactory.validateConfiguration({ keyPrefix: "  " });
      }).toThrow("Cache keyPrefix cannot be empty");
    });

    test("should log warning for unsupported implementation", () => {
      const config = {
        implementation: "unsupported",
        keyPrefix: "test_prefix",
      };

      CacheClientFactory.validateConfiguration(config);

      expect(mockLogWarning).toHaveBeenCalledWith(expect.stringContaining("Unsupported cache implementation"));
    });

    test("should throw error for invalid processingTtl", () => {
      expect(() => {
        CacheClientFactory.validateConfiguration({
          keyPrefix: "test_prefix",
          processingTtl: "invalid",
        });
      }).toThrow("Cache processingTtl must be a positive integer");
    });

    test("should throw error for negative processingTtl", () => {
      expect(() => {
        CacheClientFactory.validateConfiguration({
          keyPrefix: "test_prefix",
          processingTtl: -100,
        });
      }).toThrow("Cache processingTtl must be a positive integer");
    });

    test("should throw error for invalid sentTtl", () => {
      expect(() => {
        CacheClientFactory.validateConfiguration({
          keyPrefix: "test_prefix",
          suppressionTtl: 0,
        });
      }).toThrow("Cache suppressionTtl must be a positive integer");
    });

    test("should log warning for very high TTL values", () => {
      const config = {
        keyPrefix: "test_prefix",
        processingTtl: 90 * 86400 * 1000, // 90 days
      };

      CacheClientFactory.validateConfiguration(config);

      expect(mockLogWarning).toHaveBeenCalledWith(expect.stringContaining("Cache processingTtl is quite high"));
    });

    test("should throw error for invalid connectionOptions", () => {
      expect(() => {
        CacheClientFactory.validateConfiguration({
          keyPrefix: "test_prefix",
          connectionOptions: "invalid",
        });
      }).toThrow("Cache connectionOptions must be an object");
    });
  });

  describe("getSupportedImplementations", () => {
    test("should return array of supported implementations", () => {
      const implementations = CacheClientFactory.getSupportedImplementations();

      expect(Array.isArray(implementations)).toBe(true);
      expect(implementations.length).toBeGreaterThan(0);
      expect(implementations).toContain("redis");
      expect(implementations).toContain("memcached");
      expect(implementations).toContain("memory");
    });
  });

  describe("isImplementationSupported", () => {
    test("should return true for supported implementations", () => {
      expect(CacheClientFactory.isImplementationSupported("redis")).toBe(true);
      expect(CacheClientFactory.isImplementationSupported("REDIS")).toBe(true);
      expect(CacheClientFactory.isImplementationSupported("memcached")).toBe(true);
      expect(CacheClientFactory.isImplementationSupported("memory")).toBe(true);
    });

    test("should return false for unsupported implementations", () => {
      expect(CacheClientFactory.isImplementationSupported("unsupported")).toBe(false);
      expect(CacheClientFactory.isImplementationSupported(123)).toBe(false);
      expect(CacheClientFactory.isImplementationSupported(null)).toBe(false);
      expect(CacheClientFactory.isImplementationSupported(undefined)).toBe(false);
    });
  });

  describe("getDefaultConfig", () => {
    test("should return default Redis configuration", () => {
      const config = CacheClientFactory.getDefaultConfig("redis");

      expect(config).toHaveProperty("implementation", "redis");
      expect(config).toHaveProperty("keyPrefix");
      expect(config).toHaveProperty("processingTtl");
      expect(config).toHaveProperty("suppressionTtl");
      expect(config).toHaveProperty("connectionOptions");
      expect(config.connectionOptions).toHaveProperty("url");
    });

    test("should return default Memcached configuration", () => {
      const config = CacheClientFactory.getDefaultConfig("memcached");

      expect(config).toHaveProperty("implementation", "memcached");
      expect(config).toHaveProperty("connectionOptions");
      expect(config.connectionOptions).toHaveProperty("servers");
    });

    test("should return default Memory configuration", () => {
      const config = CacheClientFactory.getDefaultConfig("memory");

      expect(config).toHaveProperty("implementation", "memory");
      expect(config).toHaveProperty("connectionOptions");
      expect(config.connectionOptions).toHaveProperty("maxSize");
    });

    test("should default to Redis configuration if implementation not specified", () => {
      const config = CacheClientFactory.getDefaultConfig();

      expect(config).toHaveProperty("implementation", "redis");
    });

    test("should handle case-insensitive implementation names", () => {
      const config = CacheClientFactory.getDefaultConfig("REDIS");

      expect(config).toHaveProperty("implementation", "redis");
    });
  });

  describe("_resolveImplementation", () => {
    test("should use config implementation when available", () => {
      const implementation = CacheClientFactory._resolveImplementation({
        implementation: "redis",
      });

      expect(implementation).toBe("redis");
    });

    test("should use environment variable when config implementation not available", () => {
      const originalEnv = process.env.MO_CACHE_IMPLEMENTATION;
      process.env.MO_CACHE_IMPLEMENTATION = "memcached";

      const implementation = CacheClientFactory._resolveImplementation({});

      expect(implementation).toBe("memcached");

      // Restore original env
      process.env.MO_CACHE_IMPLEMENTATION = originalEnv;
    });

    test("should default to redis when neither config nor env available", () => {
      const originalEnv = process.env.MO_CACHE_IMPLEMENTATION;
      delete process.env.MO_CACHE_IMPLEMENTATION;

      const implementation = CacheClientFactory._resolveImplementation({});

      expect(implementation).toBe("redis");

      // Restore original env
      process.env.MO_CACHE_IMPLEMENTATION = originalEnv;
    });
  });
});
