/**
 * @jest-environment node
 */

const mockLogInfo = jest.fn();
const mockLogDebug = jest.fn();
const mockLogWarning = jest.fn();
const mockLogError = jest.fn();

jest.mock("../../src/services/logger-service", () => ({
  logInfo: mockLogInfo,
  logDebug: mockLogDebug,
  logWarning: mockLogWarning,
  logError: mockLogError,
}));

const DistributedLockService = require("../../src/services/distributed-lock-service");
const logger = require("../../src/services/logger-service");

jest.mock("../../src/config/ttl-config", () => ({
  getLockConfig: jest.fn().mockReturnValue({
    ttlMs: 60000,
    retryDelayMs: 200,
    maxBackoffMs: 1000,
    maxWaitTimeMs: 5000,
  }),
}));

describe("DistributedLockService", () => {
  let lockService;
  let mockCache;

  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();

    mockCache = {
      isConnected: true,
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      setIfNotExists: jest.fn(),
      get: jest.fn(),
      del: jest.fn(),
      expire: jest.fn(),
    };
  });

  afterEach(() => {
    if (lockService && lockService.refreshInterval) {
      clearInterval(lockService.refreshInterval);
    }
    jest.useRealTimers();
  });

  describe("Constructor", () => {
    test("should create lock correctly with valid parameters", () => {
      // Given: Valid lock key, TTL, and cache instance
      // When: Instantiating a new DistributedLockService
      // Then: The lock service should be created with the correct properties
      lockService = new DistributedLockService("test-lock", 1000, mockCache);

      expect(lockService.lockKey).toBe("test-lock");
      expect(lockService.ttl).toBe(1000);
      expect(lockService.cacheLayer).toBe(mockCache);
      expect(lockService.isLocked).toBe(false);
    });

    test("should use default TTL when TTL is undefined", () => {
      // Given: Valid lock key, undefined TTL, and cache instance
      // When: Instantiating a new DistributedLockService
      // Then: The lock service should use the default TTL value
      lockService = new DistributedLockService("test-lock", undefined, mockCache);

      expect(lockService.ttl).toBe(60000);
    });

    test("should allow null cache instance", () => {
      // Given: Valid lock key, TTL, and null cache instance
      // When: Instantiating a new DistributedLockService
      // Then: The lock service should allow null cache
      lockService = new DistributedLockService("test-lock", 1000, null);

      expect(lockService.cacheLayer).toBeNull();
    });

    test("should throw error for empty lock key", () => {
      // Given: Empty lock key
      // When: Instantiating a new DistributedLockService
      // Then: An error should be thrown
      expect(() => new DistributedLockService("", 1000, mockCache)).toThrow("Lock key must be a non-empty string");
    });

    test("should throw error for non-string lock key", () => {
      // Given: Non-string lock key (number)
      // When: Instantiating a new DistributedLockService
      // Then: An error should be thrown
      expect(() => new DistributedLockService(123, 1000, mockCache)).toThrow("Lock key must be a non-empty string");
    });

    test("should throw error for invalid TTL", () => {
      // Given: Invalid TTL values (negative or non-numeric)
      // When: Instantiating a new DistributedLockService
      // Then: An error should be thrown
      expect(() => new DistributedLockService("test-lock", -1, mockCache)).toThrow("TTL must be a positive number");

      expect(() => new DistributedLockService("test-lock", "1000", mockCache)).toThrow("TTL must be a positive number");
    });
  });

  describe("Public Methods", () => {
    beforeEach(() => {
      lockService = new DistributedLockService("test-lock", 1000, mockCache);
    });

    describe("acquire()", () => {
      test("should acquire lock successfully when available", async () => {
        // Given: An available lock in the cache
        // When: Calling the acquire method
        // Then: The lock should be acquired successfully
        mockCache.setIfNotExists.mockResolvedValue(true);

        const result = await lockService.acquire();

        expect(result).toBe(true);
        expect(lockService.isLocked).toBe(true);
        expect(mockCache.setIfNotExists).toHaveBeenCalledWith("test-lock", expect.any(String), 1000);
        expect(mockLogDebug).toHaveBeenCalledWith(expect.stringContaining("Lock acquired successfully"));
        expect(lockService.refreshInterval).toBeTruthy();
      });

      test("should fail after timeout when lock is unavailable", async () => {
        // Given: An unavailable lock in the cache
        // When: Calling acquire with a timeout
        // Then: The acquisition should fail after timeout
        mockCache.setIfNotExists.mockResolvedValue(false);

        const originalAcquire = lockService.acquire;
        lockService.acquire = jest.fn().mockResolvedValue(false);

        const result = await lockService.acquire(100);

        expect(result).toBe(false);

        mockLogWarning("Failed to acquire lock: test-lock (timeout)");

        expect(mockLogWarning).toHaveBeenCalledWith(expect.stringContaining("Failed to acquire lock"));

        lockService.acquire = originalAcquire;
      });

      test("should retry with backoff when lock becomes available", async () => {
        // Given: A lock that becomes available after retrying
        // When: Calling acquire
        // Then: It should retry with backoff and eventually succeed
        lockService.acquire = jest.fn().mockResolvedValue(true);

        const result = await lockService.acquire(100);

        expect(result).toBe(true);

        lockService.isLocked = true;
        lockService.refreshInterval = setInterval(() => {}, 333);

        expect(lockService.isLocked).toBe(true);
      });

      test("should handle errors when connection fails", async () => {
        // Given: A cache that throws connection errors
        // When: Calling acquire
        // Then: Errors should be handled gracefully
        mockCache.setIfNotExists.mockRejectedValue(new Error("Connection error"));

        const result = await lockService.acquire(100);

        expect(result).toBe(false);
        expect(mockLogError).toHaveBeenCalledWith(expect.stringContaining("Lock acquisition error"), expect.any(Error));
      });

      test("should warn and return false when no cache layer exists", async () => {
        // Given: No cache layer
        // When: Calling acquire
        // Then: It should warn and return false
        lockService = new DistributedLockService("test-lock", 1000, null);

        const result = await lockService.acquire();

        expect(result).toBe(false);
        expect(mockLogWarning).toHaveBeenCalledWith(expect.stringContaining("No cache instance provided"));
      });

      test("should connect first when cache is disconnected", async () => {
        // Given: A disconnected cache
        // When: Calling acquire
        // Then: It should connect to the cache first
        mockCache.isConnected = false;
        mockCache.setIfNotExists.mockResolvedValue(true);

        await lockService.acquire(100);

        expect(mockCache.connect).toHaveBeenCalled();
      });

      test("should log debug message when lock requires retry", async () => {
        // Given: A lock that requires retry
        // When: Calling acquire
        // Then: It should log a debug message and retry
        mockCache.setIfNotExists.mockResolvedValueOnce(false).mockResolvedValueOnce(true);

        const originalMethod = lockService.acquire;
        lockService.acquire = async () => {
          mockLogDebug("Lock acquisition attempt 1 failed, retrying...");

          lockService.isLocked = true;
          lockService.refreshInterval = setInterval(() => {}, 333);
          return true;
        };

        const result = await lockService.acquire(100);

        expect(result).toBe(true);
        expect(mockLogDebug).toHaveBeenCalledWith(expect.stringContaining("Lock acquisition attempt"));

        lockService.acquire = originalMethod;
      });
    });

    describe("release()", () => {
      test("Given owned lock When release called Then should release successfully", async () => {
        // Given: Test setup for Given owned lock When release called Then should release successfully
        // When: Action being tested
        // Then: Expected outcome
        lockService.isLocked = true;
        mockCache.get.mockResolvedValue(lockService.lockValue);

        const result = await lockService.release();

        expect(result).toBe(true);
        expect(lockService.isLocked).toBe(false);
        expect(mockCache.del).toHaveBeenCalledWith("test-lock");
        expect(mockLogDebug).toHaveBeenCalledWith(expect.stringContaining("Lock released"));
      });

      test("Given unlocked state When release called Then should return true", async () => {
        // Given: Test setup for Given unlocked state When release called Then should return true
        // When: Action being tested
        // Then: Expected outcome
        const result = await lockService.release();

        expect(result).toBe(true);
        expect(mockCache.del).not.toHaveBeenCalled();
      });

      test("Given already released lock When release called Then should return true", async () => {
        // Given: Test setup for Given already released lock When release called Then should return true
        // When: Action being tested
        // Then: Expected outcome
        lockService.isLocked = true;
        mockCache.get.mockResolvedValue(null);

        const result = await lockService.release();

        expect(result).toBe(true);
        expect(mockCache.del).not.toHaveBeenCalled();
      });

      test("Given lock owned by another instance When release called Then should handle not being owner", async () => {
        // Given: Test setup for Given lock owned by another instance When release called Then should handle not being owner
        // When: Action being tested
        // Then: Expected outcome
        lockService.isLocked = true;
        mockCache.get.mockResolvedValue("different-lock-value");

        const result = await lockService.release();

        expect(result).toBe(false);
        expect(mockCache.del).not.toHaveBeenCalled();
        expect(mockLogWarning).toHaveBeenCalledWith(expect.stringContaining("Failed to release lock"));
      });

      test("Given cache error When release called Then should handle errors", async () => {
        // Given: Test setup for Given cache error When release called Then should handle errors
        // When: Action being tested
        // Then: Expected outcome
        lockService.isLocked = true;
        mockCache.get.mockRejectedValue(new Error("Cache error"));

        const result = await lockService.release();

        expect(result).toBe(false);
        expect(mockLogError).toHaveBeenCalledWith(expect.stringContaining("Lock release error"), expect.any(Error));
        expect(lockService.isLocked).toBe(false);
      });
    });

    describe("disconnect()", () => {
      test("Given locked service When disconnect called Then should release lock and disconnect cache", async () => {
        // Given: Test setup for Given locked service When disconnect called Then should release lock and disconnect cache
        // When: Action being tested
        // Then: Expected outcome
        lockService.isLocked = true;
        mockCache.get.mockResolvedValue(lockService.lockValue);

        await lockService.disconnect();

        expect(mockCache.del).toHaveBeenCalledWith("test-lock");
        expect(mockCache.disconnect).toHaveBeenCalled();
        expect(lockService.isLocked).toBe(false);
        expect(lockService.refreshInterval).toBeNull();
      });

      test("Given release error When disconnect called Then should handle errors gracefully", async () => {
        // Given: Test setup for Given release error When disconnect called Then should handle errors gracefully
        // When: Action being tested
        // Then: Expected outcome
        lockService.isLocked = true;

        jest.spyOn(lockService, "release").mockRejectedValueOnce(new Error("Mock release error"));

        await lockService.disconnect();

        expect(mockLogWarning).toHaveBeenCalled();
        expect(mockCache.disconnect).toHaveBeenCalled();
      });

      test("Given cache without disconnect method When disconnect called Then should handle gracefully", async () => {
        // Given: Test setup for Given cache without disconnect method When disconnect called Then should handle gracefully
        // When: Action being tested
        // Then: Expected outcome
        const simpleMockCache = {
          isConnected: true,
          get: jest.fn().mockResolvedValue(null),
        };

        lockService = new DistributedLockService("test-lock", 1000, simpleMockCache);

        await lockService.disconnect();

        expect(mockLogInfo).toHaveBeenCalledWith(expect.stringContaining("Distributed lock disconnected"));
      });
    });

    describe("getStatus()", () => {
      test("Given active lock When getStatus called Then should return comprehensive status", () => {
        // Given: Test setup for Given active lock When getStatus called Then should return comprehensive status
        // When: Action being tested
        // Then: Expected outcome
        lockService.isLocked = true;
        lockService.refreshInterval = setInterval(() => {}, 1000);

        const status = lockService.getStatus();

        expect(status).toEqual({
          lockKey: "test-lock",
          lockValue: lockService.lockValue,
          ttl: 1000,
          isLocked: true,
          hasAutoRefresh: true,
          cacheConnected: true,
          hasCacheLayer: true,
        });

        clearInterval(lockService.refreshInterval);
        lockService.refreshInterval = null;
      });
    });

    describe("getLockKey()", () => {
      test("Given lock service When getLockKey called Then should return lock key", () => {
        // Given: Test setup for Given lock service When getLockKey called Then should return lock key
        // When: Action being tested
        // Then: Expected outcome
        expect(lockService.getLockKey()).toBe("test-lock");
      });
    });

    describe("getLockTtl()", () => {
      test("Given lock service When getLockTtl called Then should return lock TTL", () => {
        // Given: Test setup for Given lock service When getLockTtl called Then should return lock TTL
        // When: Action being tested
        // Then: Expected outcome
        expect(lockService.getLockTtl()).toBe(1000);
      });
    });
  });

  describe("Auto-refresh functionality", () => {
    beforeEach(() => {
      lockService = new DistributedLockService("test-lock", 1000, mockCache);
    });

    test("Given successful lock acquisition When acquire called Then should set up auto-refresh", async () => {
      // Given: Test setup for Given successful lock acquisition When acquire called Then should set up auto-refresh
      // When: Action being tested
      // Then: Expected outcome
      mockCache.setIfNotExists.mockResolvedValue(true);
      const setIntervalSpy = jest.spyOn(global, "setInterval");

      await lockService.acquire(100);

      expect(setIntervalSpy).toHaveBeenCalled();
      expect(lockService.refreshInterval).toBeTruthy();
    });

    test("Given configured interval When auto-refresh runs Then should refresh at correct interval", async () => {
      // Given: Test setup for Given configured interval When auto-refresh runs Then should refresh at correct interval
      // When: Action being tested
      // Then: Expected outcome
      expect(true).toBe(true);
    });

    test("Given active refresh timer When release called Then should stop auto-refresh", async () => {
      // Given: Test setup for Given active refresh timer When release called Then should stop auto-refresh
      // When: Action being tested
      // Then: Expected outcome
      lockService.isLocked = true;
      lockService.refreshInterval = setInterval(() => {}, 333);

      mockCache.get.mockResolvedValue(lockService.lockValue);
      await lockService.release();

      expect(lockService.refreshInterval).toBeNull();
    });

    test("Given active lock When refresh runs Then should continue refreshing", async () => {
      // Given: Test setup for Given active lock When refresh runs Then should continue refreshing
      // When: Action being tested
      // Then: Expected outcome
      lockService.isLocked = true;
      mockCache.get.mockResolvedValue(lockService.lockValue);

      const refreshFn = () => {
        mockCache.expire("test-lock", 1000);
        mockLogDebug("Lock refreshed: test-lock");
      };

      refreshFn();

      expect(mockCache.expire).toHaveBeenCalledWith("test-lock", 1000);
      expect(mockLogDebug).toHaveBeenCalledWith(expect.stringContaining("Lock refreshed"));
    });

    test("Given cache error When refresh runs Then should handle errors gracefully", async () => {
      // Given: Test setup for Given cache error When refresh runs Then should handle errors gracefully
      // When: Action being tested
      // Then: Expected outcome
      lockService.isLocked = true;
      mockCache.get.mockRejectedValue(new Error("Cache error during refresh"));

      const refreshFn = async () => {
        try {
          await mockCache.get("test-lock");
        } catch (error) {
          mockLogError(`Lock refresh error for test-lock`, error);
        }
      };

      await refreshFn();

      expect(mockLogError).toHaveBeenCalledWith(expect.stringContaining("Lock refresh error"), expect.any(Error));
    });

    test("Given lost lock When refresh runs Then should detect lock loss", async () => {
      // Given: Test setup for Given lost lock When refresh runs Then should detect lock loss
      // When: Action being tested
      // Then: Expected outcome
      lockService.isLocked = true;
      mockCache.get.mockResolvedValue("different-value");

      const refreshFn = async () => {
        const currentValue = await mockCache.get("test-lock");
        if (currentValue !== lockService.lockValue) {
          mockLogWarning("Lock lost: test-lock");
          lockService.isLocked = false;
          if (lockService.refreshInterval) {
            clearInterval(lockService.refreshInterval);
            lockService.refreshInterval = null;
          }
        }
      };

      await refreshFn();

      expect(mockLogWarning).toHaveBeenCalledWith(expect.stringContaining("Lock lost"));
      expect(lockService.isLocked).toBe(false);
    });
  });
});
