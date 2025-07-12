// Mock the logger and TTL config first
const logDebugMock = jest.fn();
const logErrorMock = jest.fn();
const logWarningMock = jest.fn();
const logConnectionEventMock = jest.fn();
const logInfoMock = jest.fn();

jest.mock("../../src/services/logger-service", () => ({
  logDebug: logDebugMock,
  logError: logErrorMock,
  logWarning: logWarningMock,
  logConnectionEvent: logConnectionEventMock,
  logInfo: logInfoMock,
}));

jest.mock("../../src/config/ttl-config", () => ({
  getCacheConfig: jest.fn().mockReturnValue({
    processingTtl: 60000,
    freezingTtl: 180000,
  }),
}));

// Require modules after mocking
const AbstractCache = require("../../src/abstracts/abstract-cache");
const logger = require("../../src/services/logger-service");

// Concrete implementation for testing
class TestCache extends AbstractCache {
  constructor(config) {
    super(config);
    this.mockStorage = new Map();
    this.connected = false;
  }

  async _checkExistingConnection() {
    return this.connected;
  }

  async _connectTo() {
    this.connected = true;
    return true;
  }

  async _disconnectFrom() {
    this.connected = false;
    return true;
  }

  async _setKeyValue(key, value, ttlMs) {
    this.mockStorage.set(key, {
      value: typeof value === "object" ? JSON.stringify(value) : value,
      expires: ttlMs ? Date.now() + ttlMs : null,
    });
    return true;
  }

  async _getKeyValue(key) {
    const item = this.mockStorage.get(key);
    if (!item) return null;

    if (item.expires && Date.now() > item.expires) {
      this.mockStorage.delete(key);
      return null;
    }

    try {
      return typeof item.value === "string" && item.value.startsWith("{") ? JSON.parse(item.value) : item.value;
    } catch (e) {
      return item.value;
    }
  }

  async _deleteKey(key) {
    return this.mockStorage.delete(key);
  }

  async _checkKeyExists(key) {
    const item = this.mockStorage.get(key);
    if (!item) return false;

    if (item.expires && Date.now() > item.expires) {
      this.mockStorage.delete(key);
      return false;
    }

    return true;
  }

  async _setKeyExpiry(key, ttlMs) {
    const item = this.mockStorage.get(key);
    if (!item) return false;

    item.expires = Date.now() + ttlMs;
    this.mockStorage.set(key, item);
    return true;
  }

  async _setKeyIfNotExists(key, value, ttlMs) {
    if (await this._checkKeyExists(key)) {
      return false;
    }

    await this._setKeyValue(key, value, ttlMs);
    return true;
  }

  async _findKeysByPattern(pattern) {
    const regex = new RegExp(pattern.replace(/\*/g, ".*"));
    return Array.from(this.mockStorage.keys()).filter(key => regex.test(key));
  }
}

describe("AbstractCache", () => {
  let cacheInstance;
  const testConfig = {
    keyPrefix: "test:",
    processingTtl: 30000,
    suppressionTtl: 90000,
    connectionOptions: { host: "localhost" },
    implementation: "test-cache",
  };

  beforeEach(() => {
    jest.clearAllMocks();
    cacheInstance = new TestCache(testConfig);
  });

  afterEach(async () => {
    if (cacheInstance.isConnected()) {
      await cacheInstance.disconnect();
    }
  });

  describe("Instantiation", () => {
    it("should reject direct AbstractCache instantiation", () => {
      expect(() => new AbstractCache(testConfig)).toThrow("AbstractCache cannot be instantiated directly");
    });

    it("should reject missing configuration", () => {
      expect(() => new TestCache()).toThrow("Configuration must be an object");
      expect(() => new TestCache("invalid")).toThrow("Configuration must be an object");
    });

    it("should reject missing or empty keyPrefix", () => {
      expect(() => new TestCache({})).toThrow("keyPrefix is required and must be a non-empty string");
      expect(() => new TestCache({ keyPrefix: "" })).toThrow("keyPrefix is required and must be a non-empty string");
      expect(() => new TestCache({ keyPrefix: " " })).toThrow("keyPrefix is required and must be a non-empty string");
    });

    it("should apply default values for optional _config parameters", () => {
      const minimalConfig = { keyPrefix: "minimal:" };
      const instance = new TestCache(minimalConfig);

      expect(instance.config.processingTtl).toBe(60000);
      expect(instance.config.suppressionTtl).toBe(180000); // From the mock config
      expect(instance.config.connectionOptions).toEqual({});
      expect(instance.config.retryOptions).toEqual({});
    });

    it("should use environment variables when available", () => {
      const originalEnv = process.env;
      process.env = {
        ...originalEnv,
        MO_CACHE_KEY_PREFIX: "env:",
        MO_CACHE_KEY_SUFFIXES_PROCESSING: "_PROC:",
        MO_CACHE_KEY_SUFFIXES_SUPPRESSION: "_FREEZE:",
      };

      const envInstance = new TestCache({ keyPrefix: "env:" });
      expect(envInstance.config.processingPrefix).toBe("env:_PROC:");
      expect(envInstance.config.suppressionPrefix).toBe("env:_FREEZE:");

      process.env = originalEnv;
    });
  });

  describe("Utility Methods", () => {
    it("should validate string values", () => {
      expect(cacheInstance.isNonEmptyString("valid")).toBe(true);
      expect(cacheInstance.isNonEmptyString("")).toBe(false);
      expect(cacheInstance.isNonEmptyString(" ")).toBe(false);
      expect(cacheInstance.isNonEmptyString(null)).toBe(false);
      expect(cacheInstance.isNonEmptyString(undefined)).toBe(false);
      expect(cacheInstance.isNonEmptyString(123)).toBe(false);
      expect(cacheInstance.isNonEmptyString({})).toBe(false);
    });

    it("should retrieve environment values", () => {
      const originalEnv = process.env;
      process.env = {
        ...originalEnv,
        TEST_ENV_KEY: "test-value",
        TEST_FALLBACK_KEY: "fallback-value",
      };

      expect(cacheInstance.getEnvironmentValue(["TEST_ENV_KEY"])).toBe("test-value");
      expect(cacheInstance.getEnvironmentValue(["MISSING_KEY", "TEST_FALLBACK_KEY"])).toBe("fallback-value");
      expect(cacheInstance.getEnvironmentValue(["MISSING_KEY"])).toBeNull();

      process.env = originalEnv;
    });
  });

  describe("Connection Management", () => {
    it("should connect successfully", async () => {
      expect(cacheInstance.isConnected()).toBe(false);
      await cacheInstance.connect();
      expect(cacheInstance.isConnected()).toBe(true);
      expect(cacheInstance.isDisconnected()).toBe(false);
    });

    it("should reuse existing connection", async () => {
      // Force a manual mock of _checkExistingConnection
      jest.spyOn(cacheInstance, "_checkExistingConnection").mockResolvedValue(true);

      await cacheInstance.connect();

      // Verify the method was called and debug log was triggered
      expect(cacheInstance._checkExistingConnection).toHaveBeenCalled();
      expect(logDebugMock).toHaveBeenCalled();
    });

    it("should handle connection errors", async () => {
      // Create error and mock rejection
      const error = new Error("Connection failure");
      jest.spyOn(cacheInstance, "_connectTo").mockRejectedValue(error);

      // Test the behavior
      await expect(cacheInstance.connect()).rejects.toThrow(error);
      expect(cacheInstance.isDisconnected()).toBe(true);
      expect(logErrorMock).toHaveBeenCalled();
    });

    it("should disconnect successfully", async () => {
      await cacheInstance.connect();
      await cacheInstance.disconnect();

      expect(cacheInstance.isDisconnected()).toBe(true);
      expect(cacheInstance.isConnected()).toBe(false);
    });

    it("should handle disconnection errors gracefully", async () => {
      // Set up the test
      await cacheInstance.connect();
      const error = new Error("Disconnect failure");
      jest.spyOn(cacheInstance, "_disconnectFrom").mockRejectedValue(error);

      // Run the test
      await cacheInstance.disconnect();

      // Verify expectations
      expect(cacheInstance.isDisconnected()).toBe(true);
      expect(logWarningMock).toHaveBeenCalled();
    });
  });

  describe("Key-Value Operations", () => {
    beforeEach(async () => {
      await cacheInstance.connect();
    });

    it("should set and get string values", async () => {
      await cacheInstance.set("key1", "value1");
      const value = await cacheInstance.get("key1");
      expect(value).toBe("value1");
    });

    it("should set and get object values with automatic serialization", async () => {
      const testObj = { name: "test", value: 123 };
      await cacheInstance.set("objKey", testObj);
      const retrievedObj = await cacheInstance.get("objKey");
      expect(retrievedObj).toEqual(testObj);
    });

    it("should respect TTL values", async () => {
      const shortTtl = 50; // 50ms
      await cacheInstance.set("expiringKey", "temp-value", shortTtl);

      const immediateValue = await cacheInstance.get("expiringKey");
      expect(immediateValue).toBe("temp-value");

      await new Promise(resolve => setTimeout(resolve, 100));

      const expiredValue = await cacheInstance.get("expiringKey");
      expect(expiredValue).toBeNull();
    });

    it("should delete keys", async () => {
      await cacheInstance.set("deleteMe", "value");
      expect(await cacheInstance.get("deleteMe")).toBe("value");

      await cacheInstance.del("deleteMe");
      expect(await cacheInstance.get("deleteMe")).toBeNull();
    });

    it("should check key existence", async () => {
      await cacheInstance.set("existingKey", "exists");

      expect(await cacheInstance.exists("existingKey")).toBe(true);
      expect(await cacheInstance.exists("missingKey")).toBe(false);
    });

    it("should conditionally set keys if not exist", async () => {
      await cacheInstance.set("uniqueKey", "original");

      const resultExisting = await cacheInstance.setIfNotExists("uniqueKey", "replacement");
      expect(resultExisting).toBe(false);
      expect(await cacheInstance.get("uniqueKey")).toBe("original");

      const resultNew = await cacheInstance.setIfNotExists("newKey", "brandnew");
      expect(resultNew).toBe(true);
      expect(await cacheInstance.get("newKey")).toBe("brandnew");
    });

    it("should update key expiration time", async () => {
      await cacheInstance.set("updateTtl", "value");

      const shortTtl = 50; // 50ms
      await cacheInstance.expire("updateTtl", shortTtl);

      await new Promise(resolve => setTimeout(resolve, 100));
      expect(await cacheInstance.get("updateTtl")).toBeNull();
    });

    it("should find keys matching pattern", async () => {
      await cacheInstance.set("user:1", "Alice");
      await cacheInstance.set("user:2", "Bob");
      await cacheInstance.set("order:1", "Product");

      const userKeys = await cacheInstance.keys("user:*");
      expect(userKeys).toContain("user:1");
      expect(userKeys).toContain("user:2");
      expect(userKeys).not.toContain("order:1");
    });

    it("should reject invalid key values", async () => {
      await expect(cacheInstance.set("", "value")).rejects.toThrow("Key must be a non-empty string");
      await expect(cacheInstance.set(null, "value")).rejects.toThrow("Key must be a non-empty string");
      await expect(cacheInstance.set("key", null)).rejects.toThrow("Value cannot be undefined or null");
    });

    it("should reject operations when disconnected", async () => {
      await cacheInstance.disconnect();

      await expect(cacheInstance.set("key", "value")).rejects.toThrow("Cache is not connected");
      await expect(cacheInstance.get("key")).rejects.toThrow("Cache is not connected");
    });
  });

  describe("Processing State Management", () => {
    const itemId = "order-123";

    beforeEach(async () => {
      await cacheInstance.connect();
    });

    it("should mark items as processing", async () => {
      await cacheInstance.markAsProcessing(itemId);

      const processingKey = `${cacheInstance.config.processingPrefix}${itemId}`;
      expect(await cacheInstance.exists(processingKey)).toBe(true);
    });

    it("should retrieve processing item IDs", async () => {
      await cacheInstance.markAsProcessing("item1");
      await cacheInstance.markAsProcessing("item2");

      const processingIds = await cacheInstance.getProcessingIds();
      expect(processingIds).toContain("item1");
      expect(processingIds).toContain("item2");
    });

    it("should mark items as completed", async () => {
      await cacheInstance.markAsProcessing(itemId);
      await cacheInstance.markAsCompletedProcessing(itemId);

      const processingKey = `${cacheInstance.config.processingPrefix}${itemId}`;
      expect(await cacheInstance.exists(processingKey)).toBe(false);
    });

    it("should mark items as suppressed", async () => {
      await cacheInstance.markAsSuppressed(itemId);

      const suppressedKey = `${cacheInstance.config.suppressionPrefix}${itemId}`;
      expect(await cacheInstance.exists(suppressedKey)).toBe(true);
    });

    it("should check if items are suppressed", async () => {
      await cacheInstance.markAsSuppressed(itemId);
      expect(await cacheInstance.isSuppressedRecently(itemId)).toBe(true);
      expect(await cacheInstance.isSuppressedRecently("other-item")).toBe(false);
    });

    it("should retrieve suppressed item IDs", async () => {
      await cacheInstance.markAsSuppressed("item1");
      await cacheInstance.markAsSuppressed("item2");

      const suppressedIds = await cacheInstance.getSuppressedIds();
      expect(suppressedIds).toContain("item1");
      expect(suppressedIds).toContain("item2");
    });
  });

  describe("Abstract Method Requirements", () => {
    const createUnimplementedInstance = () => {
      class IncompleteCache extends AbstractCache {
        constructor(config) {
          super(config);
        }
      }

      return new IncompleteCache({ keyPrefix: "test:" });
    };

    it("should require implementation of abstract methods", async () => {
      const unimplemented = createUnimplementedInstance();

      // Test a subset of critical abstract methods
      await expect(unimplemented._checkExistingConnection()).rejects.toThrow(/must be implemented/);
      await expect(unimplemented._connectTo()).rejects.toThrow(/must be implemented/);
      await expect(unimplemented._getKeyValue("key")).rejects.toThrow(/must be implemented/);
      await expect(unimplemented._setKeyValue("key", "value")).rejects.toThrow(/must be implemented/);
      await expect(unimplemented._deleteKey("key")).rejects.toThrow(/must be implemented/);
    });
  });
});
