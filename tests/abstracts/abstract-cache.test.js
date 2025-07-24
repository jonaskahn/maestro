const mockLogDebug = jest.fn();
const mockLogError = jest.fn();
const mockLogWarning = jest.fn();
const mockLogConnectionEvent = jest.fn();
const mockLogInfo = jest.fn();
jest.mock("../../src/services/logger-service", () => ({
  logDebug: mockLogDebug,
  logError: mockLogError,
  logWarning: mockLogWarning,
  logConnectionEvent: mockLogConnectionEvent,
  logInfo: mockLogInfo,
}));
jest.mock("../../src/config/ttl-config", () => ({
  getTopicConfig: jest.fn().mockReturnValue({
    processingTtl: 60000,
    suppressionTtl: 180000,
  }),
}));

const AbstractCache = require("../../src/abstracts/abstract-cache");

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
    test("Given AbstractCache class When instantiated directly Then it should throw an error", () => {
      expect(() => new AbstractCache(testConfig)).toThrow("AbstractCache cannot be instantiated directly");
    });

    test("Given missing configuration When instantiated Then it should throw an error", () => {
      expect(() => new TestCache()).toThrow("Configuration must be an object");
      expect(() => new TestCache("invalid")).toThrow("Configuration must be an object");
    });

    test("Given missing or empty keyPrefix When instantiated Then it should throw an error", () => {
      expect(() => new TestCache({})).toThrow("keyPrefix is required and must be a non-empty string");
      expect(() => new TestCache({ keyPrefix: "" })).toThrow("keyPrefix is required and must be a non-empty string");
      expect(() => new TestCache({ keyPrefix: " " })).toThrow("keyPrefix is required and must be a non-empty string");
    });

    test("Given minimal configuration When instantiated Then it should apply default values", () => {
      const minimalConfig = { keyPrefix: "minimal:" };
      const instance = new TestCache(minimalConfig);

      expect(instance.config.processingTtl).toBe(60000);
      expect(instance.config.suppressionTtl).toBe(180000);
      expect(instance.config.connectionOptions).toEqual({});
      expect(instance.config.retryOptions).toEqual({});
    });

    test("Given environment variables When instantiated Then it should use environment values", () => {
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
    test("Given various input values When isNonEmptyString called Then it should validate correctly", () => {
      expect(cacheInstance.isNonEmptyString("valid")).toBe(true);
      expect(cacheInstance.isNonEmptyString("")).toBe(false);
      expect(cacheInstance.isNonEmptyString(" ")).toBe(false);
      expect(cacheInstance.isNonEmptyString(null)).toBe(false);
      expect(cacheInstance.isNonEmptyString(undefined)).toBe(false);
      expect(cacheInstance.isNonEmptyString(123)).toBe(false);
      expect(cacheInstance.isNonEmptyString({})).toBe(false);
    });

    test("Given environment variables When getEnvironmentValue called Then should retrieve values correctly", () => {
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
    test("Given disconnected cache When connect called Then should connect successfully", async () => {
      expect(cacheInstance.isConnected()).toBe(false);
      await cacheInstance.connect();
      expect(cacheInstance.isConnected()).toBe(true);
      expect(cacheInstance.isDisconnected()).toBe(false);
    });

    test("Given existing connection When connect called Then should reuse connection", async () => {
      jest.spyOn(cacheInstance, "_checkExistingConnection").mockResolvedValue(true);

      await cacheInstance.connect();

      expect(cacheInstance._checkExistingConnection).toHaveBeenCalled();
      expect(mockLogDebug).toHaveBeenCalled();
    });

    test("Given connection error When connect called Then should handle error", async () => {
      const error = new Error("Connection failure");
      jest.spyOn(cacheInstance, "_connectTo").mockRejectedValue(error);

      await expect(cacheInstance.connect()).rejects.toThrow(error);
      expect(cacheInstance.isDisconnected()).toBe(true);
      expect(mockLogError).toHaveBeenCalled();
    });

    test("Given connected cache When disconnect called Then should disconnect successfully", async () => {
      await cacheInstance.connect();
      await cacheInstance.disconnect();

      expect(cacheInstance.isDisconnected()).toBe(true);
      expect(cacheInstance.isConnected()).toBe(false);
    });

    test("Given disconnection error When disconnect called Then should handle error gracefully", async () => {
      await cacheInstance.connect();
      const error = new Error("Disconnect failure");
      jest.spyOn(cacheInstance, "_disconnectFrom").mockRejectedValue(error);

      await cacheInstance.disconnect();

      expect(cacheInstance.isDisconnected()).toBe(true);
      expect(mockLogWarning).toHaveBeenCalled();
    });
  });

  describe("Key-Value Operations", () => {
    beforeEach(async () => {
      await cacheInstance.connect();
    });

    test("Given string values When set and get called Then should store and retrieve correctly", async () => {
      await cacheInstance.set("key1", "value1");
      const value = await cacheInstance.get("key1");
      expect(value).toBe("value1");
    });

    test("Given object values When set and get called Then should serialize and deserialize correctly", async () => {
      const testObj = { name: "test", value: 123 };
      await cacheInstance.set("objKey", testObj);
      const retrievedObj = await cacheInstance.get("objKey");
      expect(retrievedObj).toEqual(testObj);
    });

    test("Given key with TTL When set called Then should respect expiration time", async () => {
      const shortTtl = 50;
      await cacheInstance.set("expiringKey", "temp-value", shortTtl);

      const immediateValue = await cacheInstance.get("expiringKey");
      expect(immediateValue).toBe("temp-value");

      await new Promise(resolve => setTimeout(resolve, 100));

      const expiredValue = await cacheInstance.get("expiringKey");
      expect(expiredValue).toBeNull();
    });

    test("Given existing key When del called Then should delete the key", async () => {
      await cacheInstance.set("deleteMe", "value");
      expect(await cacheInstance.get("deleteMe")).toBe("value");

      await cacheInstance.del("deleteMe");
      expect(await cacheInstance.get("deleteMe")).toBeNull();
    });

    test("Given keys When exists called Then should check existence correctly", async () => {
      await cacheInstance.set("existingKey", "exists");

      expect(await cacheInstance.exists("existingKey")).toBe(true);
      expect(await cacheInstance.exists("missingKey")).toBe(false);
    });

    test("Given keys When setIfNotExists called Then should conditionally set values", async () => {
      await cacheInstance.set("uniqueKey", "original");

      const resultExisting = await cacheInstance.setIfNotExists("uniqueKey", "replacement");
      expect(resultExisting).toBe(false);
      expect(await cacheInstance.get("uniqueKey")).toBe("original");

      const resultNew = await cacheInstance.setIfNotExists("newKey", "brandnew");
      expect(resultNew).toBe(true);
      expect(await cacheInstance.get("newKey")).toBe("brandnew");
    });

    test("Given key When expire called Then should update expiration time", async () => {
      await cacheInstance.set("updateTtl", "value");

      const shortTtl = 50;
      await cacheInstance.expire("updateTtl", shortTtl);

      await new Promise(resolve => setTimeout(resolve, 100));
      expect(await cacheInstance.get("updateTtl")).toBeNull();
    });

    test("Given keys with patterns When keys called Then should find matching keys", async () => {
      await cacheInstance.set("user:1", "Alice");
      await cacheInstance.set("user:2", "Bob");
      await cacheInstance.set("order:1", "Product");

      const userKeys = await cacheInstance.keys("user:*");
      expect(userKeys).toContain("user:1");
      expect(userKeys).toContain("user:2");
      expect(userKeys).not.toContain("order:1");
    });

    test("Given invalid key values When set called Then should reject", async () => {
      await expect(cacheInstance.set("", "value")).rejects.toThrow("Key must be a non-empty string");
      await expect(cacheInstance.set(null, "value")).rejects.toThrow("Key must be a non-empty string");
      await expect(cacheInstance.set("key", null)).rejects.toThrow("Value cannot be undefined or null");
    });

    test("Given disconnected cache When operations called Then should reject", async () => {
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

    test("Given item ID When markAsProcessing called Then should mark item as processing", async () => {
      await cacheInstance.markAsProcessing(itemId);

      const processingKey = `${cacheInstance.config.processingPrefix}${itemId}`;
      expect(await cacheInstance.exists(processingKey)).toBe(true);
    });

    test("Given processing items When getProcessingIds called Then should retrieve IDs", async () => {
      await cacheInstance.markAsProcessing("item1");
      await cacheInstance.markAsProcessing("item2");

      const processingIds = await cacheInstance.getProcessingIds();
      expect(processingIds).toContain("item1");
      expect(processingIds).toContain("item2");
    });

    test("Given processing item When clearProcessingState called Then should complete processing", async () => {
      await cacheInstance.markAsProcessing(itemId);
      await cacheInstance.clearProcessingState(itemId);

      const processingKey = `${cacheInstance.config.processingPrefix}${itemId}`;
      expect(await cacheInstance.exists(processingKey)).toBe(false);
    });

    test("Given item ID When markAsSuppressed called Then should mark item as suppressed", async () => {
      await cacheInstance.markAsSuppressed(itemId);

      const suppressedKey = `${cacheInstance.config.suppressionPrefix}${itemId}`;
      expect(await cacheInstance.exists(suppressedKey)).toBe(true);
    });

    test("Given suppressed item When isSuppressedRecently called Then should check suppression correctly", async () => {
      await cacheInstance.markAsSuppressed(itemId);
      expect(await cacheInstance.isSuppressedRecently(itemId)).toBe(true);
      expect(await cacheInstance.isSuppressedRecently("other-item")).toBe(false);
    });

    test("Given suppressed items When getSuppressedIds called Then should retrieve IDs", async () => {
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

    test("Given unimplemented abstract methods When called Then should throw errors", async () => {
      const unimplemented = createUnimplementedInstance();

      await expect(unimplemented._checkExistingConnection()).rejects.toThrow(/must be implemented/);
      await expect(unimplemented._connectTo()).rejects.toThrow(/must be implemented/);
      await expect(unimplemented._getKeyValue("key")).rejects.toThrow(/must be implemented/);
      await expect(unimplemented._setKeyValue("key", "value")).rejects.toThrow(/must be implemented/);
      await expect(unimplemented._deleteKey("key")).rejects.toThrow(/must be implemented/);
    });
  });
});
