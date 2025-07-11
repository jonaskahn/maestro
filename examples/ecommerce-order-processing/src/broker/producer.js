/**
 * Order Producer for Ecommerce Example
 *
 * Produces order messages to Kafka from database records.
 */

const { DefaultProducer } = require("../../../../src");
const Logger = require("../utils/logger");
const database = require("../database/mongodb-client");

const ORDER_STATES = {
  PENDING: 1,
  COMPLETED: 2,
  FAILED: 3,
};

const ORDER_PRIORITIES = {
  NORMAL: "normal",
  HIGH: "high",
};

const DEFAULT_RETRY_SETTINGS = {
  MAX_RETRIES: 3,
  RETRY_DELAY_MS: 5000,
  BACKOFF_MULTIPLIER: 2,
};

class OrderProducer extends DefaultProducer {
  constructor(config = {}) {
    super({
      topic: config.topic || "ecommerce-orders",
      maxLag: config.maxLag || 50,
      cacheOptions: {
        keyPrefix: config.keyPrefix || "ECOMMERCE",
        processingTtl: config.processingTtl || 240_000,
      },
    });

    this.processedCount = 0;
    this.failedCount = 0;
    this.dbInitialized = false;
  }

  /**
   * Initialize database connection
   */
  async initializeDatabase() {
    Logger.info("Initializing database connection");
    await database.connect();
    Logger.success("Database ready");
  }

  async _ensureDatabaseInitialized() {
    if (!this.dbInitialized) {
      await this.initializeDatabase();
      this.dbInitialized = true;
    }
  }

  /**
   * Connect and initialize database
   */
  async connect() {
    await super.connect();
    await this._ensureDatabaseInitialized();
    return true;
  }

  /**
   * Get pending orders for processing
   * @param {object} criteria - MongoDB query criteria
   * @param {number} limit - Maximum number of orders to return
   * @param {Array} excludedIds - List of order IDs to exclude
   * @returns {Promise<Array>} Pending orders
   */
  async getPendingOrders(criteria, limit = 10, excludedIds = []) {
    try {
      if (!database.isConnected) {
        await database.connect();
      }

      const orders = await database.getPendingOrders(criteria, limit, excludedIds);
      return orders || [];
    } catch (error) {
      Logger.error("Failed to get pending orders", error);
      return [];
    }
  }

  /**
   * Get next batch of items to process
   */
  async getNextItems(criteria, limit, excludedIds) {
    await this._ensureDatabaseInitialized();

    const pendingOrdersCriteria = {
      state: ORDER_STATES.PENDING,
      retryCount: { $lt: DEFAULT_RETRY_SETTINGS.MAX_RETRIES },
      priority: criteria?.priority || {
        $in: [ORDER_PRIORITIES.NORMAL, ORDER_PRIORITIES.HIGH],
      },
    };

    const pendingOrders = await this.getPendingOrders(pendingOrdersCriteria, limit, excludedIds);

    if (pendingOrders.length === 0) {
      return [];
    }

    Logger.debug(`Producer found ${pendingOrders.length} pending orders`);
    return pendingOrders;
  }

  getItemId(item) {
    return item._id || item.orderId;
  }

  async _onItemProcessSuccess(orderId) {
    try {
      await database.collections.orders.updateOne(
        { _id: orderId },
        {
          $set: {
            status: "sent",
            sentAt: new Date(),
            updatedAt: new Date(),
          },
        }
      );

      this.processedCount++;
      Logger.success(`Marked order ${orderId} as sent`);
    } catch (error) {
      Logger.error(`Failed to mark order ${orderId} as sent`, error);
      throw error;
    }
  }

  async _onItemProcessFailed(orderId, errorMessage) {
    try {
      await database.markOrderAsFailed(orderId, errorMessage);
      this.failedCount++;
      Logger.warn(`Marked order ${orderId} as failed: ${errorMessage}`);
    } catch (error) {
      Logger.error(`Failed to mark order ${orderId} as failed`, error);
      throw error;
    }
  }

  /**
   * Cleanup database connection
   */
  async cleanup() {
    if (database.isConnected) {
      Logger.info("Disconnecting from database");
      await database.disconnect();
      Logger.success("Database disconnected");
    }
  }

  /**
   * Get production statistics
   * @returns {Object} Statistics about produced orders
   */
  getStats() {
    const totalAttempted = this.processedCount + this.failedCount;
    const successRate = totalAttempted > 0 ? ((this.processedCount / totalAttempted) * 100).toFixed(1) : "0.0";

    return {
      processedCount: this.processedCount,
      failedCount: this.failedCount,
      totalAttempted,
      successRate: parseFloat(successRate),
    };
  }

  /**
   * Reset statistics
   */
  resetStats() {
    this.processedCount = 0;
    this.failedCount = 0;
  }
}

module.exports = OrderProducer;
