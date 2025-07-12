/**
 * Order Producer
 *
 * Handles the production of order messages to Kafka from database records.
 * Extends the DefaultProducer class to implement ecommerce-specific order processing logic.
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

/**
 * OrderProducer class
 *
 * Produces order messages to Kafka based on database records.
 * Extends DefaultProducer to implement order-specific logic.
 */
class OrderProducer extends DefaultProducer {
  constructor(config) {
    super(config);
  }

  /**
   * Initializes database connection
   *
   * @returns {Promise<void>} - Resolves when the database is connected
   */
  async initializeDatabase() {
    await database.connect();
    Logger.success("Database ready");
  }

  /**
   * Ensures the database is initialized
   *
   * @private
   * @returns {Promise<void>} - Resolves when database is initialized
   */
  async _ensureDatabaseInitialized() {
    if (!this.dbInitialized) {
      await this.initializeDatabase();
      this.dbInitialized = true;
    }
  }

  /**
   * Connects to Kafka and initializes the database
   *
   * @returns {Promise<void>} - Resolves when connections are established
   */
  async connect() {
    await super.connect();
    await this._ensureDatabaseInitialized();
  }

  /**
   * Gets the next batch of items to process
   *
   * @param {Object} criteria - Query criteria for filtering orders
   * @param {number} limit - Maximum number of items to retrieve
   * @param {Array<string>} excludedIds - IDs to exclude from the query
   * @returns {Promise<Array>} List of pending orders
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

  /**
   * Gets pending orders for processing from the database
   *
   * @param {object} criteria - MongoDB query criteria
   * @param {number} limit - Maximum number of orders to return
   * @param {Array} excludedIds - List of order IDs to exclude
   * @returns {Promise<Array>} Pending orders
   */
  async getPendingOrders(criteria, limit = 10, excludedIds = []) {
    try {
      const orders = await database.getPendingOrders(criteria, limit, excludedIds);
      return orders || [];
    } catch (error) {
      Logger.error("Failed to get pending orders", error);
      return [];
    }
  }

  /**
   * Gets unique identifier for an item
   *
   * @param {Object} item - The item object
   * @returns {string} The item's unique identifier
   */
  getItemId(item) {
    return item._id || item.orderId;
  }

  /**
   * Cleans up database connection
   *
   * @returns {Promise<void>} - Resolves when cleanup is complete
   */
  async cleanup() {
    if (database.isConnected) {
      Logger.info("Disconnecting from database");
      await database.disconnect();
      Logger.success("Database disconnected");
    }
  }
}

module.exports = OrderProducer;
