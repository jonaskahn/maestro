/**
 * Order Consumer
 *
 * Handles the consumption and processing of order messages from Kafka.
 * Extends the DefaultConsumer class to implement ecommerce-specific order processing logic.
 */

const { DefaultConsumer } = require("../../../../src");
const database = require("../database/mongodb-client");
const Logger = require("../utils/logger");

const SIMULATION_DELAYS = {
  INVENTORY_CHECK: 1_000,
  PAYMENT_PROCESSING: 1_000,
  ORDER_VALIDATION: 1_000,
};

/**
 * OrderConsumer class
 *
 * Processes orders from Kafka and updates their status in the database.
 * Extends DefaultConsumer to implement order-specific processing logic.
 */
class OrderConsumer extends DefaultConsumer {
  constructor(config = {}) {
    super(config);
  }

  /**
   * Processes an order
   *
   * @param {Object} orderData - Order data to process
   * @returns {Promise<Object>} Processing result
   */
  async process(orderData) {
    await this.validateOrder(orderData);
    await this.processOrderSteps(orderData);
    return this.createProcessingResult(orderData);
  }

  /**
   * Gets unique identifier for an item
   *
   * @param {Object} orderData - The order data object
   * @returns {string} The order's unique identifier
   */
  getItemId(orderData) {
    return orderData?._id;
  }

  /**
   * Checks if an item has been processed
   *
   * @private
   * @param {string} itemId - The item ID to check
   * @returns {Promise<boolean>} True if the item has been processed
   */
  async _isItemProcessed(itemId) {
    return await database.isOrderCompleted(itemId);
  }

  /**
   * Handles successful item processing
   *
   * @private
   * @param {string} itemId - ID of the successfully processed item
   * @returns {Promise<void>} - Resolves when the database is updated
   */
  async _onItemProcessSuccess(itemId) {
    await database.markOrderAsCompleted(itemId);
    Logger.success(`Order ${itemId} marked as completed`);
  }

  /**
   * Handles failed item processing
   *
   * @private
   * @param {string} itemId - ID of the failed item
   * @param {Error} error - Error that caused the failure
   * @returns {Promise<void>} - Resolves when the database is updated
   */
  async _onItemProcessFailed(itemId, error) {
    await database.markOrderAsFailed(itemId, error?.message);
    Logger.warn(`Order ${itemId} marked as failed: ${error?.message}`);
  }

  /**
   * Extracts order ID from order object
   *
   * @param {Object} order - Order object
   * @returns {string} Order ID
   */
  extractOrderId(order) {
    return order.orderId || order._id?.toString();
  }

  /**
   * Validates order data
   *
   * @param {Object} order - Order to validate
   * @throws {Error} If the order is invalid
   */
  validateOrder(order) {
    if (!order.orderId && !order._id) {
      throw new Error("Order must have an ID");
    }

    if (!order.items || order.items.length === 0) {
      throw new Error("Order must have items");
    }
  }

  /**
   * Processes the steps required to complete an order
   *
   * @param {Object} order - Order to process
   * @returns {Promise<void>} - Resolves when processing is complete
   */
  async processOrderSteps(order) {
    await this.simulateProcessingDelay();
    await this.updateOrderStatus(order, "processed");
  }

  /**
   * Updates order status in the database
   *
   * @param {Object} order - Order to update
   * @param {string} status - New status
   * @returns {Promise<void>} - Resolves when the database is updated
   */
  async updateOrderStatus(order, status) {
    if (status === "completed" || status === "processed") {
      await database.markOrderAsCompleted(order._id);
    } else if (status === "failed") {
      await database.markOrderAsFailed(order._id, "Processing failed");
    }
  }

  /**
   * Simulates processing delay for demonstration purposes
   *
   * @returns {Promise<void>} - Resolves after the delay
   */
  simulateProcessingDelay() {
    return new Promise(resolve => setTimeout(resolve, SIMULATION_DELAYS.ORDER_VALIDATION));
  }

  /**
   * Creates processing result object
   *
   * @param {Object} order - Processed order
   * @returns {Object} Processing result
   */
  createProcessingResult(order) {
    return {
      orderId: this.extractOrderId(order),
      status: "processed",
      processedAt: new Date(),
      total: order.total,
      items: order.items?.length || 0,
    };
  }
}

module.exports = OrderConsumer;
