/**
 * Order Consumer for Ecommerce Example
 *
 * Processes orders from Kafka and updates their status in the database.
 */

const { DefaultConsumer } = require("../../../../src");
const database = require("../database/mongodb-client");
const Logger = require("../utils/logger");

const SIMULATION_DELAYS = {
  INVENTORY_CHECK: 100,
  PAYMENT_PROCESSING: 60_000,
  ORDER_VALIDATION: 120_000,
};

class OrderConsumer extends DefaultConsumer {
  constructor(config = {}) {
    super({
      topic: config.topic || "ecommerce-orders",
      maxConcurrency: config.maxConcurrency || 10,
      cacheOptions: {
        keyPrefix: config.keyPrefix || "ECOMMERCE",
        processingTtl: config.processingTtl || 240_000,
      },
    });

    this.stats = {
      processedCount: 0,
      failedCount: 0,
      startTime: Date.now(),
    };
  }

  /**
   * Process an order
   * @param {Object} orderData - Order data to process
   * @returns {Promise<Object>} Processing result
   */
  async process(orderData) {
    const orderId = this.extractOrderId(orderData);

    try {
      Logger.info(`Processing order ${orderId}`);

      await this.validateOrder(orderData);
      await this.processOrderSteps(orderData);

      const result = this.createProcessingResult(orderData);
      this.updateSuccessStats();

      Logger.success(`Order ${orderId} processed successfully`);
      return result;
    } catch (error) {
      this.handleProcessingError(orderId, error);
      throw error;
    }
  }

  getItemId(orderData) {
    return orderData?._id;
  }

  async _isItemProcessed(itemId) {
    try {
      return await database.isOrderCompleted(itemId);
    } catch (error) {
      Logger.error(`Error checking order completion for ${itemId}`, error);
      return false;
    }
  }

  async _onItemProcessSuccess(itemId) {
    try {
      await database.markOrderAsCompleted(itemId);
      this.updateSuccessStats();
      Logger.success(`Order ${itemId} marked as completed`);
    } catch (error) {
      Logger.error(`Error updating order ${itemId}`, error);
    }
  }

  async _onItemProcessFailed(itemId, error) {
    try {
      await database.markOrderAsFailed(itemId, error?.message);
      Logger.warn(`Order ${itemId} marked as failed: ${error?.message}`);
    } catch (dbError) {
      Logger.error(`Error marking order ${itemId} as failed`, dbError);
    }
  }

  extractOrderId(order) {
    return order.orderId || order._id?.toString();
  }

  async validateOrder(order) {
    if (!order.orderId && !order._id) {
      throw new Error("Order must have an ID");
    }

    if (!order.items || order.items.length === 0) {
      throw new Error("Order must have items");
    }
  }

  async processOrderSteps(order) {
    await this.simulateProcessingDelay();
    await this.updateOrderStatus(order, "processed");
  }

  async updateOrderStatus(order, status) {
    if (status === "completed" || status === "processed") {
      await database.markOrderAsCompleted(order._id);
    } else if (status === "failed") {
      await database.markOrderAsFailed(order._id, "Processing failed");
    }
  }

  simulateProcessingDelay() {
    return new Promise(resolve => setTimeout(resolve, SIMULATION_DELAYS.ORDER_VALIDATION));
  }

  createProcessingResult(order) {
    return {
      orderId: this.extractOrderId(order),
      status: "processed",
      processedAt: new Date(),
      total: order.total,
      items: order.items?.length || 0,
    };
  }

  updateSuccessStats() {
    this.stats.processedCount++;
  }

  handleProcessingError(orderId, error) {
    this.stats.failedCount++;
    Logger.error(`Order processing failed for ${orderId}`, error);
  }

  /**
   * Get processing statistics
   * @returns {Object} Statistics about processed orders
   */
  getStats() {
    const uptime = Date.now() - this.stats.startTime;
    const total = this.stats.processedCount + this.stats.failedCount;
    const successRate = total > 0 ? ((this.stats.processedCount / total) * 100).toFixed(2) : 0;

    return {
      processedCount: this.stats.processedCount,
      failedCount: this.stats.failedCount,
      total,
      successRate: parseFloat(successRate),
      uptime: Math.round(uptime / 1000),
    };
  }
}

module.exports = OrderConsumer;
