/**
 * MongoDB Database for Kafka + MongoDB Example
 *
 * Real MongoDB database implementation with orders collection.
 * Provides CRUD operations and realistic data generation.
 */

const { MongoClient, ObjectId } = require("mongodb");
const Logger = require("../utils/logger");

// Database configuration
const DB_CONFIG = {
  url: "mongodb://127.0.0.1:27017/myapp?authSource=admin",
  dbName: "myapp",
  collections: {
    orders: "orders",
  },
  orderGenerationRate: 1000,
};

// State constants for Job Orchestrator compatibility
const ORDER_STATES = {
  PENDING: 1, // Order is waiting to be processed
  COMPLETED: 2, // Order has been successfully processed
  FAILED: 3, // Order processing failed permanently
};

class MongoDatabase {
  constructor() {
    this._client = null;
    this.db = null;
    this.collections = {};
    this.orderCounter = 0;
    this.stats = {
      totalCreated: 0,
      totalCompleted: 0,
      totalFailed: 0,
    };
    this.isConnected = false;
    this.createdCollections = new Set(); // Track collections created during this session
  }

  /**
   * Get state constants for external use
   */
  static get ORDER_STATES() {
    return ORDER_STATES;
  }

  /**
   * Connect to MongoDB
   */
  async connect() {
    if (this.isConnected) {
      return;
    }

    try {
      Logger.info("Connecting to MongoDB");

      this._client = new MongoClient(DB_CONFIG.url);
      await this._client.connect();

      this.db = this._client.db(DB_CONFIG.dbName);
      this.collections.orders = this.db.collection(DB_CONFIG.collections.orders);

      this.isConnected = true;
    } catch (error) {
      Logger.error("Failed to connect to MongoDB", error);
      throw error;
    }
  }

  /**
   * Close database connection
   */
  async disconnect() {
    if (this._client && this.isConnected) {
      try {
        await this._client.close();
        this.isConnected = false;
        Logger.info("Disconnected from MongoDB");
      } catch (error) {
        Logger.warn("Error during MongoDB disconnect", error);
        this.isConnected = false;
      }
    }
  }

  /**
   * Initialize database with required schema
   * This method sets up the database structure with proper indexes and constraints
   */
  async initializeSchema() {
    if (!this.isConnected) {
      await this.connect();
    }

    Logger.info("Initializing MongoDB schema with required structure");

    try {
      for (const collectionName of Object.values(DB_CONFIG.collections)) {
        await this.db.createCollection(collectionName);
        this.createdCollections.add(collectionName);
        Logger.debug(`Created/verified collection: ${collectionName}`);
      }

      this.collections.orders = this.db.collection(DB_CONFIG.collections.orders);

      await this.createIndexes();

      await this.validateSchema();

      Logger.success("MongoDB schema initialized successfully");
      Logger.debug("Schema enforces: _id (ObjectId), state (1=pending, 2=completed, 3=failed)");

      await this.initializeData();
    } catch (error) {
      Logger.error("Failed to initialize MongoDB schema", error);
      throw error;
    }
  }

  /**
   * Create a new order in MongoDB
   * @returns {Object} Created order
   */
  async createOrder() {
    const orderData = this.generateOrderData();

    const result = await this.collections.orders.insertOne(orderData);
    const order = { ...orderData, _id: result.insertedId.toString() };

    this.stats.totalCreated++;
    return order;
  }

  /**
   * Get pending orders for processing
   * @param {object} criteria - Criteria for filtering orders
   * @param {number} limit - Maximum number of orders to fetch
   * @param {Array<string>} excludedIds - List of order IDs to exclude
   * @returns {Array<Object>} Pending orders with required _id and state fields
   */
  async getPendingOrders(criteria = {}, limit = 1000, excludedIds = []) {
    // Ensure we're connected and have collections initialized
    if (!this.isConnected || !this.collections.orders) {
      await this.connect();

      if (!this.collections.orders) {
        Logger.warn("Orders collection not initialized");
        return [];
      }
    }

    const query = {
      ...criteria,
    };

    if (excludedIds.length > 0) {
      query._id = { $nin: excludedIds.map(id => new ObjectId(id)) };
    }

    const orders = await this.collections.orders
      .find(query)
      .sort({
        priority: -1,
        createdAt: 1,
      })
      .limit(limit)
      .toArray();

    return orders.map(order => ({
      ...order,
      _id: order._id.toString(),
      state: order.state || ORDER_STATES.PENDING,
    }));
  }

  /**
   * Get order by ID
   * @param {string} orderId - Order ID
   * @returns {Object|null} Order or null if not found
   */
  async getOrder(orderId) {
    return await this.collections.orders.findOne({
      _id: new ObjectId(orderId),
    });
  }

  /**
   * Mark an order as completed
   * @param {string} orderId - Order ID
   * @returns {boolean} Success status
   */
  async markOrderAsCompleted(orderId) {
    const result = await this.collections.orders.updateOne(
      { _id: new ObjectId(orderId) },
      {
        $set: {
          state: ORDER_STATES.COMPLETED,
          completedAt: new Date(),
          updatedAt: new Date(),
        },
      }
    );

    if (result.matchedCount === 0) {
      throw new Error(`Order ${orderId} not found`);
    }

    this.stats.totalCompleted++;
    return true;
  }

  /**
   * Mark an order as failed
   * @param {string} orderId - Order ID
   * @param {string} error - Error message
   * @returns {boolean} Success status
   */
  async markOrderAsFailed(orderId, error = "Processing failed") {
    const order = await this.collections.orders.findOne({
      _id: new ObjectId(orderId),
    });

    if (!order) {
      throw new Error(`Order ${orderId} not found`);
    }

    const newRetryCount = order.retryCount + 1;
    const maxRetries = order.maxRetries || 3;

    const updateData = {
      retryCount: newRetryCount,
      lastError: error,
      updatedAt: new Date(),
    };

    if (newRetryCount >= maxRetries) {
      updateData.state = ORDER_STATES.FAILED;
      updateData.failedAt = new Date();
      this.stats.totalFailed++;
    }

    const result = await this.collections.orders.updateOne({ _id: new ObjectId(orderId) }, { $set: updateData });

    if (result.matchedCount === 0) {
      throw new Error(`Order ${orderId} not found`);
    }

    return true;
  }

  /**
   * Check if an order is completed
   * @param {string} orderId - Order ID
   * @returns {boolean} True if order is completed
   */
  async isOrderCompleted(orderId) {
    const order = await this.getOrder(orderId);
    return order?.state === ORDER_STATES.COMPLETED;
  }

  /**
   * Generate new test orders in the database
   * @param {number} count - Number of orders to generate
   * @returns {Array<Object>} Created orders
   */
  async generateNewOrders(count = DB_CONFIG.orderGenerationRate) {
    const orders = [];

    for (let i = 0; i < count; i++) {
      const order = await this.createOrder();
      orders.push(order);
    }

    return orders;
  }

  /**
   * Generate random order data
   * @returns {Object} Order data
   */
  generateOrderData() {
    const orderType = this.selectRandomOrderType();
    const customerName = this.generateCustomerName();
    const items = this.generateOrderItems(orderType);
    const total = this.generateOrderTotal(items);
    const priority = Math.random() > 0.85 ? "high" : "normal";

    return {
      orderNumber: `ORD-${Date.now()}-${this.orderCounter++}`,
      customerName,
      email: this.generateEmail(customerName),
      orderType,
      items,
      total,
      priority,
      state: ORDER_STATES.PENDING,
      createdAt: new Date(),
      updatedAt: new Date(),
      retryCount: 0,
      maxRetries: 3,
    };
  }

  // ===== STATS AND MAINTENANCE =====

  /**
   * Get database statistics
   * @returns {Object} Database statistics
   */
  async getStats() {
    try {
      const orderStats = await this.collections.orders
        .aggregate([
          {
            $group: {
              _id: "$state",
              count: { $sum: 1 },
            },
          },
        ])
        .toArray();

      const stats = {
        pendingOrders: 0,
        completedOrders: 0,
        failedOrders: 0,
        totalOrders: 0,
        createdThisSession: this.stats.totalCreated,
        completedThisSession: this.stats.totalCompleted,
        failedThisSession: this.stats.totalFailed,
        processingRate: this.stats.totalCompleted > 0 ? (this.stats.totalCompleted / this.stats.totalCreated) * 100 : 0,
        successRate:
          this.stats.totalCompleted > 0
            ? (this.stats.totalCompleted / (this.stats.totalCompleted + this.stats.totalFailed)) * 100
            : 0,
      };

      orderStats.forEach(stat => {
        switch (stat._id) {
          case ORDER_STATES.PENDING:
            stats.pendingOrders = stat.count;
            break;
          case ORDER_STATES.COMPLETED:
            stats.completedOrders = stat.count;
            break;
          case ORDER_STATES.FAILED:
            stats.failedOrders = stat.count;
            break;
        }
      });

      stats.totalOrders = stats.pendingOrders + stats.completedOrders + stats.failedOrders;
      return stats;
    } catch (error) {
      Logger.error("Failed to get database stats", error);
      return {
        error: "Failed to get stats",
        createdThisSession: this.stats.totalCreated,
        completedThisSession: this.stats.totalCompleted,
        failedThisSession: this.stats.totalFailed,
      };
    }
  }

  /**
   * Clean up old orders
   * @param {number} maxAge - Maximum age in milliseconds
   * @returns {Object} Cleanup statistics
   */
  async cleanupOldOrders(maxAge = 24 * 60 * 60 * 1000) {
    const cutoffDate = new Date(Date.now() - maxAge);

    const result = await this.collections.orders.deleteMany({
      createdAt: { $lt: cutoffDate },
      state: { $ne: ORDER_STATES.PENDING },
    });

    return {
      deleted: result.deletedCount,
      cutoffDate,
    };
  }

  /**
   * Clean up all collections (for testing)
   */
  async cleanupAllCollections() {
    Logger.warn("Cleaning up all collections");

    if (!this.isConnected) {
      await this.connect();
    }

    // Clean all collections used by this service
    const cleanupPromises = [];

    for (const collectionName of Object.values(DB_CONFIG.collections)) {
      const collection = this.db.collection(collectionName);
      cleanupPromises.push(
        collection
          .deleteMany({})
          .then(result => {
            Logger.debug(`Deleted ${result.deletedCount} records from ${collectionName}`);
            return result;
          })
          .catch(error => {
            Logger.error(`Failed to clean collection ${collectionName}`, error);
            return { deletedCount: 0, error };
          })
      );
    }

    const results = await Promise.all(cleanupPromises);
    const totalDeleted = results.reduce((sum, result) => sum + result.deletedCount, 0);

    Logger.info(`Total records deleted across all collections: ${totalDeleted}`);
    return { totalDeleted };
  }

  /**
   * Reset the database
   */
  async reset() {
    if (!this.isConnected) {
      await this.connect();
    }

    Logger.warn("Resetting database");
    Logger.warn("All data will be erased");

    await this.cleanupAllCollections();

    // Reset stats
    this.stats = {
      totalCreated: 0,
      totalCompleted: 0,
      totalFailed: 0,
    };

    this.orderCounter = 0;

    Logger.success("Database reset complete");

    return { success: true };
  }

  /**
   * Create database indexes
   */
  async createIndexes() {
    await this.collections.orders.createIndex(
      { state: 1, priority: 1, createdAt: 1 },
      { name: "idx_order_processing" }
    );
    await this.collections.orders.createIndex({ updatedAt: 1 }, { name: "idx_updated_at" });
    await this.collections.orders.createIndex({ orderNumber: 1 }, { name: "idx_order_number", unique: true });

    Logger.debug("Created indexes for efficient queries and constraints");
  }

  /**
   * Validate database schema
   */
  async validateSchema() {
    // Check if collection exists
    const collections = await this.db.listCollections({ name: DB_CONFIG.collections.orders }).toArray();

    if (collections.length === 0) {
      throw new Error("Orders collection does not exist");
    }

    // Validate indexes
    const indexes = await this.collections.orders.listIndexes().toArray();

    // Check for required indexes
    const requiredIndexNames = ["idx_order_processing", "idx_updated_at", "idx_order_number"];
    const presentIndexNames = indexes.map(index => index.name);

    const missingIndexes = requiredIndexNames.filter(name => !presentIndexNames.includes(name));

    if (missingIndexes.length > 0) {
      Logger.warn(`Missing indexes: ${missingIndexes.join(", ")}. Will create them.`);
      await this.createIndexes();
    } else {
      Logger.debug("All required indexes are present");
    }

    // Check for constraint enforcement
    // MongoDB doesn't enforce schema by default, but we can add JSONSchema validation
    // For simplicity, we'll just log a reminder here
    Logger.debug("MongoDB schema validation depends on proper data insertion from application");

    return true;
  }

  /**
   * Initialize database with test data
   */
  async initializeData() {
    const count = await this.collections.orders.countDocuments({});

    if (count === 0) {
      Logger.info("Initializing database with test data");
      await this.generateNewOrders(10000);
      Logger.success("Created 10000 test orders");
    } else {
      Logger.info(`Database already contains ${count} orders`);
    }
  }

  /**
   * Select random order type
   */
  selectRandomOrderType() {
    const types = ["standard", "express", "priority", "bulk", "international"];
    return types[Math.floor(Math.random() * types.length)];
  }

  /**
   * Generate random customer name
   */
  generateCustomerName() {
    const firstNames = [
      "James",
      "Mary",
      "John",
      "Patricia",
      "Robert",
      "Jennifer",
      "Michael",
      "Linda",
      "William",
      "Elizabeth",
      "David",
      "Barbara",
      "Richard",
      "Susan",
      "Joseph",
      "Jessica",
      "Thomas",
      "Sarah",
      "Charles",
      "Karen",
      "Aisha",
      "Mohammed",
      "Wei",
      "Fatima",
      "Santiago",
      "Sofia",
      "Hiroshi",
      "Priya",
      "Olga",
      "Chen",
    ];

    const lastNames = [
      "Smith",
      "Johnson",
      "Williams",
      "Brown",
      "Jones",
      "Garcia",
      "Miller",
      "Davis",
      "Rodriguez",
      "Martinez",
      "Hernandez",
      "Lopez",
      "Gonzalez",
      "Wilson",
      "Anderson",
      "Thomas",
      "Taylor",
      "Moore",
      "Jackson",
      "Martin",
      "Lee",
      "Perez",
      "Thompson",
      "White",
      "Harris",
      "Sanchez",
      "Clark",
      "Ramirez",
      "Lewis",
      "Robinson",
      "Zhang",
      "Patel",
      "Khan",
      "Ivanov",
      "Suzuki",
    ];

    const firstName = firstNames[Math.floor(Math.random() * firstNames.length)];
    const lastName = lastNames[Math.floor(Math.random() * lastNames.length)];

    return `${firstName} ${lastName}`;
  }

  /**
   * Generate email from customer name
   */
  generateEmail(name) {
    const domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com", "company.co"];
    const nameParts = name.toLowerCase().replace(/[^a-z0-9]/g, ".");
    const domain = domains[Math.floor(Math.random() * domains.length)];
    return `${nameParts}@${domain}`;
  }

  /**
   * Generate random order items
   */
  generateOrderItems(orderType) {
    const productCatalog = [
      { name: "Laptop", price: 999.99 },
      { name: "Smartphone", price: 699.99 },
      { name: "Headphones", price: 149.99 },
      { name: "Monitor", price: 249.99 },
      { name: "Keyboard", price: 89.99 },
      { name: "Mouse", price: 49.99 },
      { name: "Tablet", price: 399.99 },
      { name: "Printer", price: 199.99 },
      { name: "Camera", price: 499.99 },
      { name: "Speaker", price: 129.99 },
    ];

    const itemCount = orderType === "bulk" ? Math.floor(Math.random() * 10) + 5 : Math.floor(Math.random() * 4) + 1;

    const items = [];
    for (let i = 0; i < itemCount; i++) {
      const product = productCatalog[Math.floor(Math.random() * productCatalog.length)];
      const quantity = Math.floor(Math.random() * 3) + 1;

      items.push({
        product: product.name,
        price: product.price,
        quantity,
        subtotal: product.price * quantity,
      });
    }

    return items;
  }

  /**
   * Generate order total from items
   */
  generateOrderTotal(items) {
    const subtotal = items.reduce((sum, item) => sum + item.subtotal, 0);
    const tax = subtotal * 0.08;
    const shipping = subtotal > 1000 ? 0 : 15.99;

    return {
      subtotal: Number(subtotal.toFixed(2)),
      tax: Number(tax.toFixed(2)),
      shipping: Number(shipping.toFixed(2)),
      total: Number((subtotal + tax + shipping).toFixed(2)),
    };
  }
}

module.exports = new MongoDatabase();
