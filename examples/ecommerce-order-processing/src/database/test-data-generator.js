/**
 * Test Data Generator
 *
 * Provides utilities for generating realistic test order data for the ecommerce application.
 * Creates orders with random but realistic properties for testing and development.
 */

const { ObjectId } = require("mongodb");
const OrderStateMapper = require("./order-state-mapper");

const ORDER_TYPES = [
  { type: "standard", weight: 0.6, processingTime: 2000 },
  { type: "express", weight: 0.25, processingTime: 1000 },
  { type: "priority", weight: 0.1, processingTime: 500 },
  { type: "bulk", weight: 0.05, processingTime: 5000 },
];

const CUSTOMER_NAMES = {
  FIRST_NAMES: ["John", "Jane", "Michael", "Sarah", "David", "Emma", "James", "Lisa", "Robert", "Mary"],
  LAST_NAMES: ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"],
};

const PRODUCT_CATALOG = [
  {
    id: "LAPTOP-001",
    name: 'Laptop Pro 15"',
    price: 1299.99,
    category: "electronics",
  },
  {
    id: "PHONE-001",
    name: "Smartphone X",
    price: 899.99,
    category: "electronics",
  },
  {
    id: "HEADPHONES-001",
    name: "Wireless Headphones",
    price: 199.99,
    category: "electronics",
  },
  {
    id: "KEYBOARD-001",
    name: "Mechanical Keyboard",
    price: 149.99,
    category: "accessories",
  },
  {
    id: "MOUSE-001",
    name: "Gaming Mouse",
    price: 79.99,
    category: "accessories",
  },
  {
    id: "MONITOR-001",
    name: '27" 4K Monitor',
    price: 499.99,
    category: "electronics",
  },
  {
    id: "DESK-001",
    name: "Standing Desk",
    price: 599.99,
    category: "furniture",
  },
  {
    id: "CHAIR-001",
    name: "Ergonomic Chair",
    price: 399.99,
    category: "furniture",
  },
  { id: "BOOK-001", name: "Clean Code Book", price: 49.99, category: "books" },
  {
    id: "COFFEE-001",
    name: "Premium Coffee Beans",
    price: 24.99,
    category: "consumables",
  },
];

/**
 * TestDataGenerator class
 *
 * Generates realistic test data for the ecommerce order processing system.
 */
class TestDataGenerator {
  /**
   * Creates a new test data generator
   */
  constructor() {
    this.orderCounter = 0;
  }

  /**
   * Generates a single random order
   *
   * @returns {Object} A randomly generated order
   */
  generateOrder() {
    const orderId = this.generateOrderId();
    const orderType = this.selectOrderType();
    const customer = this.generateCustomer();
    const items = this.generateOrderItems();
    const total = this.calculateTotal(items);

    return {
      _id: new ObjectId(),
      orderId,
      state: OrderStateMapper.toNumericState("pending"),
      type: orderType.type,
      URL: `/api/orders/${orderId}`,
      customer,
      items,
      total,
      priority: this.determinePriority(orderType),
      createdAt: new Date(),
      updatedAt: new Date(),
      processingTime: orderType.processingTime,
      retryCount: 0,
      maxRetries: 3,
    };
  }

  /**
   * Generates a batch of random orders
   *
   * @param {number} count - Number of orders to generate
   * @returns {Array<Object>} Array of generated orders
   */
  generateBatch(count) {
    const orders = [];
    for (let i = 0; i < count; i++) {
      orders.push(this.generateOrder());
    }
    return orders;
  }

  /**
   * Generates a unique order ID
   *
   * @returns {string} Unique order ID
   */
  generateOrderId() {
    return `ORD-${Date.now()}-${++this.orderCounter}`;
  }

  /**
   * Generates a random customer
   *
   * @returns {Object} Customer object with id, name, and email
   */
  generateCustomer() {
    const firstName = this.selectRandom(CUSTOMER_NAMES.FIRST_NAMES);
    const lastName = this.selectRandom(CUSTOMER_NAMES.LAST_NAMES);

    return {
      id: `CUST-${Math.floor(Math.random() * 10000)}`,
      name: `${firstName} ${lastName}`,
      email: this.generateEmail(firstName, lastName),
    };
  }

  /**
   * Generates random order items
   *
   * @returns {Array<Object>} Array of order items
   */
  generateOrderItems() {
    const itemCount = Math.floor(Math.random() * 4) + 1;
    const items = [];
    const selectedProducts = new Set();

    for (let i = 0; i < itemCount; i++) {
      let product;
      do {
        product = this.selectRandom(PRODUCT_CATALOG);
      } while (selectedProducts.has(product.id));

      selectedProducts.add(product.id);

      items.push({
        productId: product.id,
        name: product.name,
        category: product.category,
        price: product.price,
        quantity: Math.floor(Math.random() * 3) + 1,
      });
    }

    return items;
  }

  /**
   * Selects a random order type based on weighted distribution
   *
   * @returns {Object} Selected order type
   */
  selectOrderType() {
    const random = Math.random();
    let cumulativeWeight = 0;

    for (const orderType of ORDER_TYPES) {
      cumulativeWeight += orderType.weight;
      if (random < cumulativeWeight) {
        return orderType;
      }
    }

    return ORDER_TYPES[0];
  }

  /**
   * Generates a random email for a customer
   *
   * @param {string} firstName - Customer's first name
   * @param {string} lastName - Customer's last name
   * @returns {string} Generated email address
   */
  generateEmail(firstName, lastName) {
    const domains = ["gmail.com", "yahoo.com", "outlook.com", "company.com"];
    const domain = this.selectRandom(domains);
    return `${firstName.toLowerCase()}.${lastName.toLowerCase()}@${domain}`;
  }

  /**
   * Calculates the total price of an order
   *
   * @param {Array<Object>} items - Order items
   * @returns {number} Total price
   */
  calculateTotal(items) {
    return items.reduce((total, item) => {
      return total + item.price * item.quantity;
    }, 0);
  }

  /**
   * Determines the priority of an order based on its type
   *
   * @param {Object} orderType - Order type object
   * @returns {string} Priority level (high or normal)
   */
  determinePriority(orderType) {
    return orderType.type === "priority" ? "high" : "normal";
  }

  /**
   * Selects a random element from an array
   *
   * @param {Array} array - Array to select from
   * @returns {*} Randomly selected element
   */
  selectRandom(array) {
    return array[Math.floor(Math.random() * array.length)];
  }
}

module.exports = TestDataGenerator;
