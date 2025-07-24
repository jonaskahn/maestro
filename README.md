# Maestro 🎭 - Job orchestrator made simple for Node.js message workflows

[![npm version](https://img.shields.io/npm/v/@jonaskahn/maestro.svg?style=flat)](https://www.npmjs.com/package/@jonaskahn/maestro)
[![Unit Tests](https://github.com/jonaskahn/maestro/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/jonaskahn/maestro/actions/workflows/unit-tests.yml)
[![Codecov](https://codecov.io/gh/jonaskahn/maestro/graph/badge.svg?token=nGIqzVIyvZ)](https://codecov.io/gh/jonaskahn/maestro)
[![Lint](https://github.com/jonaskahn/maestro/actions/workflows/lint.yml/badge.svg)](https://github.com/jonaskahn/maestro/actions/workflows/lint.yml)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/jonaskahn/maestro/pulls)

## Introduction

Maestro is a lightweight, flexible library for orchestrating message-based workflows in Node.js applications. It
provides abstractions and implementations for producers, consumers, monitoring, and caching to simplify working with
message brokers like Kafka.

## Features

- 🔧 **Unified Abstractions** - Common interfaces for message brokers and caching
- 🔄 **Message Processing** - Message suppression, concurrency control, and error handling
- 💪 **Distributed Coordination** - Locks and synchronization across services
- 📊 **Intelligent Monitoring** - Backpressure detection and adaptive rate limiting
- 🛡️ **Reliability** - Graceful shutdown, connection management, and recovery
- 🔍 **Observability** - Comprehensive logging and metrics collection

## Installation

```bash
# Using npm
npm install @jonaskahn/maestro

# Using yarn
yarn add @jonaskahn/maestro
```

## Quick Start

### Producer Example

```javascript
const { DefaultProducer } = require("@jonaskahn/maestro");

class OrderProducer extends DefaultProducer {
  /**
   * Gets the next batch of items to process
   *
   * @param {Object} criteria - Query criteria for filtering orders
   * @param {number} limit - Maximum number of items to retrieve
   * @param {Array<string>} excludedIds - IDs to exclude from the query
   * @returns {Promise<Array>} List of pending orders
   */
  async getNextItems(criteria, limit, excludedIds) {
    // Query database for pending orders with filters
    const pendingOrders = await database.getPendingOrders(criteria, limit, excludedIds);
    return pendingOrders;
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
   * Connects to Kafka and initializes the database
   *
   * @returns {Promise<void>} - Resolves when connections are established
   */
  async connect() {
    await super.connect();
    await database.connect();
  }

  /**
   * Cleans up database connection
   *
   * @returns {Promise<void>} - Resolves when cleanup is complete
   */
  async cleanup() {
    await database.disconnect();
  }
}

// Create producer with configuration
const producer = new OrderProducer({
  topic: "ecommerce-orders",
  topicOptions: {
    processingTtl: 240000,
    lagThreshold: 100,
    lagMonitorInterval: 5000,
  },
});

await producer.connect();
await producer.produce({ state: 1, priority: "normal" }, 50);
```

### Consumer Example

```javascript
const { DefaultConsumer } = require("@jonaskahn/maestro");

class OrderConsumer extends DefaultConsumer {
  /**
   * Processes an order
   *
   * @param {Object} orderData - Order data to process
   * @returns {Promise<Object>} Processing result
   */
  async process(orderData) {
    // Validate and process the order
    await this.validateOrder(orderData);
    await this.processOrderSteps(orderData);
    return {
      orderId: this.getItemId(orderData),
      status: "processed",
      processedAt: new Date(),
    };
  }

  /**
   * Gets unique identifier for an item
   *
   * @param {Object} orderData - The order data object
   * @returns {string} The order's unique identifier
   */
  getItemId(orderData) {
    return orderData._id;
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
  }
}

// Create consumer with configuration
const consumer = new OrderConsumer({
  topic: "ecommerce-orders",
  topicOptions: {
    processingTtl: 240000,
    maxConcurrency: 10,
  },
});

await consumer.connect();
await consumer.consume();
```

## Documentation

- [Getting Started](./docs/guide/README.md)
- [API Reference](./docs/api/README.md)
- [Components](./docs/components/README.md)

## Examples

See the [examples](./examples/) directory for more comprehensive examples:

- [E-commerce Order Processing](./examples/ecommerce-order-processing/): Demonstrates using Maestro for processing
  e-commerce orders with Kafka

## Implementation Status

### Message Brokers

- [x] Kafka - Producer and consumer implementations

### Cache Providers

- [x] Redis - Full implementation with distributed locks
- [ ] Memcached - Coming soon
- [ ] In-memory - For testing purposes

### Monitoring

- [x] Consumer lag monitoring
- [x] System resource monitoring
- [ ] Prometheus metrics integration - Future plan
- [ ] Dashboard visualization - Future plan

## Supported Node.js Versions

- Node.js >= 18

## Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

## License

[MIT License](./LICENSE)
