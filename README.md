# 🎭 Maestro

A powerful message queue abstraction framework providing unified interfaces for producers, consumers, and cache layers
across different message brokers.

## 📋 Overview

Maestro provides a clean abstraction over different message queue implementations, allowing you to write message
processing code once and switch underlying infrastructure with minimal changes. It offers common patterns for producers,
consumers, and caching while handling the complexities of each broker implementation.

## ✨ Features

- **🔄 Multi-broker support**: Write once, run anywhere with support for:
  - Kafka (implemented)
  - RabbitMQ (planned)
  - BullMQ (planned)

- **📦 Caching layer abstractions**:
  - Redis (implemented)
  - Memcached (planned)
  - In-memory cache (planned)

- **🛠️ Advanced features**:
  - Message suppression (cooldown period)
  - Distributed locking
  - Graceful shutdown handling
  - Backpressure monitoring
  - Message tracking and metrics
  - Concurrency management
  - Standardized error handling

## 🏗️ Architecture

Maestro is built on a solid foundation of abstract classes that define the core interfaces:

- `AbstractProducer`: Base class for all message producers
- `AbstractConsumer`: Base class for all message consumers
- `AbstractCache`: Base class for all caching providers
- `AbstractMonitorService`: Base class for backpressure monitoring services

Concrete implementations extend these abstractions to provide broker-specific functionality while maintaining a
consistent API.

## 📊 Flow Diagrams

### Overview Flow

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Your App      │     │    Maestro      │     │  Infrastructure │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  YourProducer   │────▶│  KafkaProducer  │────▶│  Kafka Broker   │
│  YourConsumer   │◀───▶│  KafkaConsumer  │◀───▶│  Redis Cache    │
│                 │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Producer Flow

```
┌──────────┐     ┌─────────────┐     ┌───────────────┐     ┌───────────┐     ┌───────────┐     ┌───────────┐
│          │     │             │     │               │     │           │     │           │     │           │
│   Data   │────▶│ Check Cache │────▶│  Fetch Items  │────▶│  Create   │────▶│ Mark As   │────▶│   Send    │
│  Source  │     │ Suppression │     │  (Excluding   │     │ Messages  │     │Suppressed │     │ to Broker │
│          │     │ & Processing│     │ Suppressed ID)│     │           │     │           │     │           │
└──────────┘     └─────────────┘     └───────────────┘     └───────────┘     └───────────┘     └───────────┘
                       │
                       │ Build exclusion list
                       ▼
                 ┌───────────────┐
                 │               │
                 │ Excluded IDs  │
                 │ - Suppressed  │
                 │ - Processing  │
                 └───────────────┘
```

### Consumer Flow

```
┌───────────┐     ┌───────────┐     ┌───────────┐     ┌───────────┐
│           │     │           │     │           │     │           │
│  Message  │────▶│  Receive  │────▶│  Process  │────▶│  Update   │
│  Broker   │     │ Messages  │     │ Business  │     │  State    │
│           │     │           │     │  Logic    │     │           │
└───────────┘     └───────────┘     └───────────┘     └───────────┘
                       │                                    │
                       │                                    │
                       ▼                                    ▼
                 ┌───────────┐                       ┌───────────┐
                 │           │                       │           │
                 │ Processed │                       │ Cache Key │
                 │  Check    │                       │ Updates   │
                 │           │                       │           │
                 └───────────┘                       └───────────┘
```

### Detailed Producer Flow

<details>
<summary>Click to expand</summary>

```
┌───────┐     ┌────────────┐     ┌────────────┐     ┌────────────────┐     ┌──────────────┐
│ START │────▶│ Connection │────▶│ Check      │────▶│ Is Under      │────▶│ Get          │
│       │     │ Check      │     │ Backpressure│     │ Pressure?     │     │ Suppressed   │
└───────┘     └────────────┘     └────────────┘     └────────────────┘     │ IDs          │
                    │                                       │              └──────┬───────┘
                    │                                       │ Yes                 │
                    ▼                                       ▼                     ▼
             ┌────────────┐                         ┌────────────────┐     ┌──────────────┐
             │ Connect()  │                         │ Return Empty   │     │ Get          │
             └──────┬─────┘                         │ Result         │     │ Processing   │
                    │                               └────────────────┘     │ IDs          │
                    └───────────────────────────────────────────────────▶ └──────┬───────┘
                                                                                 │
                                                                                 ▼
┌────────────────┐     ┌────────────────┐     ┌────────────────┐           ┌──────────────┐
│ Return Empty   │◀────│ Items Found?   │◀────│ getNextItems() │◀──────────│ Build        │
│ Result         │     │                │     │ with excludedIds│           │ ExcludedIDs  │
└────────────────┘     └────────┬───────┘     └────────────────┘           └──────────────┘
       ▲                        │ Yes
       │                        ▼
       │              ┌────────────────┐     ┌────────────────┐     ┌────────────────┐
       │              │ Acquire        │────▶│ Lock Acquired? │────▶│ Mark Items     │
       │              │ Distributed    │     │                │     │ as Suppressed  │
       │              │ Lock           │     └────────┬───────┘     └───────┬────────┘
       │              └────────────────┘              │ No                  │
       │                      ▲                       ▼                     ▼
       │                      │             ┌────────────────┐     ┌────────────────┐
       │                      │             │ Handle Lock    │     │ Create Broker  │
       │                      │             │ Failure        │     │ Messages       │
       │                      │             └────────────────┘     └───────┬────────┘
       │                      │                                           │
       │                      │                                           ▼
       │                      │                                  ┌────────────────┐
       │                      │                                  │ Send Messages  │
       │                      │                                  │ to Broker      │
       │                      │                                  └───────┬────────┘
       │                      │                                          │
       └──────────────────────┴──────────────────────────────────────────┘
```

</details>

### Detailed Consumer Flow

<details>
<summary>Click to expand</summary>

```
┌───────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐
│ START │────▶│ Connection │────▶│ Configure  │────▶│ Start      │────▶│ Wait for   │
│       │     │ Check      │     │ Consumption│     │ Consuming  │     │ Message    │
└───────┘     └────────────┘     └────────────┘     └────────────┘     └─────┬──────┘
                    │                                                        │
                    ▼                                                        │
             ┌────────────┐                                                  │
             │ Connect()  │◀─────────────────────────────────────────────────┘
             └────────────┘                                                  │
                                                                             ▼
┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐
│ Return to  │◀────│ Remove     │◀────│ Mark as    │◀────│ Processing │◀────│ Message    │
│ Wait       │     │ Processing │     │ Completed  │     │ Successful?│     │ Received   │
└────────────┘     │ Mark      │     └────────────┘     └─────┬──────┘     └────────────┘
                   └────────────┘           ▲                  │ No               │
                                           │                   ▼                  ▼
                                           │          ┌────────────┐       ┌────────────┐
                                           └──────────│ Mark as    │       │ Extract ID │
                                                      │ Failed     │       │            │
                                                      └────────────┘       └─────┬──────┘
                                                                                 │
                               ┌────────────┐     ┌────────────┐     ┌────────────┐
                               │ process()  │◀────│ Mark as    │◀────│ Already    │
                               │ Business   │     │ Processing │     │ Processed? │
                               │ Logic      │     └────────────┘     └─────┬──────┘
                               └─────┬──────┘                              │ Yes
                                     │                                     ▼
                                     │                              ┌────────────┐
                                     └─────────────────────────────▶│ Skip       │
                                                                    │ Message    │
                                                                    └────────────┘
```

</details>

## 🚀 Getting Started

### 📥 Installation

```bash
npm install @jonaskahn/maestro
```

### 🔰 Implementation Guide

To start using Maestro in your own project, follow these steps:

#### 1. Create a Producer

Extend the appropriate base producer class for your broker:

```javascript
const { KafkaProducer } = require("@jonaskahn/maestro");

class MyProducer extends KafkaProducer {
  constructor() {
    super({
      topic: "my-topic",
      brokerOptions: {
        clientOptions: {
          brokers: ["kafka:9092"],
        },
      },
      cacheOptions: {
        keyPrefix: "my-app",
      },
    });
  }

  // Optional: Override methods for custom behavior
  getNextItems(criteria, limit, excludedIds) {
    // Your logic to fetch items to be produced
    return fetchItemsFromDatabase(criteria, limit);
  }

  getItemId(item) {
    return item.uniqueId; // Identify your items
  }
}
```

#### 2. Create a Consumer

Extend the appropriate base consumer class for your broker:

```javascript
const { KafkaConsumer } = require("@jonaskahn/maestro");

class MyConsumer extends KafkaConsumer {
  constructor() {
    super({
      topic: "my-topic",
      maxConcurrency: 5,
      brokerOptions: {
        clientOptions: {
          brokers: ["kafka:9092"],
        },
        consumerOptions: {
          groupId: "my-service",
          fromBeginning: false,
        },
      },
      cacheOptions: {
        keyPrefix: "my-app",
      },
    });
  }

  // Implement your business logic
  async process(message) {
    console.log(`Processing message: ${JSON.stringify(message)}`);
    // Your message handling logic here
    return true;
  }
}
```

#### 3. Run Your Application

```javascript
async function main() {
  // Producer example
  const producer = new MyProducer();
  await producer.connect();
  await producer.produce({ status: "pending" }, 10);

  // Consumer example
  const consumer = new MyConsumer();
  await consumer.connect();
  await consumer.consume();

  // The consumer will continue running until explicitly stopped
}

main().catch(console.error);
```

## 🛒 Example Application

The repository includes an e-commerce order processing example that demonstrates how to use Maestro in a real-world
scenario:

### 🧩 Components

#### 📤 Producer Implementation

The `OrderProducer` extends `KafkaProducer` and adds:

- Database integration to fetch pending orders
- Order state management (pending, sent, failed)
- Statistics tracking
- Custom success/failure handling

```javascript
class OrderProducer extends KafkaProducer {
  // Configure Kafka and Redis connections
  constructor(_config = {}) {
    super({
      topic: "ecommerce-orders",
      brokerOptions: { clientOptions: { brokers: ["localhost:9092"] } },
      cacheOptions: { keyPrefix: "ECOMMERCE" },
    });
  }

  // Fetch pending orders from MongoDB
  async getNextItems(criteria, limit, excludedIds) {
    const pendingOrders = await this.getPendingOrders(
      {
        state: ORDER_STATES.PENDING,
        ...criteria,
      },
      limit,
      excludedIds
    );

    return pendingOrders;
  }

  // Update database when message is successfully sent
  async _onItemProcessSuccess(orderId) {
    await database.collections.orders.updateOne({ _id: orderId }, { $set: { status: "sent", sentAt: new Date() } });
  }
}
```

#### 📥 Consumer Implementation

The `OrderConsumer` extends `KafkaConsumer` and adds:

- Order validation and processing logic
- Database updates for order status
- Custom message handling
- Processing statistics

```javascript
class OrderConsumer extends KafkaConsumer {
  constructor(_config = {}) {
    super({
      topic: "ecommerce-orders",
      maxConcurrency: 10,
      brokerOptions: { clientOptions: { brokers: ["localhost:9092"] } },
      cacheOptions: { keyPrefix: "ECOMMERCE" },
    });
  }

  // Business logic for order processing
  async process(orderData) {
    await this.validateOrder(orderData);
    await this.processOrderSteps(orderData);
    return this.createProcessingResult(orderData);
  }

  // Mark order as completed in the database
  async _onItemProcessSuccess(itemId) {
    await database.markOrderAsCompleted(itemId);
  }
}
```

### 🏃‍♂️ Running the Example

The example includes a Docker Compose file with Kafka, Zookeeper, Redis, and MongoDB:

```bash
cd examples/ecommerce-order-processing
npm install
docker-compose up -d
npm run producer  # In one terminal
npm run consumer  # In another terminal
```

## 📜 License

MIT License

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

**Made by Jonas Kahn with 💖**
