# Kafka Environment Configuration

This document provides details on all environment variables used within the Kafka implementation of the message broker
abstraction.

## Constructor Configuration vs. Environment Variables

Kafka components can be configured in two ways:

1. **Constructor Parameters**: Passed directly to the consumer/producer constructor
2. **Environment Variables**: Set in the environment and used as defaults

The constructor parameters take precedence over environment variables. If a value is not provided in the constructor,
the system will fall back to environment variables, and finally to hardcoded defaults.

### Example Constructor Configuration

```javascript
// Producer example from ecommerce-order-processing
const producer = new OrderProducer({
  topic: "ecommerce-orders", // Required: Topic name
  topicOptions: {
    processingTtl: 3_000, // Override default TTL
    lagThreshold: 100, // Set backpressure threshold
    lagMonitorInterval: 5_000, // Set monitor check interval
  },
});

// Consumer example from ecommerce-order-processing
const consumer = new OrderConsumer({
  topic: "ecommerce-orders", // Required: Topic name
  topicOptions: {
    processingTtl: 3_000, // Override default TTL
    maxConcurrency: 10, // Set concurrent message processing
  },
});
```

### Standardized Configuration

After standardization, the configurations become:

#### Consumer Configuration

```json
{
  "topic": "ecommerce-orders",
  "topicOptions": {
    "partitions": 3,
    "replicationFactor": 1,
    "allowAutoTopicCreation": true,
    "keyPrefix": "ECOMMERCE-ORDERS",
    // Automatically uppercase topic name
    "processingTtl": 3000,
    "suppressionTtl": 9000
    // 3x processingTtl
  },
  "groupId": "ecommerce-orders-processors",
  // ${topic}-processors
  "cacheOptions": {
    "keyPrefix": "ECOMMERCE-ORDERS",
    "processingTtl": 3000,
    "suppressionTtl": 9000
  },
  "clientOptions": {
    "clientId": "ecommerce-orders-client-1752486095172",
    "brokers": ["localhost:9092"],
    "connectionTimeout": 1000,
    "requestTimeout": 30000,
    "enforceRequestTimeout": true,
    "retry": {
      "initialRetryTime": 100,
      "retries": 10,
      "factor": 0.2,
      "multiplier": 2,
      "maxRetryTime": 30000
    }
  },
  "maxConcurrency": 10,
  "eachBatchAutoResolve": true,
  "autoCommit": false,
  // Note: Default is false, not true
  "autoCommitInterval": 5000,
  "autoCommitThreshold": 100,
  "consumerOptions": {
    "groupId": "ecommerce-orders-processors",
    "sessionTimeout": 30000,
    "rebalanceTimeout": 150000,
    // 5x sessionTimeout
    "heartbeatInterval": 3000,
    // sessionTimeout/10
    "maxBytesPerPartition": 1048576,
    "minBytes": 1,
    "maxBytes": 10485760,
    "maxWaitTimeInMs": 5000,
    "fromBeginning": true,
    "maxInFlightRequests": null,
    "retry": {
      "initialRetryTime": 300,
      "retries": 5,
      "factor": 0.2,
      "multiplier": 2,
      "maxRetryTime": 30000
    }
  }
}
```

#### Producer Configuration

```json
{
  "topic": "ecommerce-orders",
  "topicOptions": {
    "partitions": 3,
    "replicationFactor": 1,
    "allowAutoTopicCreation": true,
    "keyPrefix": "ECOMMERCE-ORDERS",
    "processingTtl": 240000,
    "suppressionTtl": 720000
    // 3x processingTtl
  },
  "groupId": "ecommerce-orders-processors",
  "cacheOptions": {
    "keyPrefix": "ECOMMERCE-ORDERS",
    "processingTtl": 240000,
    "suppressionTtl": 720000
  },
  "clientOptions": {
    "clientId": "ecommerce-orders-client-1752486155921",
    "brokers": ["localhost:9092"],
    "connectionTimeout": 1000,
    "requestTimeout": 30000,
    "enforceRequestTimeout": true,
    "retry": {
      "initialRetryTime": 100,
      "retries": 10,
      "factor": 0.2,
      "multiplier": 2,
      "maxRetryTime": 30000
    }
  },
  "producerOptions": {
    "acks": -1,
    "timeout": 30000,
    "compression": 1,
    // 1 = GZIP
    "idempotent": false,
    "maxInFlightRequests": 5,
    "transactionTimeout": 60000,
    "retries": 10,
    "retry": {
      "initialRetryTime": 300,
      "retries": 5,
      "factor": 0.2,
      "multiplier": 2,
      "maxRetryTime": 30000
    }
  },
  "lagThreshold": 100,
  "lagMonitorInterval": 5000,
  "useSuppression": true,
  "useDistributedLock": true
}
```

## Client Configuration

| Environment Variable                   | Default                         | Required | Description                                                                             | Constructor Equivalent                               |
| -------------------------------------- | ------------------------------- | -------- | --------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| `MO_KAFKA_CLIENT_ID`                   | `job-orchestrator-${timestamp}` | No       | Client ID for Kafka connections (after standardization: `${topic}-client-${timestamp}`) | `brokerOptions.clientOptions.clientId`               |
| `MO_KAFKA_BROKERS`                     | `localhost:9092`                | No       | Comma-separated list of Kafka broker addresses                                          | `brokerOptions.clientOptions.brokers`                |
| `MO_KAFKA_ENFORCE_REQUEST_TIMEOUT`     | `true`                          | No       | Whether to enforce request timeout                                                      | `brokerOptions.clientOptions.enforceRequestTimeout`  |
| `MO_KAFKA_INITIAL_RETRY_TIME_MS`       | `100`                           | No       | Initial retry time in milliseconds                                                      | `brokerOptions.clientOptions.retry.initialRetryTime` |
| `MO_KAFKA_RETRY_COUNT`                 | `10`                            | No       | Number of retries for client operations                                                 | `brokerOptions.clientOptions.retry.retries`          |
| `MO_KAFKA_MAX_RETRY_TIME_MS`           | `30000`                         | No       | Maximum retry time in milliseconds                                                      | `brokerOptions.clientOptions.retry.maxRetryTime`     |
| `MO_NETWORK_OPERATION_BASE_TIMEOUT_MS` | `5000`                          | No       | Base timeout for network operations (used for derived values)                           | Not directly configurable via constructor            |
| `MO_KAFKA_CONNECTION_TIMEOUT_MS`       | `1000` (derived)                | No       | Connection timeout in milliseconds                                                      | `brokerOptions.clientOptions.connectionTimeout`      |
| `MO_KAFKA_REQUEST_TIMEOUT_MS`          | `30000` (derived)               | No       | Request timeout in milliseconds                                                         | `brokerOptions.clientOptions.requestTimeout`         |

## Producer Configuration

| Environment Variable                       | Default              | Required | Description                                           | Constructor Equivalent                                    |
| ------------------------------------------ | -------------------- | -------- | ----------------------------------------------------- | --------------------------------------------------------- |
| `MO_KAFKA_PRODUCER_ACKS`                   | `-1`                 | No       | Acknowledgment level (`-1`=all, `0`=none, `1`=leader) | `brokerOptions.producerOptions.acks`                      |
| `MO_KAFKA_PRODUCER_TIMEOUT`                | Request timeout      | No       | Producer request timeout in milliseconds              | `brokerOptions.producerOptions.timeout`                   |
| `MO_KAFKA_COMPRESSION_TYPE`                | `gzip`               | No       | Compression type (`gzip`, `lz4`, `zstd`, `none`)      | `brokerOptions.producerOptions.compression`               |
| `MO_KAFKA_PRODUCER_IDEMPOTENT`             | `false`              | No       | Whether to enable idempotent production               | `brokerOptions.producerOptions.idempotent`                |
| `MO_KAFKA_PRODUCER_MAX_IN_FLIGHT_REQUESTS` | `5`                  | No       | Maximum number of in-flight requests                  | `brokerOptions.producerOptions.maxInFlightRequests`       |
| `MO_KAFKA_PRODUCER_TRANSACTION_TIMEOUT`    | Request timeout \* 2 | No       | Transaction timeout in milliseconds                   | `brokerOptions.producerOptions.transactionTimeout`        |
| `MO_KAFKA_PRODUCER_RETRIES`                | `10`                 | No       | Number of retries for producer operations             | `brokerOptions.producerOptions.retries`                   |
| `MO_KAFKA_PRODUCER_LAG_THRESHOLD`          | `100`                | No       | Consumer lag threshold for backpressure monitoring    | `lagThreshold` or `topicOptions.lagThreshold`             |
| `MO_KAFKA_PRODUCER_LAG_INTERVAL`           | `5000`               | No       | Interval for checking consumer lag in milliseconds    | `lagMonitorInterval` or `topicOptions.lagMonitorInterval` |

## Consumer Configuration

| Environment Variable                        | Default                 | Required | Description                                            | Constructor Equivalent                               |
| ------------------------------------------- | ----------------------- | -------- | ------------------------------------------------------ | ---------------------------------------------------- |
| `MO_TASK_PROCESSING_BASE_TTL_MS`            | `30000`                 | No       | Base TTL for task processing (used for derived values) | Not directly configurable via constructor            |
| `MO_KAFKA_CONSUMER_FROM_BEGINNING`          | `true`                  | No       | Whether to consume from the beginning of the topic     | `brokerOptions.consumerOptions.fromBeginning`        |
| `MO_KAFKA_CONSUMER_SESSION_TIMEOUT`         | Processing TTL          | No       | Consumer session timeout in milliseconds               | `brokerOptions.consumerOptions.sessionTimeout`       |
| `MO_KAFKA_CONSUMER_REBALANCE_TIMEOUT`       | Processing TTL \* 5     | No       | Rebalance timeout in milliseconds                      | `brokerOptions.consumerOptions.rebalanceTimeout`     |
| `MO_KAFKA_CONSUMER_HEARTBEAT_INTERVAL`      | Processing TTL / 10     | No       | Heartbeat interval in milliseconds                     | `brokerOptions.consumerOptions.heartbeatInterval`    |
| `MO_KAFKA_CONSUMER_MAX_BYTES_PER_PARTITION` | `1048576`               | No       | Maximum bytes per partition                            | `brokerOptions.consumerOptions.maxBytesPerPartition` |
| `MO_KAFKA_CONSUMER_MIN_BYTES`               | `1`                     | No       | Minimum bytes to fetch                                 | `brokerOptions.consumerOptions.minBytes`             |
| `MO_KAFKA_CONSUMER_MAX_BYTES`               | `10485760`              | No       | Maximum bytes to fetch                                 | `brokerOptions.consumerOptions.maxBytes`             |
| `MO_KAFKA_CONSUMER_MAX_WAIT_TIME_MS`        | Connection timeout \* 5 | No       | Maximum wait time in milliseconds                      | `brokerOptions.consumerOptions.maxWaitTimeInMs`      |
| `MO_KAFKA_CONSUMER_MAX_IN_FLIGHT_REQUESTS`  | `null`                  | No       | Maximum in-flight requests (null = unlimited)          | `brokerOptions.consumerOptions.maxInFlightRequests`  |
| `MO_KAFKA_CONSUMER_RETRIES`                 | `5`                     | No       | Number of retries for consumer operations              | `brokerOptions.consumerOptions.retry.retries`        |
| `MO_KAFKA_CONSUMER_EACH_BATCH_AUTO_RESOLVE` | `true`                  | No       | Whether to auto-resolve each batch                     | Not commonly overridden in constructor               |
| `MO_KAFKA_CONSUMER_AUTO_COMMIT`             | `false`                 | No       | Whether to auto-commit offsets                         | Not commonly overridden in constructor               |
| `MO_KAFKA_CONSUMER_AUTO_COMMIT_INTERVAL`    | `5000`                  | No       | Auto-commit interval in milliseconds                   | Not commonly overridden in constructor               |
| `MO_KAFKA_CONSUMER_AUTO_COMMIT_THRESHOLD`   | `100`                   | No       | Auto-commit threshold in number of messages            | Not commonly overridden in constructor               |
| `MO_MAX_CONCURRENT_MESSAGES`                | `1`                     | No       | Maximum concurrent messages to process                 | `maxConcurrency` or `topicOptions.maxConcurrency`    |

## Topic Configuration

| Environment Variable                | Default                  | Required | Description                                                   | Constructor Equivalent                            |
| ----------------------------------- | ------------------------ | -------- | ------------------------------------------------------------- | ------------------------------------------------- |
| `MO_TASK_PROCESSING_STATE_TTL_MS`   | Processing base TTL      | No       | TTL for processing state in milliseconds (must be ≥ 1000ms)   | `topicOptions.processingTtl` or `processingTtl`   |
| `MO_TASK_SUPPRESSION_STATE_TTL_MS`  | Processing base TTL \* 3 | No       | TTL for suppression in milliseconds (must be > processingTtl) | `topicOptions.suppressionTtl` or `suppressionTtl` |
| `MO_KAFKA_TOPIC_PARTITIONS`         | `3`                      | No       | Number of partitions for topic                                | `topicOptions.partitions`                         |
| `MO_KAFKA_TOPIC_REPLICATION_FACTOR` | `1`                      | No       | Replication factor for topic                                  | `topicOptions.replicationFactor`                  |
| `MO_KAFKA_TOPIC_AUTO_CREATION`      | `true`                   | No       | Whether to create topic if it doesn't exist                   | `topicOptions.allowAutoTopicCreation`             |

## Monitor Service Configuration

| Environment Variable                | Default                    | Required | Description                                        | Constructor Equivalent                    |
| ----------------------------------- | -------------------------- | -------- | -------------------------------------------------- | ----------------------------------------- |
| `MO_BACKPRESSURE_CHECK_INTERVAL_MS` | Network timeout \* 3       | No       | Interval for checking backpressure in milliseconds | Not directly configurable via constructor |
| `MO_BACKPRESSURE_CACHE_TTL_MS`      | Network timeout \* 3 \* 20 | No       | TTL for backpressure cache in milliseconds         | Not directly configurable via constructor |
| `MO_BACKOFF_MIN_DELAY_MS`           | Retry interval \* 3        | No       | Minimum backoff delay in milliseconds              | Not directly configurable via constructor |
| `MO_BACKOFF_MAX_DELAY_MS`           | Retry interval \* 6        | No       | Maximum backoff delay in milliseconds              | Not directly configurable via constructor |

## Base Configuration Values

These primary values are used to derive other timeouts and intervals:

| Environment Variable                   | Default | Required | Description                         | Constructor Equivalent                    |
| -------------------------------------- | ------- | -------- | ----------------------------------- | ----------------------------------------- |
| `MO_TASK_PROCESSING_BASE_TTL_MS`       | `30000` | No       | Base TTL for task processing        | Not directly configurable via constructor |
| `MO_NETWORK_OPERATION_BASE_TIMEOUT_MS` | `5000`  | No       | Base timeout for network operations | Not directly configurable via constructor |
| `MO_RETRY_BASE_INTERVAL_MS`            | `3000`  | No       | Base interval for retries           | Not directly configurable via constructor |

## Value Standardization

The `standardizeConfig` method in `KafkaManager` is responsible for:

1. Merging user-provided configuration with default values
2. Setting derived values based on primary TTLs
3. Enforcing validation rules:
   - Processing TTL must be ≥ 1000ms
   - Suppression TTL must be > Processing TTL
4. Setting consistent defaults:
   - `clientId` defaults to `${topic}-client-${timestamp}`
   - `groupId` defaults to `${topic}-processors`
   - Key prefixes default to topic name in uppercase
   - SSL and SASL settings are preserved if provided
   - Suppression TTL is automatically set to 3x Processing TTL if not specified

## Retry Configuration

Both client and consumer/producer have their own retry configurations:

### Client Retry Configuration

```javascript
const retry = {
  initialRetryTime: 100, // Default initial retry time in ms
  retries: 10, // Default number of retries
  factor: 0.2, // Exponential factor
  multiplier: 2, // Retry multiplier
  maxRetryTime: 30000, // Maximum retry time in ms
};
```

### Consumer/Producer Retry Configuration

```javascript
const restry = {
  initialRetryTime: 300, // Default initial retry time in ms
  retries: 5, // Default number of retries
  factor: 0.2, // Exponential factor
  multiplier: 2, // Retry multiplier
  maxRetryTime: 30000, // Maximum retry time in ms
};
```

## Simplified Configuration Structure

The constructor accepts a simplified configuration structure that gets standardized internally:

```json
// This simplified configuration:
{
  "topic": "my-topic",
  // Required: Kafka topic name
  "groupId": "my-group",
  // Optional: Consumer group ID (default: ${topic}-processors)
  "topicOptions": {
    // Optional: Topic-specific settings
    "processingTtl": 30000,
    // TTL for processing state
    "suppressionTtl": 90000,
    // TTL for suppression
    "partitions": 3,
    // Partitions for the topic
    "replicationFactor": 1,
    // Replication factor
    "allowAutoTopicCreation": true,
    // Create topic if it doesn't exist
    "maxConcurrency": 5
    // Concurrent messages (consumer)
  },
  "useSuppression": true,
  // Enable message deduplication
  "useDistributedLock": true,
  // Enable distributed locks
  "maxConcurrency": 5,
  // Concurrent messages (alternative location)
  "lagThreshold": 100,
  // Producer backpressure threshold
  "lagMonitorInterval": 5000
  // Producer lag check interval
}
```

## Example Docker Compose Configuration

Below is an example Kafka configuration from the ecommerce-order-processing example:

```yaml
kafka:
  image: confluentinc/cp-kafka:7.4.0
  environment:
    KAFKA_BROKER_ID: 1
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
    KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    KAFKA_COMPRESSION_TYPE: gzip
    KAFKA_LOG_COMPRESSION_TYPE: gzip
    KAFKA_MESSAGE_MAX_BYTES: 1048576
    KAFKA_LOG_SEGMENT_BYTES: 1073741824
```

## Example Environment Variables

Below is an example of a comprehensive Kafka configuration with environment variables:

```dotenv
# Client configuration
MO_KAFKA_CLIENT_ID=my-application
MO_KAFKA_BROKERS=kafka1:9092,kafka2:9092,kafka3:9092
MO_KAFKA_ENFORCE_REQUEST_TIMEOUT=true
MO_KAFKA_INITIAL_RETRY_TIME_MS=200
MO_KAFKA_RETRY_COUNT=5
MO_KAFKA_MAX_RETRY_TIME_MS=20000

# Producer configuration
MO_KAFKA_PRODUCER_ACKS=-1
MO_KAFKA_COMPRESSION_TYPE=gzip
MO_KAFKA_PRODUCER_IDEMPOTENT=true
MO_KAFKA_PRODUCER_MAX_IN_FLIGHT_REQUESTS=10
MO_KAFKA_PRODUCER_RETRIES=3

# Consumer configuration
MO_KAFKA_CONSUMER_FROM_BEGINNING=false
MO_KAFKA_CONSUMER_MAX_BYTES_PER_PARTITION=2097152
MO_KAFKA_CONSUMER_MAX_BYTES=20971520
MO_KAFKA_CONSUMER_RETRIES=3
MO_MAX_CONCURRENT_MESSAGES=5

# Topic configuration
MO_KAFKA_TOPIC_PARTITIONS=3
MO_KAFKA_TOPIC_REPLICATION_FACTOR=3
MO_KAFKA_TOPIC_AUTO_CREATION=true

# Base timeouts
MO_TASK_PROCESSING_BASE_TTL_MS=60000
MO_NETWORK_OPERATION_BASE_TIMEOUT_MS=10000
MO_RETRY_BASE_INTERVAL_MS=5000
```

## Notes on TTL Relationships

The system validates relationships between TTLs to prevent race conditions:

1. Distributed Lock TTL should be at least 2x Task Processing TTL
2. Lock refresh interval should not be too close to Task Processing TTL
3. Backpressure check interval should not exceed Kafka request timeout
