# E-commerce Order Processing Example

This example demonstrates how to use Maestro to build a robust e-commerce order processing system with Kafka and
MongoDB.

## Overview

This application shows a complete implementation of:

- **Order Producer**: Fetches pending orders from MongoDB and sends them to Kafka
- **Order Consumer**: Processes orders from Kafka and updates their status in MongoDB
- **Scheduled Processing**: Uses cron jobs to periodically check for new orders
- **Graceful Shutdown**: Proper resource cleanup and connection handling

## Architecture

```
┌─────────────┐    ┌─────────┐    ┌──────────────┐
│  MongoDB    │◄───┤Producer │───►│  Kafka Topic │
│ (Orders DB) │    │(Cronjob)│    │(ecommerce-   │
└─────────────┘    └─────────┘    │   orders)    │
       ▲                          └──────┬───────┘
       │                                 │
       │                                 ▼
       │                          ┌──────────────┐
       └──────────────────────────┤  Consumer    │
                                  │  (Daemon)    │
                                  └──────────────┘
```

## Prerequisites

- Docker and Docker Compose
- Node.js 18+
- npm or yarn

## Getting Started

### 1. Start the Infrastructure

```bash
# Start all required services (Kafka, MongoDB, Redis, etc.)
docker-compose up -d

# Or use the restart script
./restart-all.sh
```

### 2. Initialize the Database

```bash
# Initialize the database with sample orders
npm run db:init

# Or generate specific number of orders
npm run db:generate 50
```

### 3. Run the Producer and Consumer

```bash
# Start the producer (in one terminal)
npm run producer

# Start the consumer (in another terminal)
npm run consumer
```

## Key Components

### Producer

The producer runs as a scheduled job that:

- Queries MongoDB for pending orders
- Sends them to Kafka with deduplication
- Updates order status after successful sending

```javascript
// See run-producer.js and src/broker/producer.js for implementation details
```

### Consumer

The consumer runs as a daemon that:

- Listens for messages on the Kafka topic
- Processes each order (validation, payment, etc.)
- Updates order status in MongoDB

```javascript
// See run-consumer.js and src/broker/consumer.js for implementation details
```

## Database Management

Several scripts are provided for database management:

```bash
# Initialize database
npm run db:init

# Reset database (clear and reinitialize)
npm run db:reset

# Generate sample orders
npm run db:generate <count>

# Show database statistics
npm run db:stats

# Clear all orders
npm run db:clear

# Drop database
npm run db:drop
```

## Configuration

The application uses environment variables for configuration:

- `LOG_LEVEL`: Set logging level (debug, info, warn, error)
- `KAFKA_BROKERS`: Kafka broker addresses
- `MONGODB_URI`: MongoDB connection string

See `.env.example` for more configuration options.

## Monitoring

- **Kafka UI**: Available at http://localhost:8080
- **Console Logs**: Both producer and consumer output detailed logs

## Advanced Features

This example demonstrates several advanced Maestro features:

- **Message Suppression**: Prevents duplicate order processing
- **Distributed Locking**: Coordinates producers in distributed environments
- **Backpressure Monitoring**: Detects and adapts to processing bottlenecks
- **Graceful Shutdown**: Proper cleanup of resources on termination

## Troubleshooting

If you encounter issues:

1. Check if all services are running: `docker-compose ps`
2. Verify Kafka topics: `docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list`
3. Reset the database: `npm run db:reset`
4. Restart all services: `./restart-all.sh`

## License

[MIT License](../../LICENSE)
