/**
 * Order Consumer Runner
 *
 * Entry point for the order consumer service.
 * Manages the lifecycle of the consumer service, including startup, shutdown, and database connections.
 */

const { OrderConsumer } = require("./src/broker");
const database = require("./src/database/mongodb-client");
const Logger = require("./src/utils/logger");

/**
 * ConsumerDaemon class
 *
 * Manages the order consumer service as a long-running daemon process.
 * Handles consumer lifecycle, database connections, and graceful shutdown.
 */
class ConsumerDaemon {
  /**
   * Creates a new consumer daemon instance
   */
  constructor() {
    this.consumer = new OrderConsumer({
      topic: "ecommerce-orders",
      topicOptions: {
        processingTtl: 240_000,
        maxConcurrency: 10,
      },
    });
  }

  /**
   * Starts the consumer service
   *
   * @returns {Promise<void>} - Resolves when the service is started
   */
  async start() {
    try {
      await this.initializeDatabase();

      await this.consumer.connect();
      await this.consumer.consume();

      this.setupGracefulShutdown();

      Logger.success("Consumer daemon started successfully");
    } catch (error) {
      Logger.error("Failed to start consumer daemon", error);
      await this.cleanup();
      process.exit(1);
    }
  }

  /**
   * Initializes the database connection
   *
   * @returns {Promise<void>} - Resolves when the database is connected
   */
  async initializeDatabase() {
    await database.connect();
  }

  /**
   * Sets up signal handlers for graceful shutdown
   */
  setupGracefulShutdown() {
    const signals = ["SIGTERM", "SIGINT", "SIGUSR2"];

    signals.forEach(signal => {
      process.on(signal, async () => {
        Logger.info(`Received ${signal}, shutting down gracefully`);
        await this.cleanup();
        process.exit(0);
      });
    });
  }

  /**
   * Cleans up resources before shutdown
   *
   * @returns {Promise<void>} - Resolves when cleanup is complete
   */
  async cleanup() {
    try {
      if (this.consumer) {
        await this.consumer.stopConsuming();
        await this.consumer.disconnect();
      }

      if (database.isConnected) {
        await database.disconnect();
      }
    } catch (error) {
      Logger.error("Cleanup error", error);
    }
  }
}

/**
 * Main application entry point
 *
 * @returns {Promise<void>} - Resolves when the application is started
 */
async function main() {
  try {
    require("dotenv").config();
    const daemon = new ConsumerDaemon();
    await daemon.start();
  } catch (error) {
    Logger.error("Startup failed", error);
    process.exit(1);
  }
}

if (require.main === module) {
  main().catch(error => {
    Logger.error("Startup failed", error);
    process.exit(1);
  });
}

module.exports = { ConsumerDaemon };
