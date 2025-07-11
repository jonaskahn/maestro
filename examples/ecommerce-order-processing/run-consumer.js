/**
 * Order Consumer Runner
 *
 * Entry point for the order consumer service.
 */

const { OrderConsumer } = require("./src/broker");
const database = require("./src/database/mongodb-client");
const Logger = require("./src/utils/logger");

class ConsumerDaemon {
  constructor() {
    this.consumer = new OrderConsumer();
  }

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

  async initializeDatabase() {
    await database.connect();
  }

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
