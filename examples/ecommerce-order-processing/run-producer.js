/**
 * Order Producer Runner
 *
 * Entry point for the order producer service with cron scheduling.
 * Manages the lifecycle of the producer service, including startup, shutdown, and periodic job execution.
 */

const cron = require("node-cron");
const { OrderProducer } = require("./src/broker");
const Logger = require("./src/utils/logger");

/**
 * ProducerCronjob class
 *
 * Manages scheduled order production jobs using cron scheduling.
 * Handles producer lifecycle, job execution, and graceful shutdown.
 */
class ProducerCronjob {
  /**
   * Creates a new producer cronjob instance
   */
  constructor() {
    this.isJobRunning = false;
    this.task = null;
    this.producer = new OrderProducer({
      topic: "ecommerce-orders",
      topicOptions: {
        processingTtl: 30_000,
        lagThreshold: 1000,
        lagMonitorInterval: 5000,
      },
    });
  }

  /**
   * Starts the producer service and cron job
   *
   * @returns {Promise<void>} - Resolves when the service is started
   */
  async start() {
    try {
      await this.producer.connect();
      await this.startCronjob();
      this.setupGracefulShutdown();
    } catch (error) {
      Logger.error("Failed to start producer cronjob", error);
      await this.cleanupResources();
    }
  }

  /**
   * Starts the cron job for scheduled order processing
   *
   * @returns {Promise<void>} - Resolves when the cron job is started
   */
  async startCronjob() {
    const cronExpression = "*/1 * * * *";
    const allowImmediateExecution = true;

    this.task = cron.schedule(
      cronExpression,
      async () => {
        await this.runProducerJob();
      },
      {
        scheduled: false,
      }
    );

    if (allowImmediateExecution) {
      await this.runProducerJob();
    }

    this.task.start();
  }

  /**
   * Runs a single producer job iteration
   *
   * @returns {Promise<void>} - Resolves when the job completes
   */
  async runProducerJob() {
    if (this.isJobRunning) {
      return;
    }

    this.isJobRunning = true;

    try {
      const pendingOrdersCriteria = { state: 1, priority: "normal" };
      await this.producer.produce(pendingOrdersCriteria, 50);
    } catch (error) {
      Logger.error(`Production job failed: ${error.message}`);
    } finally {
      this.isJobRunning = false;
    }
  }

  /**
   * Sets up signal handlers for graceful shutdown
   */
  setupGracefulShutdown() {
    const signals = ["SIGTERM", "SIGINT", "SIGUSR2"];

    signals.forEach(signal => {
      process.on(signal, async () => {
        Logger.info(`Received ${signal}, shutting down gracefully`);

        if (this.task) {
          this.task.stop();
        }

        await this.cleanupResources();
      });
    });
  }

  /**
   * Cleans up resources before shutdown
   *
   * @returns {Promise<void>} - Resolves when cleanup is complete
   */
  async cleanupResources() {
    const cleanupTasks = [];

    if (this.producer) {
      cleanupTasks.push(this.producer.disconnect().catch(err => Logger.error("Producer cleanup error", err)));
      cleanupTasks.push(this.producer.cleanup().catch(err => Logger.error("Database cleanup error", err)));
    }

    await Promise.allSettled(cleanupTasks);
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
    const cronjob = new ProducerCronjob();
    await cronjob.start();
  } catch (error) {
    Logger.error("Startup failed", error);
  }
}

if (require.main === module) {
  main().catch(error => {
    Logger.error("Startup failed", error);
  });
}

module.exports = { ProducerCronjob };
