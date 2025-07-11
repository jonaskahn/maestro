/**
 * Order Producer Runner
 *
 * Entry point for the order producer service with cron scheduling.
 */

const cron = require("node-cron");
const { OrderProducer } = require("./src/broker");
const Logger = require("./src/utils/logger");

class ProducerCronjob {
  constructor() {
    this.isJobRunning = false;
    this.task = null;
    this.producer = new OrderProducer();
  }

  async start() {
    try {
      await this.producer.connect();
      await this.startCronjob();
      this.setupGracefulShutdown();

      Logger.success("Producer cronjob started successfully");
    } catch (error) {
      Logger.error("Failed to start producer cronjob", error);
      await this.cleanupResources();
      process.exit(1);
    }
  }

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

  setupGracefulShutdown() {
    const signals = ["SIGTERM", "SIGINT", "SIGUSR2"];

    signals.forEach(signal => {
      process.on(signal, async () => {
        Logger.info(`Received ${signal}, shutting down gracefully`);

        if (this.task) {
          this.task.stop();
        }

        await this.cleanupResources();
        process.exit(0);
      });
    });
  }

  async cleanupResources() {
    const cleanupTasks = [];

    if (this.producer) {
      cleanupTasks.push(this.producer.disconnect().catch(err => Logger.error("Producer cleanup error", err)));
      cleanupTasks.push(this.producer.cleanup().catch(err => Logger.error("Database cleanup error", err)));
    }

    await Promise.allSettled(cleanupTasks);
  }
}

async function main() {
  try {
    const cronjob = new ProducerCronjob();
    await cronjob.start();
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

module.exports = { ProducerCronjob };
