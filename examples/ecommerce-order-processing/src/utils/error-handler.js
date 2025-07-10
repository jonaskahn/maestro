/**
 * Error Handler
 * Centralized error handling following Clean Code principles
 */

const Logger = require("./logger");

class ErrorHandler {
  static setup() {
    process.on("uncaughtException", ErrorHandler.handleUncaughtException);
    process.on("unhandledRejection", ErrorHandler.handleUnhandledRejection);
  }

  static handleUncaughtException(error) {
    Logger.error("Uncaught Exception", error);
    process.exit(1);
  }

  static handleUnhandledRejection(reason) {
    Logger.error("Unhandled Rejection", reason);
    process.exit(1);
  }

  static handleConfigurationError(error) {
    Logger.error("Configuration error", error);
    Logger.info("Please check your environment variables");
    Logger.info("Or run 'npm run db:init' to setup the database first");
    process.exit(1);
  }

  static handleApplicationError(error) {
    Logger.error("Application failed to start", error);
    process.exit(1);
  }
}

module.exports = ErrorHandler;
