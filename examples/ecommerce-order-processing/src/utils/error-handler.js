/**
 * Error Handler Service
 *
 * Provides centralized error handling functionality for the application.
 * Handles uncaught exceptions, unhandled rejections, and application-specific errors.
 */

const Logger = require("./logger");

/**
 * ErrorHandler class
 *
 * Static utility class for centralized error handling across the application.
 */
class ErrorHandler {
  /**
   * Sets up global error handlers for the process
   */
  static setup() {
    process.on("uncaughtException", ErrorHandler.handleUncaughtException);
    process.on("unhandledRejection", ErrorHandler.handleUnhandledRejection);
  }

  /**
   * Handles uncaught exceptions
   *
   * @param {Error} error - The uncaught exception
   */
  static handleUncaughtException(error) {
    Logger.error("Uncaught Exception", error);
  }

  /**
   * Handles unhandled promise rejections
   *
   * @param {Error|any} reason - The rejection reason
   */
  static handleUnhandledRejection(reason) {
    Logger.error("Unhandled Rejection", reason);
  }

  /**
   * Handles configuration errors
   *
   * @param {Error} error - The configuration error
   */
  static handleConfigurationError(error) {
    Logger.error("Configuration error", error);
    Logger.info("Please check your environment variables");
    Logger.info("Or run 'npm run db:init' to setup the database first");
  }

  /**
   * Handles application startup errors
   *
   * @param {Error} error - The application error
   */
  static handleApplicationError(error) {
    Logger.error("Application failed to start", error);
  }
}

module.exports = ErrorHandler;
