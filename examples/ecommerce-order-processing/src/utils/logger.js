/**
 * Logger Service
 *
 * Provides standardized logging functionality for the application.
 * Implements different log levels and formatting for consistent output.
 */

/**
 * Logger class
 *
 * Static utility class for standardized logging with different severity levels.
 */
class Logger {
  /**
   * Logs informational messages
   *
   * @param {string} message - Message to log
   * @param {...any} args - Additional arguments to log
   */
  static info(message, ...args) {
    console.log(`[INFO] ${message}`, ...args);
  }

  /**
   * Logs success messages
   *
   * @param {string} message - Message to log
   * @param {...any} args - Additional arguments to log
   */
  static success(message, ...args) {
    console.log(`[SUCCESS] ${message}`, ...args);
  }

  /**
   * Logs error messages
   *
   * @param {string} message - Error message
   * @param {Error|null} error - Optional error object
   */
  static error(message, error = null) {
    if (error instanceof Error) {
      console.error(`[ERROR] ${message}:`, error.message);
    } else {
      console.error(`[ERROR] ${message}`);
    }
  }

  /**
   * Logs warning messages
   *
   * @param {string} message - Warning message
   * @param {...any} args - Additional arguments to log
   */
  static warn(message, ...args) {
    console.warn(`[WARN] ${message}`, ...args);
  }

  /**
   * Logs debug messages (only when DEBUG=true)
   *
   * @param {string} message - Debug message
   * @param {...any} args - Additional arguments to log
   */
  static debug(message, ...args) {
    if (process.env.DEBUG === "true") {
      console.log(`[DEBUG] ${message}`, ...args);
    }
  }

  /**
   * Displays tabular data
   *
   * @param {Object|Array} data - Data to display in table format
   */
  static table(data) {
    console.table(data);
  }

  /**
   * Prints a separator line for visual organization of logs
   */
  static separator() {
    console.log("=".repeat(50));
  }
}

module.exports = Logger;
