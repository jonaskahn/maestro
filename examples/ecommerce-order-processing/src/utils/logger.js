/**
 * Logger
 * Standardized logging following Clean Code principles
 */

class Logger {
  static info(message, ...args) {
    console.log(`[INFO] ${message}`, ...args);
  }

  static success(message, ...args) {
    console.log(`[SUCCESS] ${message}`, ...args);
  }

  static error(message, error = null) {
    if (error instanceof Error) {
      console.error(`[ERROR] ${message}:`, error.message);
    } else {
      console.error(`[ERROR] ${message}`);
    }
  }

  static warn(message, ...args) {
    console.warn(`[WARN] ${message}`, ...args);
  }

  static debug(message, ...args) {
    if (process.env.DEBUG === "true") {
      console.log(`[DEBUG] ${message}`, ...args);
    }
  }

  static table(data) {
    console.table(data);
  }

  static separator() {
    console.log("=".repeat(50));
  }
}

module.exports = Logger;
