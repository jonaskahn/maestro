/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Centralized Logging Service
 *
 * Provides a standardized interface for application-wide logging with multiple
 * formats, severity levels, and specialized methods for different types of events.
 * Built on top of Winston logger with configurable transports and formatting options.
 * Exposes singleton instance for consistent logging across the application.
 */
const winston = require("winston");

const LOG_LEVELS = {
  ERROR: "error",
  WARN: "warn",
  INFO: "info",
  DEBUG: "debug",
};

const LOG_FORMATS = {
  SIMPLE: "simple",
  JSON: "json",
  DETAILED: "detailed",
};

/**
 * Log categories with standardized icons
 */
const LOG_CATEGORIES = {
  CONNECTION: {
    CONNECT: "🔌",
    DISCONNECT: "️‍💥",
    RECONNECT: "⛓️‍💥",
    READY: "📍",
  },
  OPERATION: {
    START: "▶️",
    STOP: "⏹️",
    PROCESSING: "⚙️",
    COMPLETE: "✓",
    BATCH: "📦",
    TASK: "📋",
  },
  STATUS: {
    INFO: "️💬",
    SUCCESS: "✅",
    WARNING: "⚠️",
    ERROR: "❌",
    DEBUG: "⚒️",
  },
  DATA: {
    CREATED: "📝",
    UPDATED: "✏️",
    DELETED: "🗑️",
    READ: "📖",
  },
  PERFORMANCE: {
    STATS: "📊",
    CONCURRENCY: "⚖️",
    TIMEOUT: "⏱️",
  },
};

/**
 * Logger Service Class
 *
 * Provides standardized logging functionality with configurable formats and levels.
 * Supports structured logging with metadata, specialized event logging methods,
 * and different output formats for development and production environments.
 */
class LoggerService {
  /**
   * Creates a new logger instance with configuration from environment
   * Logger level and format are determined from LOG_LEVEL and LOG_FORMAT environment variables
   */
  constructor() {
    this.logger = this._createLogger();
  }

  /**
   * Logs informational messages
   * @param {string} message - Message to log
   * @param {Object} metadata - Additional context data
   */
  logInfo(message, metadata) {
    this.logger.info(
      `${LOG_CATEGORIES.STATUS.INFO} ${message?.trim()} ${metadata ? JSON.stringify(metadata, null, 2) : ""}`
    );
  }

  /**
   * Logs debug messages for development and troubleshooting
   * @param {string} message - Message to log
   * @param {Object} metadata - Additional context data
   */
  logDebug(message, metadata) {
    this.logger.debug(
      `${LOG_CATEGORIES.STATUS.DEBUG} ${message?.trim()} ${metadata ? JSON.stringify(metadata, null, 2) : ""}`
    );
  }

  /**
   * Logs warning messages for potentially problematic situations
   * @param {string} message - Message to log
   * @param {Object} metadata - Additional context data
   */
  logWarning(message, metadata) {
    this.logger.warn(
      `${LOG_CATEGORIES.STATUS.WARNING} ${message?.trim()} ${metadata ? JSON.stringify(metadata, null, 2) : ""}`
    );
  }

  /**
   * Logs error messages with optional error object and stack trace
   * @param {string} message - Error description
   * @param {Error} error - Error object with stack trace
   * @param {Object} metadata - Additional context data
   */
  logError(message, error = null, metadata = {}) {
    const errorInfo = error
      ? {
          error: error.message,
          stack: error.stack,
        }
      : {};

    this.logger.error(
      `${LOG_CATEGORIES.STATUS.ERROR} ${message?.trim()} ${JSON.stringify({ ...errorInfo, ...metadata }, null, 2)}`
    );
  }

  /**
   * Logs batch operation events with structured identifiers
   * @param {string} batchId - Unique batch identifier
   * @param {string} operation - Operation description
   * @param {Object} details - Operation-specific details
   */
  logBatchOperation(batchId, operation, details) {
    this.logger.info(
      `${LOG_CATEGORIES.OPERATION.BATCH} [Batch ${batchId}] ${operation} ${details ? JSON.stringify(details, null, 2) : ""}`
    );
  }

  /**
   * Logs task operation events with structured identifiers
   * @param {string} taskId - Unique task identifier
   * @param {string} operation - Operation description
   * @param {Object} details - Task-specific details
   */
  logTaskOperation(taskId, operation, details) {
    this.logger.info(
      `${LOG_CATEGORIES.OPERATION.TASK} [${taskId}] ${operation} ${details ? JSON.stringify(details, null, 2) : ""}`
    );
  }

  /**
   * Logs connection and disconnection events for services
   * @param {string} service - Service name (e.g., "Kafka", "Redis")
   * @param {string} event - Connection event description
   * @param {Object} details - Connection-specific details
   */
  logConnectionEvent(service, event, details) {
    let icon = LOG_CATEGORIES.STATUS.INFO;

    if (event.includes("connect")) {
      icon = LOG_CATEGORIES.CONNECTION.CONNECT;
    } else if (event.includes("disconnect")) {
      icon = LOG_CATEGORIES.CONNECTION.DISCONNECT;
    } else if (event.includes("reconnect")) {
      icon = LOG_CATEGORIES.CONNECTION.RECONNECT;
    } else if (event.includes("ready")) {
      icon = LOG_CATEGORIES.CONNECTION.READY;
    }
    this.logger.debug(`${icon} ${service}: ${event} ${details ? JSON.stringify(details) : ""}`);
  }

  /**
   * Logs concurrency status for worker pools and processing queues
   * @param {number} active - Number of active workers
   * @param {number} max - Maximum number of workers
   * @param {number} queued - Number of queued tasks
   * @param {Object} additionalInfo - Additional concurrency metrics
   */
  logConcurrencyStatus(active, max, queued, additionalInfo) {
    this.logger.info(
      `${LOG_CATEGORIES.PERFORMANCE.CONCURRENCY} Concurrency Status: ${active}/${max} active, ${queued} queued ${additionalInfo ? JSON.stringify(additionalInfo, null, 2) : ""}`
    );
  }

  /**
   * Logs processing statistics and performance metrics
   * @param {Object} statistics - Processing statistics object
   */
  logProcessingStatistics(statistics) {
    this.logger.info(
      `${LOG_CATEGORIES.PERFORMANCE.STATS} Processing Statistics ${statistics ? JSON.stringify(statistics, null, 2) : ""}`
    );
  }

  /**
   * Checks if debug level logging is currently enabled
   * @returns {boolean} True if debug logging is enabled
   */
  isDebugEnabled() {
    return this.logger.isDebugEnabled();
  }

  /**
   * Checks if specific log level is enabled
   * @param {string} level - Log level to check ('error', 'warn', 'info', 'debug')
   * @returns {boolean} True if specified level is enabled
   */
  isLevelEnabled(level) {
    return this.logger.isLevelEnabled(level);
  }

  /**
   * Creates and configures a Winston logger instance
   * @returns {Object} Configured Winston logger
   */
  _createLogger() {
    const logLevel = this._getLogLevel();
    const logFormat = this._getLogFormat();

    return winston.createLogger({
      level: logLevel,
      format: this._createFormat(logFormat),
      transports: this._createTransports(),
      exitOnError: false,
    });
  }

  /**
   * Gets log level from environment or defaults to INFO
   * @returns {string} Log level
   */
  _getLogLevel() {
    return process.env.LOG_LEVEL || LOG_LEVELS.INFO;
  }

  /**
   * Gets log format from environment or defaults to SIMPLE
   * @returns {string} Log format type
   */
  _getLogFormat() {
    return process.env.LOG_FORMAT || LOG_FORMATS.SIMPLE;
  }

  /**
   * Creates Winston format based on format type
   * @param {string} formatType - Format type (simple, json, detailed)
   * @returns {Object} Winston format
   */
  _createFormat(formatType) {
    const timestamp = winston.format.timestamp({
      format: "YYYY-MM-DD HH:mm:ss",
    });

    const formats = {
      [LOG_FORMATS.SIMPLE]: winston.format.combine(
        timestamp,
        winston.format.colorize(),
        winston.format.printf(({ timestamp, level, message, ...meta }) => {
          const metaString = Object.keys(meta).length ? ` ${JSON.stringify(meta)}` : "";
          return `${timestamp} [${level}] ${message?.trim()}${metaString}`;
        })
      ),
      [LOG_FORMATS.JSON]: winston.format.combine(timestamp, winston.format.json()),
      [LOG_FORMATS.DETAILED]: winston.format.combine(
        timestamp,
        winston.format.colorize(),
        winston.format.errors({ stack: true }),
        winston.format.printf(({ timestamp, level, message, stack, ...meta }) => {
          const metaString = Object.keys(meta).length ? `\nMeta: ${JSON.stringify(meta, null, 2)}` : "";
          const stackString = stack ? `\nStack: ${stack}` : "";
          return `${timestamp} [${level}] ${message?.trim()}${metaString}${stackString}`;
        })
      ),
    };

    return formats[formatType] || formats[LOG_FORMATS.SIMPLE];
  }

  /**
   * Creates Winston transports for log outputs
   * @returns {Array} Array of Winston transports
   */
  _createTransports() {
    return [
      new winston.transports.Console({
        handleExceptions: true,
        handleRejections: true,
      }),
    ];
  }
}

const loggerInstance = new LoggerService();

/**
 * Export bound methods from singleton instance and the class itself
 * This allows both direct function usage and extending the class if needed
 */
module.exports = {
  logInfo: loggerInstance.logInfo.bind(loggerInstance),
  logDebug: loggerInstance.logDebug.bind(loggerInstance),
  logWarning: loggerInstance.logWarning.bind(loggerInstance),
  logError: loggerInstance.logError.bind(loggerInstance),
  logBatchOperation: loggerInstance.logBatchOperation.bind(loggerInstance),
  logTaskOperation: loggerInstance.logTaskOperation.bind(loggerInstance),
  logConnectionEvent: loggerInstance.logConnectionEvent.bind(loggerInstance),
  logConcurrencyStatus: loggerInstance.logConcurrencyStatus.bind(loggerInstance),
  logProcessingStatistics: loggerInstance.logProcessingStatistics.bind(loggerInstance),
  isDebugEnabled: loggerInstance.isDebugEnabled.bind(loggerInstance),
  isLevelEnabled: loggerInstance.isLevelEnabled.bind(loggerInstance),
  LOG_CATEGORIES,
  LoggerService,
  default: LoggerService,
};
