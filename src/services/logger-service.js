/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Centralized Logging Service using Winston
 *
 * Provides structured logging with multiple formats, levels, and specialized methods for application events.
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

class LoggerService {
  constructor() {
    this.logger = this._createLogger();
  }

  /**
   * Logs informational messages
   * @param {string} message - Message to log
   * @param {Object} metadata - Additional context data
   */
  logInfo(message, metadata = {}) {
    this.logger.info(message, metadata);
  }

  /**
   * Logs debug messages for development and troubleshooting
   * @param {string} message - Message to log
   * @param {Object} metadata - Additional context data
   */
  logDebug(message, metadata = {}) {
    this.logger.debug(message, metadata);
  }

  /**
   * Logs warning messages for potentially problematic situations
   * @param {string} message - Message to log
   * @param {Object} metadata - Additional context data
   */
  logWarning(message, metadata = {}) {
    this.logger.warn(message, metadata);
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

    this.logger.error(message, { ...errorInfo, ...metadata });
  }

  /**
   * Logs batch operation events with structured identifiers
   * @param {string} batchId - Unique batch identifier
   * @param {string} operation - Operation description
   * @param {Object} details - Operation-specific details
   */
  logBatchOperation(batchId, operation, details = {}) {
    this.logInfo(`[Batch ${batchId}] ${operation}`, details);
  }

  /**
   * Logs task operation events with structured identifiers
   * @param {string} taskId - Unique task identifier
   * @param {string} operation - Operation description
   * @param {Object} details - Task-specific details
   */
  logTaskOperation(taskId, operation, details = {}) {
    this.logInfo(`[${taskId}] ${operation}`, details);
  }

  /**
   * Logs connection and disconnection events for services
   * @param {string} service - Service name (e.g., "Kafka", "Redis")
   * @param {string} event - Connection event description
   * @param {Object} details - Connection-specific details
   */
  logConnectionEvent(service, event, details = {}) {
    this.logInfo(`${service} ${event}`, details);
  }

  /**
   * Logs concurrency status for worker pools and processing queues
   * @param {number} active - Number of active workers
   * @param {number} max - Maximum number of workers
   * @param {number} queued - Number of queued tasks
   * @param {Object} additionalInfo - Additional concurrency metrics
   */
  logConcurrencyStatus(active, max, queued, additionalInfo = {}) {
    this.logInfo(`Concurrency Status: ${active}/${max} active, ${queued} queued`, additionalInfo);
  }

  /**
   * Logs processing statistics and performance metrics
   * @param {Object} statistics - Processing statistics object
   */
  logProcessingStatistics(statistics) {
    this.logInfo("Processing Statistics", statistics);
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

  _getLogLevel() {
    return process.env.LOG_LEVEL || LOG_LEVELS.INFO;
  }

  _getLogFormat() {
    return process.env.LOG_FORMAT || LOG_FORMATS.SIMPLE;
  }

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
          return `${timestamp} [${level}]: ${message}${metaString}`;
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
          return `${timestamp} [${level}]: ${message}${metaString}${stackString}`;
        })
      ),
    };

    return formats[formatType] || formats[LOG_FORMATS.SIMPLE];
  }

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
  LoggerService,
  default: LoggerService,
};
