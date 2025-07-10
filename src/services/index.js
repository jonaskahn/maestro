/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * Central export for generic services.
 *
 * Example:
 *   const { DistributedLockService, LoggerService } = require("@/services");
 */

module.exports = {
  DistributedLockService: require("./distributed-lock-service"),
  LoggerService: require("./logger-service"),
};
