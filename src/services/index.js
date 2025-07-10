/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * Central export for generic services.
 *
 * @module services
 * @exports {Class} DistributedLockService - Service for distributed locking across processes
 * @exports {Object} LoggerService - Centralized logging service
 *
 * Example:
 *   const { DistributedLockService, LoggerService } = require("@/services");
 */

module.exports = {
  DistributedLockService: require("./distributed-lock-service"),
  LoggerService: require("./logger-service"),
};
