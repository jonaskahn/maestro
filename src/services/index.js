/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Central export file for system-wide services and utilities
 *
 * This module exports common service classes used throughout the application,
 * providing standardized access to logging, distributed locking, and other
 * shared functionality. These services are designed to be application-agnostic
 * and can be used across different components.
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
