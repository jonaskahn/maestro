/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the MIT License found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Central export file for all abstract base classes used in the message orchestration system.
 *
 * These abstract classes define standardized interfaces for messaging operations,
 * caching, and monitoring across different implementations. They provide a common
 * foundation for implementing concrete provider-specific classes.
 *
 * @module abstracts
 * @exports {Class} AbstractProducer - Base class for message broker producers
 * @exports {Class} AbstractConsumer - Base class for message broker consumers
 * @exports {Class} AbstractCache - Base class for cache implementations
 * @exports {Class} AbstractMonitorService - Base class for backpressure monitoring services
 *
 * Example:
 *   const { AbstractProducer } = require("@/abstracts");
 */

module.exports = {
  AbstractProducer: require("./abstract-producer"),
  AbstractConsumer: require("./abstract-consumer"),
  AbstractCache: require("./abstract-cache"),
  AbstractMonitorService: require("./abstract-monitor-service"),
};
