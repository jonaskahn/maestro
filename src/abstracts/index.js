/**
 * @license
 * Copyleft (c) 2025 Jonas Kahn. All rights are not reserved.
 *
 * This source code is licensed under the Apache License 2.0 found in the
 * LICENSE file in the root directory of this source tree.
 *
 * Central export file for all abstract base classes. Importing from
 * '@/abstracts' will give you convenient access to the core abstractions
 * used throughout the Maestro code-base.
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
