/**
 * Jest setup file for @jonaskahn/maestro
 *
 * This file runs before tests are executed, setting up the environment
 * and applying global configurations.
 */

require("events").EventEmitter.defaultMaxListeners = 100;

// Silence console logs during tests unless explicitly needed
global.console = {
  ...global.console,
  log: jest.fn(),
  debug: jest.fn(),
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
};

process.env.MO_TASK_PROCESSING_BASE_TTL_MS = "10000";
process.env.MO_DELAY_BASE_TIMEOUT_MS = 0;
