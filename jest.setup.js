/**
 * Jest setup file for @jonaskahn/maestro
 *
 * This file runs before tests are executed, setting up the environment
 * and applying global configurations.
 */

// Increase the default max listeners to fix memory leak warning
require("events").EventEmitter.defaultMaxListeners = 20;

// Silence console logs during tests unless explicitly needed
global.console = {
  ...global.console,
  log: jest.fn(),
  debug: jest.fn(),
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
};
