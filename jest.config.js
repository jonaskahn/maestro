/**
 * Jest configuration for @jonaskahn/maestro
 */
module.exports = {
  // The test environment that will be used for testing
  testEnvironment: "node",

  // The glob patterns Jest uses to detect test files
  testMatch: ["**/tests/**/*.test.js"],

  // An array of regexp pattern strings that are matched against all test paths
  testPathIgnorePatterns: ["/node_modules/"],

  // Indicates whether each individual test should be reported during the run
  verbose: true,

  // Automatically clear mock calls and instances between every test
  clearMocks: true,

  // The directory where Jest should output its coverage files
  coverageDirectory: "coverage",

  // Indicates which provider should be used to instrument code for coverage
  coverageProvider: "v8",

  // A list of reporter names that Jest uses when writing coverage reports
  coverageReporters: ["text", "lcov", "clover", "html", "json-summary"],

  // An array of glob patterns indicating a set of files for which coverage information should be collected
  collectCoverageFrom: ["src/**/*.js", "!**/node_modules/**", "!**/vendor/**", "!**/__tests__/**", "!**/coverage/**"],

  // The threshold enforcement for coverage results
  coverageThreshold: {
    global: {
      branches: 70,
      functions: 80,
      lines: 80,
      statements: 80,
    },
  },

  // Setup files to run before tests
  setupFilesAfterEnv: ["<rootDir>/jest.setup.js"],

  // A map from regular expressions to module names that allow to stub out resources
  moduleNameMapper: {
    "^@/(.*)$": "<rootDir>/src/$1",
  },

  // Setup for @/ import resolution
  modulePaths: ["<rootDir>"],

  // Cleanup test environment after each test
  testEnvironmentOptions: {
    teardown: {
      // Clean up any timers after each test
      timeoutMs: 1000,
    },
  },

  // Provide a timeout value for tests
  testTimeout: 10000,

  // Extension mapping for imports
  moduleFileExtensions: ["js", "json", "node"],
};
