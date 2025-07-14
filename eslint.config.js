const js = require("@eslint/js");
const prettier = require("eslint-config-prettier");

module.exports = [
  js.configs.recommended,
  prettier,
  {
    ignores: [
      // Dependencies
      "node_modules/",
      "npm-debug.log*",
      "yarn-debug.log*",
      "yarn-error.log*",

      // Build outputs
      "dist/",
      "build/",
      "coverage/",

      // Environment files
      ".env",
      ".env.local",
      ".env.development.local",
      ".env.test.local",
      ".env.production.local",

      // Docker
      ".dockerignore",
      "Dockerfile*",

      // Git
      ".git/",
      ".gitignore",

      // IDE
      ".vscode/",
      ".idea/",
      "*.swp",
      "*.swo",

      // Logs
      "logs/",
      "*.log",

      // Runtime data
      "pids/",
      "*.pid",
      "*.seed",
      "*.pid.lock",

      // Optional npm cache directory
      ".npm",

      // Optional eslint cache
      ".eslintcache",

      // Yarn Integrity file
      ".yarn-integrity",

      // Lock files
      "package-lock.json",
      "yarn.lock",

      // Tests (exclude during npm pack)
      "tests/",
      "jest.config.js",
      "jest.setup.js",
    ],
  },
  {
    files: ["**/*.js"],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "commonjs",
      globals: {
        console: "readonly",
        process: "readonly",
        Buffer: "readonly",
        __dirname: "readonly",
        __filename: "readonly",
        module: "readonly",
        require: "readonly",
        exports: "readonly",
        global: "readonly",
        setTimeout: "readonly",
        clearTimeout: "readonly",
        setInterval: "readonly",
        clearInterval: "readonly",
        crypto: "readonly",
      },
    },
    rules: {
      "no-unused-private-class-members": "off",
      "no-console": "off",
      "no-unused-vars": ["error", { argsIgnorePattern: "^_" }],
      "no-var": "error",
      "prefer-const": "error",
      "no-multiple-empty-lines": ["error", { max: 2, maxEOF: 1 }],
      "no-trailing-spaces": "error",
      "eol-last": "error",
      "no-process-exit": "warn",
      "no-path-concat": "error",
      "no-new-require": "error",
      "no-mixed-requires": "error",
      "no-buffer-constructor": "error",
      "handle-callback-err": "error",
      "no-throw-literal": "error",
      "prefer-template": "error",
      "object-shorthand": "error",
      "no-param-reassign": "error",
      "no-async-promise-executor": "error",
      "require-await": "warn",
    },
  },
  {
    files: ["*.test.js", "tests/**/*.js"],
    languageOptions: {
      globals: {
        describe: "readonly",
        it: "readonly",
        test: "readonly",
        expect: "readonly",
        beforeEach: "readonly",
        afterEach: "readonly",
        beforeAll: "readonly",
        afterAll: "readonly",
        jest: "readonly",
      },
    },
  },
  {
    files: ["src/**/*.js", "tests/**/*.js"],
    rules: {
      "no-process-exit": "off",
    },
  },
  {
    files: ["src/abstracts/**/*.js"],
    rules: {
      "no-unused-vars": [
        "error",
        {
          args: "none",
          argsIgnorePattern: "^_",
        },
      ],
    },
  },
];
