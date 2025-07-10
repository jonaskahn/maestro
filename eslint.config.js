import js from "@eslint/js";
import prettier from "eslint-config-prettier";

export default [
  js.configs.recommended,
  prettier,
  {
    files: ["**/*.js"],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "module",
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
