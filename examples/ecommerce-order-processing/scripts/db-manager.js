/**
 * Database Manager Script
 *
 * Unified script for database management tasks including:
 * - Initialization and schema setup
 * - Test data generation
 * - Database reset and cleanup
 * - Statistics and reporting
 * - Data destruction (clear or drop)
 */

const { MongoClient } = require("mongodb");
const database = require("../src/database/mongodb-client");
const Logger = require("../src/utils/logger");
const readline = require("readline");

const DB_CONFIG = {
  url: database.DB_CONFIG?.url || "mongodb://localhost:27017/myapp?authSource=admin",
  dbName: database.DB_CONFIG?.dbName || "myapp",
  collections: database.DB_CONFIG?.collections || {
    orders: "orders",
  },
};

/**
 * Initialize the database with required schema and test data
 * @param {number} testDataCount - Number of test orders to generate (0 to skip)
 */
async function initializeDatabase(testDataCount = 0) {
  try {
    await database.connect();
    await database.initializeSchema();

    if (testDataCount > 0) {
      await generateTestOrders(testDataCount);
    }

    Logger.success("Database initialized successfully");
    return true;
  } catch (error) {
    Logger.error("Failed to initialize database", error);
    throw error;
  }
}

/**
 * Generate a specified number of test orders
 * @param {number} count - Number of orders to generate
 * @returns {Promise<Array>} Generated orders
 */
async function generateTestOrders(count = 10) {
  try {
    if (!database.isConnected) {
      await database.connect();
    }

    Logger.info(`Generating ${count} test orders...`);
    const orders = await database.generateNewOrders(count);

    Logger.success(`Successfully generated ${orders.length} test orders`);
    return orders;
  } catch (error) {
    Logger.error("Failed to generate test orders", error);
    throw error;
  }
}

/**
 * Reset the database (for testing/development purposes)
 */
async function resetDatabase() {
  try {
    await database.connect();
    await database.reset();
    Logger.success("Database reset successfully");
    return true;
  } catch (error) {
    Logger.error("Failed to reset database", error);
    throw error;
  }
}

/**
 * Clean up old orders
 */
async function cleanupDatabase() {
  try {
    await database.connect();
    await database.cleanupOldOrders();
    Logger.success("Database cleanup completed");
    return true;
  } catch (error) {
    Logger.error("Failed to clean up database", error);
    throw error;
  }
}

/**
 * Show database statistics
 */
async function showStats() {
  try {
    await database.connect();
    const stats = await database.getStats();
    Logger.info("Database Statistics:");
    Logger.info(JSON.stringify(stats, null, 2));
    return stats;
  } catch (error) {
    Logger.error("Failed to get database stats", error);
    throw error;
  }
}

/**
 * Drop all collections in the database
 */
async function dropCollections() {
  let client;

  try {
    Logger.info("Dropping all collections");

    // Connect directly to MongoDB
    client = new MongoClient(DB_CONFIG.url);
    await client.connect();

    const db = client.db(DB_CONFIG.dbName);
    const collections = await db.listCollections().toArray();

    for (const collection of collections) {
      try {
        await db.dropCollection(collection.name);
        Logger.success(`Dropped collection: ${collection.name}`);
      } catch (error) {
        if (error.code === 26) {
          Logger.warn(`Collection ${collection.name} does not exist`);
        } else {
          throw error;
        }
      }
    }

    Logger.success("All collections dropped");
    return true;
  } catch (error) {
    Logger.error("Failed to drop collections", error);
    throw error;
  } finally {
    if (client) {
      await client.close();
    }
  }
}

/**
 * Clear all data from all collections
 */
async function clearAllData() {
  let client;

  try {
    Logger.info("Clearing ALL data from ALL collections");

    // Connect directly to MongoDB
    client = new MongoClient(DB_CONFIG.url);
    await client.connect();

    const db = client.db(DB_CONFIG.dbName);
    const collections = await db.listCollections().toArray();

    let totalDeleted = 0;
    const results = {};

    for (const collection of collections) {
      const collectionName = collection.name;

      // Skip system collections
      if (collectionName.startsWith("system.")) {
        continue;
      }

      try {
        const coll = db.collection(collectionName);
        const result = await coll.deleteMany({});

        if (result.deletedCount > 0) {
          results[collectionName] = result.deletedCount;
          totalDeleted += result.deletedCount;
          Logger.info(`Deleted ${result.deletedCount} records from ${collectionName}`);
        }
      } catch (error) {
        Logger.warn(`Error clearing collection ${collectionName}: ${error.message}`);
      }
    }

    Logger.info(`Cleanup Summary:`);
    Logger.info(`Total records deleted: ${totalDeleted}`);

    if (totalDeleted === 0) {
      Logger.info("Database was already empty");
    } else {
      Logger.success("Database completely cleared");
    }

    return { results, totalDeleted };
  } catch (error) {
    Logger.error("Failed to clear data", error);
    throw error;
  } finally {
    if (client) {
      await client.close();
    }
  }
}

/**
 * Destroy the database - either clear data or drop collections
 */
async function destroyDatabase(mode = "clear") {
  try {
    Logger.info("Database Destruction");
    Logger.separator();
    Logger.info(`Mode: ${mode === "drop" ? "DROP collections" : "CLEAR data"}`);

    if (mode === "drop") {
      await dropCollections();
    } else {
      await clearAllData();
    }

    Logger.success("Database destruction completed");
    return true;
  } catch (error) {
    Logger.error("Database destruction failed", error);
    throw error;
  }
}

/**
 * Interactive confirmation for destructive operations
 */
function confirmAction(actionType) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  let prompt;

  switch (actionType) {
    case "clear":
      prompt = "Are you sure you want to CLEAR all database data? (yes/no): ";
      break;
    case "drop":
      prompt = "Are you sure you want to DROP all database collections? (yes/no): ";
      break;
    case "reset":
      prompt = "Are you sure you want to RESET the database? (yes/no): ";
      break;
    default:
      prompt = `Are you sure you want to perform '${actionType}'? (yes/no): `;
  }

  return new Promise(resolve => {
    rl.question(prompt, answer => {
      rl.close();
      if (answer.toLowerCase() === "yes") {
        Logger.info(`Proceeding with database ${actionType}`);
        resolve(true);
      } else {
        Logger.info(`Database ${actionType} cancelled`);
        resolve(false);
      }
    });
  });
}

// Main command handler
async function handleCommand(command, args) {
  try {
    let orderCount;
    let confirmed;

    switch (command) {
      case "init":
        await initializeDatabase();
        break;

      case "generate":
        orderCount = parseInt(args[0], 10) || 10;
        await generateTestOrders(orderCount);
        break;

      case "init-with-data":
        orderCount = parseInt(args[0], 10) || 50;
        await initializeDatabase(orderCount);
        break;

      case "reset":
        if (args.includes("--force")) {
          await resetDatabase();
        } else {
          confirmed = await confirmAction("reset");
          if (confirmed) {
            await resetDatabase();
          }
        }
        break;

      case "cleanup":
        await cleanupDatabase();
        break;

      case "stats":
        await showStats();
        break;

      case "destroy":
      case "clear":
        if (args.includes("--force")) {
          await destroyDatabase("clear");
        } else {
          confirmed = await confirmAction("clear");
          if (confirmed) {
            await destroyDatabase("clear");
          }
        }
        break;

      case "drop":
        if (args.includes("--force")) {
          await destroyDatabase("drop");
        } else {
          confirmed = await confirmAction("drop");
          if (confirmed) {
            await destroyDatabase("drop");
          }
        }
        break;

      default:
        Logger.error(`Unknown command: ${command}`);
        showHelp();
        return false;
    }

    return true;
  } catch (error) {
    Logger.error(`Command '${command}' failed:`, error);
    return false;
  }
}

// Display help information
function showHelp() {
  Logger.info("Database Manager - Available commands:");
  Logger.info("  init             - Initialize database schema and indexes");
  Logger.info("  init-with-data N - Initialize database and generate N test orders (default: 50)");
  Logger.info("  generate N       - Generate N test orders (default: 10)");
  Logger.info("  stats            - Show database statistics");
  Logger.info("  cleanup          - Clean up old orders");
  Logger.info("  reset            - Reset database (clear and recreate schema)");
  Logger.info("  clear            - Clear all data but keep collections");
  Logger.info("  drop             - Drop all collections");
  Logger.info("");
  Logger.info("Options:");
  Logger.info("  --force          - Skip confirmation for destructive actions");
}

// If this script is run directly
if (require.main === module) {
  const args = process.argv.slice(2);
  const command = args.shift() || "help";

  if (command === "help") {
    showHelp();
  }

  // Execute the command and close connections when done
  (async () => {
    try {
      await handleCommand(command, args);

      // Only disconnect if we're using the database module
      if (database.isConnected) {
        await database.disconnect();
      }
    } catch (error) {
      Logger.error("Script execution failed", error);
    }
  })();
}

module.exports = {
  initializeDatabase,
  generateTestOrders,
  resetDatabase,
  cleanupDatabase,
  destroyDatabase,
  showStats,
  dropCollections,
  clearAllData,
};
