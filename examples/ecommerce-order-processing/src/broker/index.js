/**
 * Broker Components Export
 *
 * Exports the OrderConsumer and OrderProducer components for the ecommerce order processing example.
 * These components handle the message queue communication for order processing.
 */

const OrderConsumer = require("./consumer");
const OrderProducer = require("./producer");

module.exports = {
  OrderConsumer,
  OrderProducer,
};
