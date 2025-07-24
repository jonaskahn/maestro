/**
 * Order State Mapper
 *
 * Provides utilities for converting between numeric and string order states.
 * Handles state validation and comparison operations for order processing.
 */

const ORDER_STATES = {
  PENDING: 1,
  COMPLETED: 2,
  FAILED: 3,
};

const NUMERIC_STATES = {
  PENDING: 1,
  COMPLETED: 2,
  FAILED: 3,
};

/**
 * OrderStateMapper class
 *
 * Static utility class for mapping between different order state representations
 * and performing state-related operations.
 */
class OrderStateMapper {
  /**
   * Converts string state to numeric state
   *
   * @param {string} stringState - The string representation of the state
   * @returns {number} The numeric state
   * @throws {Error} If the state is unknown
   */
  static toNumericState(stringState) {
    switch (stringState) {
      case ORDER_STATES.PENDING:
        return NUMERIC_STATES.PENDING;
      case ORDER_STATES.COMPLETED:
        return NUMERIC_STATES.COMPLETED;
      case ORDER_STATES.FAILED:
        return NUMERIC_STATES.FAILED;
      default:
        throw new Error(`Unknown state: ${stringState}`);
    }
  }

  /**
   * Converts numeric state to string state
   *
   * @param {number} numericState - The numeric representation of the state
   * @returns {string} The string state
   */
  static toStringState(numericState) {
    switch (numericState) {
      case NUMERIC_STATES.PENDING:
        return ORDER_STATES.PENDING;
      case NUMERIC_STATES.COMPLETED:
        return ORDER_STATES.COMPLETED;
      case NUMERIC_STATES.FAILED:
        return ORDER_STATES.FAILED;
      default:
        return ORDER_STATES.PROCESSING;
    }
  }

  /**
   * Checks if a state represents a completed order
   *
   * @param {number|string} state - The state to check
   * @returns {boolean} True if the state represents a completed order
   */
  static isCompleted(state) {
    return state === NUMERIC_STATES.COMPLETED || state === ORDER_STATES.COMPLETED;
  }

  /**
   * Checks if a state represents a pending order
   *
   * @param {number|string} state - The state to check
   * @returns {boolean} True if the state represents a pending order
   */
  static isPending(state) {
    return state === NUMERIC_STATES.PENDING || state === ORDER_STATES.PENDING;
  }

  /**
   * Checks if a state represents a failed order
   *
   * @param {number|string} state - The state to check
   * @returns {boolean} True if the state represents a failed order
   */
  static isFailed(state) {
    return state === NUMERIC_STATES.FAILED || state === ORDER_STATES.FAILED;
  }
}

module.exports = OrderStateMapper;
