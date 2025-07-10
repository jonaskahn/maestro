/**
 * Order State Mapper
 *
 * Converts between numeric and string order states.
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

class OrderStateMapper {
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

  static isCompleted(state) {
    return state === NUMERIC_STATES.COMPLETED || state === ORDER_STATES.COMPLETED;
  }

  static isPending(state) {
    return state === NUMERIC_STATES.PENDING || state === ORDER_STATES.PENDING;
  }

  static isFailed(state) {
    return state === NUMERIC_STATES.FAILED || state === ORDER_STATES.FAILED;
  }
}

module.exports = OrderStateMapper;
