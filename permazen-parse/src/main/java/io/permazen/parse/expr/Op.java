
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import io.permazen.parse.ParseSession;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

/**
 * Java expression operators.
 */
public enum Op {

// NOTE: operations without overridden apply() methods are handled in the corresponding parser class

// Array access

    ARRAY_ACCESS(2, "[]") {
        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Value apply(ParseSession session, final Value targetValue, final Value itemValue) {

            // Check null
            final Object target = targetValue.checkNotNull(session, "array access");

            // Handle map
            if (target instanceof Map) {
                final Map map = (Map)target;
                final Object key = targetValue.get(session);
                return new AbstractLValue() {
                    @Override
                    public Object get(ParseSession session) {
                        return map.get(key);
                    }
                    @Override
                    public void set(ParseSession session, Value value) {
                        final Object obj = value.get(session);
                        try {
                            map.put(key, obj);
                        } catch (RuntimeException e) {
                            throw new EvalException("invalid map put operation"
                              + (e.getMessage() != null ? ": " + e.getMessage() : ""), e);
                        }
                    }
                };
            }

            // Handle list
            if (target instanceof List) {
                final List list = (List)target;
                final int index = itemValue.checkIntegral(session, "list index");
                return new AbstractLValue() {
                    @Override
                    public Object get(ParseSession session) {
                        return list.get(index);
                    }
                    @Override
                    public void set(ParseSession session, Value value) {
                        final Object obj = value.get(session);
                        try {
                            list.set(index, obj);
                        } catch (RuntimeException e) {
                            throw new EvalException("invalid list set operation"
                              + (e.getMessage() != null ? ": " + e.getMessage() : ""), e);
                        }
                    }
                };
            }

            // Assume it must be an array
            final int index = itemValue.checkIntegral(session, "array index");
            return new AbstractLValue() {
                @Override
                public Object get(ParseSession session) {
                    try {
                        return Array.get(target, index);
                    } catch (IllegalArgumentException e) {
                        throw new EvalException("invalid array access operation on non-array of type `"
                          + target.getClass().getName() + "'", e);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        throw new EvalException("array index out of bounds"
                          + (e.getMessage() != null ? ": " + e.getMessage() : ""), e);
                    }
                }
                @Override
                public void set(ParseSession session, Value value) {
                    final Object obj = value.get(session);
                    try {
                        Array.set(target, index, obj);
                    } catch (IllegalArgumentException e) {
                        throw new EvalException("invalid array set operation"
                          + (e.getMessage() != null ? ": " + e.getMessage() : ""), e);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        throw new EvalException("array index out of bounds"
                          + (e.getMessage() != null ? ": " + e.getMessage() : ""), e);
                    }
                }
            };
        }
    },

// Member access

    MEMBER_ACCESS(2, "."),

// Invoke method

    INVOKE_METHOD(2, "()"),

// Unary

    POST_INCREMENT(1, "++"),
    POST_DECREMENT(1, "--"),
    PRE_INCREMENT(1, "++"),
    PRE_DECREMENT(1, "--"),

    UNARY_PLUS(1, "+") {
        @Override
        Value apply(ParseSession session, Value value) {
            return new ConstValue(value.checkNumeric(session, "unary plus"));        // note: returned value is not an L-value
        }
    },

    UNARY_MINUS(1, "-") {
        @Override
        Value apply(ParseSession session, Value value) {
            return value.negate(session);
        }
    },

    LOGICAL_NOT(1, "!") {
        @Override
        Value apply(ParseSession session, Value value) {
            return new ConstValue(!value.checkBoolean(session, "logical `not'"));
        }
    },

    INVERT(1, "~") {
        @Override
        Value apply(ParseSession session, Value value) {
            return value.invert(session);
        }
    },

// Cast

    CAST(1, "()") {
        // Handled by CastExprParser
    },

// Multiplicative

    MULTIPLY(2, "*") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            return lhs.multiply(session, rhs);
        }
    },

    DIVIDE(2, "/") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            return lhs.divide(session, rhs);
        }
    },

    MODULO(2, "%") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            return lhs.mod(session, rhs);
        }
    },

// Additive

    PLUS(2, "+") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            return lhs.add(session, rhs);
        }
    },

    MINUS(2, "-") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            return lhs.subtract(session, rhs);
        }
    },

// Shift

    LSHIFT(2, "<<") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            return lhs.lshift(session, rhs);
        }
    },

    RSHIFT(2, ">>") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            return lhs.rshift(session, rhs);
        }
    },

    URSHIFT(2, ">>>") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            return lhs.urshift(session, rhs);
        }
    },

// Relational

    LT(2, "<") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            return lhs.compare(session, rhs, Value.LT);
        }
    },

    GT(2, ">") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            return lhs.compare(session, rhs, Value.GT);
        }
    },

    LTEQ(2, "<=") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            return lhs.compare(session, rhs, Value.LT | Value.EQ);
        }
    },

    GTEQ(2, ">=") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            return lhs.compare(session, rhs, Value.GT | Value.EQ);
        }
    },

    INSTANCEOF(2, "instanceof") {
        // Handled by InstanceofParser
    },

// Equality

    EQUAL(2, "==") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            final Object lval = lhs.get(session);
            final Object rval = rhs.get(session);
            if (lval == null || rval == null)
                return new ConstValue(lval == rval);
            if (lval instanceof Number || rval instanceof Number)
                return lhs.compare(session, rhs, Value.EQ);
            return new ConstValue(lval.equals(rval));
        }
    },

    NOT_EQUAL(2, "!=") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            return new ConstValue(!(Boolean)Op.EQUAL.apply(session, lhs, rhs).get(session));
        }
    },

// Bitwise

    AND(2, "&") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            return lhs.and(session, rhs);
        }
    },

    OR(2, "|") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            return lhs.or(session, rhs);
        }
    },

    XOR(2, "^") {
        @Override
        Value apply(ParseSession session, Value lhs, Value rhs) {
            return lhs.xor(session, rhs);
        }
    },

// Logical

    LOGICAL_AND(2, "&&"),
    LOGICAL_OR(2, "||"),

// Conditional

    CONDITIONAL(3, "?:"),

// Assignment

    EQUALS(2, "="),
    PLUS_EQUALS(2, "+="),
    MINUS_EQUALS(2, "-="),
    MULTIPLY_EQUALS(2, "*="),
    DIVIDE_EQUALS(2, "/="),
    MODULO_EQUALS(2, "%="),
    AND_EQUALS(2, "&="),
    XOR_EQUALS(2, "^="),
    OR_EQUALS(2, "|="),
    LSHIFT_EQUALS(2, "<<="),
    RSHIFT_EQUALS(2, ">>="),
    URSHIFT_EQUALS(2, ">>>=");

// Fields

    private final int arity;
    private final String symbol;

// Constructors

    Op(int arity, String symbol) {
        this.arity = arity;
        this.symbol = symbol;
    }

// Methods

    /**
     * Get the arity of this symbol.
     *
     * @return symbol arity
     */
    public int getArity() {
        return this.arity;
    }

    /**
     * Get the symbol associated with this operator.
     *
     * @return operator symbol
     */
    public String getSymbol() {
        return this.symbol;
    }

    /**
     * Apply this operator to the given parameters.
     *
     * @param session current session
     * @param args operator arguments
     * @return result of operation
     * @throws IllegalArgumentException if {@code args} contains inappropriate value(s)
     * @throws IllegalArgumentException if the length of {@code args} does not match this operator
     */
    public Value apply(ParseSession session, Value... args) {
        if (args.length != this.arity)
            throw new EvalException("wrong number of arguments " + args.length + " != " + this.arity + " given to " + this);
        switch (args.length) {
        case 1:
            return this.apply(session, args[0]);
        case 2:
            return this.apply(session, args[0], args[1]);
        case 3:
            return this.apply(session, args[0], args[1], args[2]);
        default:
            throw new RuntimeException("internal error");
        }
    }

    /**
     * Get the {@link Op} corresponding to the given symbol.
     * Note: some symbols correspond to multiple {@link Op}s:
     * <ul>
     *  <li>For {@code +} or {@code -}, the binary operator is returned</li>
     *  <li>For {@code ++} or {@code --}, the post-increment operator is returned</li>
     * </ul>
     *
     * @param symbol symbol
     * @return corresponding operator
     * @throws IllegalArgumentException if no such {@link Op} exists
     */
    public static Op forSymbol(String symbol) {
        switch (symbol) {
        case "+":
            return Op.PLUS;
        case "-":
            return Op.MINUS;
        case "++":
            return Op.POST_INCREMENT;
        case "--":
            return Op.POST_DECREMENT;
        default:
            for (Op op : Op.values()) {
                if (op.symbol.equals(symbol))
                    return op;
            }
            throw new IllegalArgumentException("no operation with symbol `" + symbol + "' exists");
        }
    }

    Value apply(ParseSession session, Value arg) {
        throw new UnsupportedOperationException();
    }

    Value apply(ParseSession session, Value arg1, Value arg2) {
        throw new UnsupportedOperationException();
    }

    Value apply(ParseSession session, Value arg1, Value arg2, Value arg3) {
        throw new UnsupportedOperationException();
    }
}

