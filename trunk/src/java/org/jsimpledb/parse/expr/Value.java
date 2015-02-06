
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import com.google.common.base.Function;

import org.jsimpledb.parse.ParseSession;

/**
 * Holds a value for use during expression evaluation.
 */
public interface Value {

    /** Flag mask value for use with {@link #compare compare()}. */
    int LT = 0x01;

    /** Flag mask value for use with {@link #compare compare()}. */
    int GT = 0x02;

    /** Flag mask value for use with {@link #compare compare()}. */
    int EQ = 0x04;

    /**
     * Special value that can be used to indicate "no value" in certain situations, such as return value
     * from a method returning void. Actually evaluates to null.
     */
    Value NO_VALUE = new ConstValue(null);

    /**
     * Evaluate this value within the given context.
     *
     * <p>
     * Normally this method should only be invoked once, and the result cached, because could have side effects.
     * </p>
     */
    Object get(ParseSession session);

    /**
     * Evaluate this value, verify that it is not null, and return it.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @throws EvalException if this value is null
     */
    Object checkNotNull(ParseSession session, String operation);

    /**
     * Evaluate this value, verify that it has boolean type, and return it.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @return boolean value
     * @throws EvalException if this value is not boolean
     */
    boolean checkBoolean(ParseSession session, String operation);

    /**
     * Evaluate this value, verify that it has numeric type (i.e., {@link Number}), and return it.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @return numeric value
     * @throws EvalException if this value is not numeric
     */
    Number checkNumeric(ParseSession session, String operation);

    /**
     * Evaluate this value, verify that it has integral type (i.e., byte, char, short, or int), and return it.
     * Return its integer value.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @return integer value
     * @throws EvalException if this value is not integral
     */
    int checkIntegral(ParseSession session, String operation);

    /**
     * Evaluate this value, verify that it has the expected type (or any sub-type), and return it.
     * Return its value cast to that type.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @return typed value
     * @throws EvalException if this value does not have the expected type
     */
    <T> T checkType(ParseSession session, String operation, Class<T> type);

    /**
     * Increment/decrement this value. Also supports {@link java.math.BigInteger} and {@link java.math.BigDecimal}.
     * This value must be an {@link LValue}.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @param increment true to increment, false to decrement
     * @return the adjusted value (which will not be an {@link LValue})
     * @throws EvalException if this value is not an {@link LValue}
     * @throws EvalException if this value is not numeric, {@link java.math.BigInteger} or {@link java.math.BigDecimal}
     */
    Value xxcrement(ParseSession session, String operation, boolean increment);

    /**
     * Negate this value.
     *
     * @param session current session
     * @throws EvalException if this value is not numeric
     */
    Value negate(ParseSession session);

    /**
     * Bitwise invert this value.
     *
     * @param session current session
     * @throws EvalException if this value is not numeric
     */
    Value invert(ParseSession session);

    /**
     * Multiply this value.
     *
     * @param session current session
     * @param that multiplicand
     * @throws EvalException if value(s) are not numeric
     */
    Value multiply(ParseSession session, Value that);

    /**
     * Divide this value.
     *
     * @param session current session
     * @param that divisor
     * @throws EvalException if value(s) are not numeric
     */
    Value divide(ParseSession session, Value that);

    /**
     * Modulo this value.
     *
     * @param session current session
     * @param that divisor
     * @throws EvalException if value(s) are not numeric
     */
    Value mod(ParseSession session, Value that);

    /**
     * Add or concatenate this value.
     *
     * @param session current session
     * @param that addend
     * @throws EvalException if value(s) are not numeric or {@link String}
     */
    Value add(ParseSession session, Value that);

    /**
     * Subtract this value. Also supports {@link java.util.Set} and {@link java.util.NavigableSet} difference.
     *
     * @param session current session
     * @param that subtrahend
     * @throws EvalException if value(s) are not numeric or {@link java.util.Set}
     */
    Value subtract(ParseSession session, Value that);

    /**
     * Left shift this value.
     *
     * @param session current session
     * @param arg shift amount
     * @throws EvalException if value(s) are not numeric
     */
    Value lshift(ParseSession session, Value arg);

    /**
     * Right shift this value.
     *
     * @param session current session
     * @param arg shift amount
     * @throws EvalException if value(s) are not numeric
     */
    Value rshift(ParseSession session, Value arg);

    /**
     * Unsigned right shift this value.
     *
     * @param session current session
     * @param arg shift amount
     * @throws EvalException if value(s) are not numeric
     */
    Value urshift(ParseSession session, Value arg);

    /**
     * And this value. Also supports {@link java.util.Set} and {@link java.util.NavigableSet} intersection.
     *
     * @param session current session
     * @param that and value
     * @throws EvalException if value(s) are not numeric, boolean, or {@link java.util.Set}
     */
    Value and(ParseSession session, Value that);

    /**
     * Or this value. Also supports {@link java.util.Set} and {@link java.util.NavigableSet} union.
     *
     * @param session current session
     * @param that or value
     * @throws EvalException if value(s) are not numeric, boolean, or {@link java.util.Set}
     */
    Value or(ParseSession session, Value that);

    /**
     * Xor this value. Also supports {@link java.util.Set} and {@link java.util.NavigableSet} symmetric difference.
     *
     * @param session current session
     * @param that xor value
     * @throws EvalException if value(s) are not numeric or boolean
     */
    Value xor(ParseSession session, Value that);

    /**
     * Compare to another value, returning boolean. Also supports {@link Comparable} comparison.
     *
     * @param session current session
     * @param that value to compare to
     * @param mask bit mask with bits {@link #LT}, {@link #GT}, and/or {@link #EQ}
     * @throws EvalException if value(s) are not numeric or mutually {@link Comparable}
     */
    Value compare(ParseSession session, Value that, int mask);

    /**
     * Verify that this instance is actually an {@link LValue}.
     *
     * @param operation description of operation for error messages
     * @return this instance cast to {@link LValue}
     * @throws EvalException if this instance is not an {@link LValue}
     */
    LValue asLValue(String operation);

    /**
     * Function that evaluates (i.e., invokes {@link #get Value.get()}) on its argument.
     */
    public static class GetFunction implements Function<Value, Object> {

        private final ParseSession session;

        public GetFunction(ParseSession session) {
            if (session == null)
                throw new IllegalArgumentException("null session");
            this.session = session;
        }

        @Override
        public Object apply(Value item) {
            return item.get(this.session);
        }
    }
}

