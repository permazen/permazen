
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

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
     * Normally this method should only be invoked once, and the result cached, because evaluation could have side effects.
     * </p>
     *
     * @param session parse session
     * @return the evaluated result
     */
    Object get(ParseSession session);

    /**
     * Get the type of this value without evaluating it.
     *
     * <p>
     * This should perform a best-effort attempt to determine the type, but should not invoke {@link #get get()}.
     * If the type is unknown, {@code Object.class} should be returned.
     *
     * @param session parse session
     * @return the expected type of the evaluated result
     */
    Class<?> getType(ParseSession session);

    /**
     * Evaluate this value, verify that it is not null, and return it.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @return the evaluated result
     * @throws EvalException if this value is null
     */
    Object checkNotNull(ParseSession session, String operation);

    /**
     * Evaluate this value, verify that it has boolean type, and return it.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @return the evaluated boolean value
     * @throws EvalException if this value is not boolean
     */
    boolean checkBoolean(ParseSession session, String operation);

    /**
     * Evaluate this value, verify that it has numeric type (i.e., {@link Number}), and return it.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @return the evaluated numeric value
     * @throws EvalException if this value is not numeric
     */
    Number checkNumeric(ParseSession session, String operation);

    /**
     * Evaluate this value, verify that it has integral type (i.e., byte, char, short, or int), and return it.
     * Return its integer value.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @return the evaluated integer value
     * @throws EvalException if this value is not integral
     */
    int checkIntegral(ParseSession session, String operation);

    /**
     * Evaluate this value, verify that it has the expected type (or any sub-type), and return it.
     * Return its value cast to that type.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @param type expected type
     * @param <T> expected type
     * @return the evaluated value cast to {@code type}
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
     * @return negated result
     * @throws EvalException if this value is not numeric
     */
    Value negate(ParseSession session);

    /**
     * Bitwise invert this value.
     *
     * @param session current session
     * @return inverted result
     * @throws EvalException if this value is not numeric
     */
    Value invert(ParseSession session);

    /**
     * Multiply this value.
     *
     * @param session current session
     * @param that multiplicand
     * @return multiplied result
     * @throws EvalException if value(s) are not numeric
     */
    Value multiply(ParseSession session, Value that);

    /**
     * Divide this value.
     *
     * @param session current session
     * @param that divisor
     * @return divided result
     * @throws EvalException if value(s) are not numeric
     */
    Value divide(ParseSession session, Value that);

    /**
     * Modulo this value.
     *
     * @param session current session
     * @param that divisor
     * @return remaindered result
     * @throws EvalException if value(s) are not numeric
     */
    Value mod(ParseSession session, Value that);

    /**
     * Add or concatenate this value.
     *
     * @param session current session
     * @param that addend
     * @return added result
     * @throws EvalException if value(s) are not numeric or {@link String}
     */
    Value add(ParseSession session, Value that);

    /**
     * Subtract this value. Also supports {@link java.util.Set} and {@link java.util.NavigableSet} difference.
     *
     * @param session current session
     * @param that subtrahend
     * @return subtracted result
     * @throws EvalException if value(s) are not numeric or {@link java.util.Set}
     */
    Value subtract(ParseSession session, Value that);

    /**
     * Left shift this value.
     *
     * @param session current session
     * @param arg shift amount
     * @return shifted result
     * @throws EvalException if value(s) are not numeric
     */
    Value lshift(ParseSession session, Value arg);

    /**
     * Right shift this value.
     *
     * @param session current session
     * @param arg shift amount
     * @return shifted result
     * @throws EvalException if value(s) are not numeric
     */
    Value rshift(ParseSession session, Value arg);

    /**
     * Unsigned right shift this value.
     *
     * @param session current session
     * @param arg shift amount
     * @return shifted result
     * @throws EvalException if value(s) are not numeric
     */
    Value urshift(ParseSession session, Value arg);

    /**
     * And this value. Also supports {@link java.util.Set} and {@link java.util.NavigableSet} intersection.
     *
     * @param session current session
     * @param that and value
     * @return and'ed result
     * @throws EvalException if value(s) are not numeric, boolean, or {@link java.util.Set}
     */
    Value and(ParseSession session, Value that);

    /**
     * Or this value. Also supports {@link java.util.Set} and {@link java.util.NavigableSet} union.
     *
     * @param session current session
     * @param that or value
     * @return or'ed result
     * @throws EvalException if value(s) are not numeric, boolean, or {@link java.util.Set}
     */
    Value or(ParseSession session, Value that);

    /**
     * Xor this value. Also supports {@link java.util.Set} and {@link java.util.NavigableSet} symmetric difference.
     *
     * @param session current session
     * @param that xor value
     * @return exclusive or'ed result
     * @throws EvalException if value(s) are not numeric or boolean
     */
    Value xor(ParseSession session, Value that);

    /**
     * Ordered comparison to another value. Supports numeric and {@link Comparable} comparison.
     *
     * @param session current session
     * @param that value to compare to
     * @param mask bit mask with bits {@link #LT}, {@link #GT}, and/or {@link #EQ}
     * @return boolean value which will be true if the comparison is true
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
    class GetFunction implements Function<Value, Object> {

        private final ParseSession session;

        public GetFunction(ParseSession session) {
            Preconditions.checkArgument(session != null, "null session");
            this.session = session;
        }

        @Override
        public Object apply(Value item) {
            return item.get(this.session);
        }
    }
}

