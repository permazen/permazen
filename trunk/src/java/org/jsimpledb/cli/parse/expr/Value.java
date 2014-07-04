
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;

import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.util.InstancePredicate;

/**
 * An evaluation-time value used in an expression.
 */
public class Value {

    static final int LT = 0x01;
    static final int GT = 0x02;
    static final int EQ = 0x04;

    static final Setter SELF = new Setter() {
        @Override
        public void set(Session session, Object value) {
            throw new RuntimeException("internal error");
        }
    };

    private final Object obj;
    private Setter setter;

    public Value(Object obj) {
        this(obj, null);
    }

    public Value(Object obj, Setter setter) {
        this.obj = obj;
        this.setter = setter == SELF ? (Setter)this : setter;
    }

    /**
     * Get the actual value.
     */
    public Object get(Session session) {
        return this.obj;
    }

    /**
     * Get the {@link Setter} associated with this value, if any.
     *
     * @return associated {@link Setter}, or null if there is none
     */
    public Setter getSetter() {
        return this.setter;
    }

    /**
     * Verify this value is not null.
     *
     * @param session current session
     * @param operation description of operation for error messages
     */
    public Object checkNotNull(Session session, String operation) {
        final Object value = this.get(session);
        if (value == null)
            throw new EvalException("invalid " + operation + " operation on null value");
        return value;
    }

    /**
     * Verify this instance has a boolean type.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @return boolean value
     */
    public boolean checkBoolean(Session session, String operation) {
        final Object value = this.checkNotNull(session, operation);
        if (!(value instanceof Boolean)) {
            throw new EvalException("invalid " + operation
              + " operation on non-boolean value of type " + value.getClass().getName());
        }
        return (Boolean)value;
    }

    /**
     * Verify this instance has a numeric type (i.e., {@link Number}).
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @return numeric value
     */
    public Number checkNumeric(Session session, String operation) {
        final Object value = this.checkNotNull(session, operation);
        if (!(value instanceof Number)) {
            throw new EvalException("invalid " + operation
              + " operation on non-numeric value of type " + value.getClass().getName());
        }
        return (Number)value;
    }

    /**
     * Increment/decrement this value. Instance must have a {@link Setter}.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @param increment true to increment, false to decrement
     * @return adjusted value
     */
    public Value increment(Session session, String operation, boolean increment) {
        this.verifySetter(operation);
        final int amount = increment ? 1 : -1;
        final Object value = this.get(session);
        Object num = value instanceof Character ? value : this.checkNumeric(session, operation);
        if (num instanceof Byte)
            num = (Byte)num + (byte)amount;
        else if (num instanceof Character)
            num = (Character)num + (char)amount;
        else if (num instanceof Short)
            num = (Short)num + (short)amount;
        else if (num instanceof Integer)
            num = (Integer)num + amount;
        else if (num instanceof Float)
            num = (Float)num + (float)amount;
        else if (num instanceof Long)
            num = (Long)num + (long)amount;
        else if (num instanceof Double)
            num = (Double)num + (double)amount;
        else if (num instanceof BigInteger)
            num = ((BigInteger)num).add(increment ? BigInteger.ONE : BigInteger.ONE.negate());
        else if (num instanceof BigDecimal)
            num = ((BigDecimal)num).add(increment ? BigDecimal.ONE : BigDecimal.ONE.negate());
        else
            throw new EvalException("invalid " + operation + " operation on value of type " + num.getClass().getName());
        this.setter.set(session, num);
        return new Value(num);
    }

    /**
     * Negate this value.
     *
     * @param session current session
     */
    public Value negate(Session session) {
        final Number num = this.promoteNumeric(session, "negate");
        if (num instanceof BigDecimal)
            return new Value(((BigDecimal)num).negate());
        if (num instanceof Double)
            return new Value(-(Double)num);
        if (num instanceof Float)
            return new Value(-(Float)num);
        if (num instanceof BigInteger)
            return new Value(((BigInteger)num).negate());
        if (num instanceof Long)
            return new Value(-(Long)num);
        if (num instanceof Integer)
            return new Value(-(Integer)num);
        throw new EvalException("invalid negate operation on value of type " + num.getClass().getName());
    }

    /**
     * Bitwise invert this value.
     *
     * @param session current session
     */
    public Value invert(Session session) {
        final Number num = this.promoteNumeric(session, "invert");
        if (num instanceof Integer)
            return new Value(~(Integer)num);
        if (num instanceof Long)
            return new Value(~(Long)num);
        throw new EvalException("invalid invert operation on value of type " + num.getClass().getName());
    }

    /**
     * Multiply this value.
     *
     * @param session current session
     */
    public Value multiply(Session session, Value that) {
        final Number lnum = this.promoteNumeric(session, "multiply", that);
        final Number rnum = that.promoteNumeric(session, "multiply", this);
        if (lnum instanceof BigDecimal)
            return new Value(((BigDecimal)lnum).multiply((BigDecimal)rnum));
        if (lnum instanceof Double)
            return new Value((Double)lnum * (Double)rnum);
        if (lnum instanceof Float)
            return new Value((Float)lnum * (Float)rnum);
        if (lnum instanceof BigInteger)
            return new Value(((BigInteger)lnum).multiply((BigInteger)rnum));
        if (lnum instanceof Long)
            return new Value((Long)lnum * (Long)rnum);
        if (lnum instanceof Integer)
            return new Value((Integer)lnum * (Integer)rnum);
        throw new EvalException("invalid multiply operation on values of type "
          + lnum.getClass().getName() + " and " + rnum.getClass().getName());
    }

    /**
     * Divide this value.
     *
     * @param session current session
     */
    public Value divide(Session session, Value that) {
        final Number lnum = this.promoteNumeric(session, "divide", that);
        final Number rnum = that.promoteNumeric(session, "divide", this);
        if (lnum instanceof BigDecimal)
            return new Value(((BigDecimal)lnum).divide((BigDecimal)rnum));
        if (lnum instanceof Double)
            return new Value((Double)lnum / (Double)rnum);
        if (lnum instanceof Float)
            return new Value((Float)lnum / (Float)rnum);
        if (lnum instanceof BigInteger)
            return new Value(((BigInteger)lnum).divide((BigInteger)rnum));
        if (lnum instanceof Long)
            return new Value((Long)lnum / (Long)rnum);
        if (lnum instanceof Integer)
            return new Value((Integer)lnum / (Integer)rnum);
        throw new EvalException("invalid divide operation on values of type "
          + lnum.getClass().getName() + " and " + rnum.getClass().getName());
    }

    /**
     * Modulo this value.
     *
     * @param session current session
     */
    public Value mod(Session session, Value that) {
        final Number lnum = this.promoteNumeric(session, "modulo", that);
        final Number rnum = that.promoteNumeric(session, "modulo", this);
        if (lnum instanceof Double)
            return new Value((Double)lnum % (Double)rnum);
        if (lnum instanceof Float)
            return new Value((Float)lnum % (Float)rnum);
        if (lnum instanceof BigInteger)
            return new Value(((BigInteger)lnum).mod((BigInteger)rnum));
        if (lnum instanceof Long)
            return new Value((Long)lnum % (Long)rnum);
        if (lnum instanceof Integer)
            return new Value((Integer)lnum % (Integer)rnum);
        throw new EvalException("invalid modulo operation on values of type "
          + lnum.getClass().getName() + " and " + rnum.getClass().getName());
    }

    /**
     * Add this value.
     *
     * @param session current session
     */
    public Value add(Session session, Value that) {

        // Handle String concatenation
        final Object thisValue = this.get(session);
        final Object thatValue = that.get(session);
        if (thisValue == null || thatValue == null || thisValue instanceof String || thatValue instanceof String)
            return new Value(String.valueOf(thisValue) + String.valueOf(thatValue));

        // Handle numeric
        final Number lnum = this.promoteNumeric(session, "add", that);
        final Number rnum = that.promoteNumeric(session, "add", this);
        if (lnum instanceof BigDecimal)
            return new Value(((BigDecimal)lnum).add((BigDecimal)rnum));
        if (lnum instanceof Double)
            return new Value((Double)lnum + (Double)rnum);
        if (lnum instanceof Float)
            return new Value((Float)lnum + (Float)rnum);
        if (lnum instanceof BigInteger)
            return new Value(((BigInteger)lnum).add((BigInteger)rnum));
        if (lnum instanceof Long)
            return new Value((Long)lnum + (Long)rnum);
        if (lnum instanceof Integer)
            return new Value((Integer)lnum + (Integer)rnum);
        throw new EvalException("invalid add operation on values of type "
          + lnum.getClass().getName() + " and " + rnum.getClass().getName());
    }

    /**
     * Subtract this value.
     *
     * @param session current session
     */
    public Value subtract(Session session, Value that) {
        final Number lnum = this.promoteNumeric(session, "subtract", that);
        final Number rnum = that.promoteNumeric(session, "subtract", this);
        if (lnum instanceof BigDecimal)
            return new Value(((BigDecimal)lnum).subtract((BigDecimal)rnum));
        if (lnum instanceof Double)
            return new Value((Double)lnum - (Double)rnum);
        if (lnum instanceof Float)
            return new Value((Float)lnum - (Float)rnum);
        if (lnum instanceof BigInteger)
            return new Value(((BigInteger)lnum).subtract((BigInteger)rnum));
        if (lnum instanceof Long)
            return new Value((Long)lnum - (Long)rnum);
        if (lnum instanceof Integer)
            return new Value((Integer)lnum - (Integer)rnum);
        throw new EvalException("invalid subtract operation on values of type "
          + lnum.getClass().getName() + " and " + rnum.getClass().getName());
    }

    /**
     * Left shift this value.
     *
     * @param session current session
     */
    public Value lshift(Session session, Value arg) {
        final Number target = this.promoteNumeric(session, "left shift");
        final Number shift = arg.promoteNumeric(session, "left shift");
        if (!(target instanceof Integer) && !(target instanceof Long))
            throw new EvalException("invalid left shift value of type " + target.getClass().getName());
        if (!(shift instanceof Integer) && !(shift instanceof Long))
            throw new EvalException("invalid left shift amount of type " + shift.getClass().getName());
        final int amount = shift.intValue() & 0x1f;
        if (target instanceof Long)
            return new Value((Long)target << amount);
        if (target instanceof Integer)
            return new Value((Integer)target << amount);
        throw new EvalException("invalid left shift operation on values of type "
          + target.getClass().getName() + " and " + shift.getClass().getName());
    }

    /**
     * Right shift this value.
     *
     * @param session current session
     */
    public Value rshift(Session session, Value arg) {
        final Number target = this.promoteNumeric(session, "right shift");
        final Number shift = arg.promoteNumeric(session, "right shift");
        if (!(target instanceof Integer) && !(target instanceof Long))
            throw new EvalException("invalid right shift value of type " + target.getClass().getName());
        if (!(shift instanceof Integer) && !(shift instanceof Long))
            throw new EvalException("invalid right shift amount of type " + shift.getClass().getName());
        final int amount = shift.intValue() & 0x1f;
        if (target instanceof Long)
            return new Value((Long)target >> amount);
        if (target instanceof Integer)
            return new Value((Integer)target >> amount);
        throw new EvalException("invalid right shift operation on values of type "
          + target.getClass().getName() + " and " + shift.getClass().getName());
    }

    /**
     * Unsigned right shift this value.
     *
     * @param session current session
     */
    public Value urshift(Session session, Value arg) {
        final Number target = this.promoteNumeric(session, "right shift");
        final Number shift = arg.promoteNumeric(session, "right shift");
        if (!(target instanceof Integer) && !(target instanceof Long))
            throw new EvalException("invalid right shift value of type " + target.getClass().getName());
        if (!(shift instanceof Integer) && !(shift instanceof Long))
            throw new EvalException("invalid right shift amount of type " + shift.getClass().getName());
        final int amount = shift.intValue() & 0x1f;
        if (target instanceof Long)
            return new Value((Long)target >>> amount);
        if (target instanceof Integer)
            return new Value((Integer)target >>> amount);
        throw new EvalException("invalid right shift operation on values of type "
          + target.getClass().getName() + " and " + shift.getClass().getName());
    }

    /**
     * And this value.
     *
     * @param session current session
     */
    public Value and(Session session, Value that) {

        // Handle boolean
        final Object thisValue = this.get(session);
        final Object thatValue = that.get(session);
        if (thisValue instanceof Boolean && thatValue instanceof Boolean)
            return new Value((Boolean)thisValue & (Boolean)thatValue);

        // Handle numeric
        final Number lnum = this.promoteNumeric(session, "`and'", that);
        final Number rnum = that.promoteNumeric(session, "`and'", this);
        if (!(lnum instanceof Integer) && !(lnum instanceof Long) && !(lnum instanceof BigInteger))
            throw new EvalException("invalid `and' operation on value of type " + lnum.getClass().getName());
        if (lnum instanceof BigInteger)
            return new Value(((BigInteger)lnum).and((BigInteger)rnum));
        if (lnum instanceof Long)
            return new Value(((Long)lnum).longValue() & ((Long)rnum).longValue());
        return new Value(((Integer)lnum).intValue() & ((Integer)rnum).intValue());
    }

    /**
     * Or this value.
     *
     * @param session current session
     */
    public Value or(Session session, Value that) {

        // Handle boolean
        final Object thisValue = this.get(session);
        final Object thatValue = that.get(session);
        if (thisValue instanceof Boolean && thatValue instanceof Boolean)
            return new Value((Boolean)thisValue | (Boolean)thatValue);

        // Handle numeric
        final Number lnum = this.promoteNumeric(session, "`or'", that);
        final Number rnum = that.promoteNumeric(session, "`or'", this);
        if (!(lnum instanceof Integer) && !(lnum instanceof Long) && !(lnum instanceof BigInteger))
            throw new EvalException("invalid `or' operation on value of type " + lnum.getClass().getName());
        if (lnum instanceof BigInteger)
            return new Value(((BigInteger)lnum).or((BigInteger)rnum));
        if (lnum instanceof Long)
            return new Value(((Long)lnum).longValue() | ((Long)rnum).longValue());
        return new Value(((Integer)lnum).intValue() | ((Integer)rnum).intValue());
    }

    /**
     * Xor this value.
     *
     * @param session current session
     */
    public Value xor(Session session, Value that) {

        // Handle boolean
        final Object thisValue = this.get(session);
        final Object thatValue = that.get(session);
        if (thisValue instanceof Boolean && thatValue instanceof Boolean)
            return new Value((Boolean)thisValue ^ (Boolean)thatValue);

        // Handle numeric
        final Number lnum = this.promoteNumeric(session, "exclusive `or'", that);
        final Number rnum = that.promoteNumeric(session, "exclusive `or'", this);
        if (!(lnum instanceof Integer) && !(lnum instanceof Long) && !(lnum instanceof BigInteger))
            throw new EvalException("invalid exclusive `or' operation on value of type " + lnum.getClass().getName());
        if (lnum instanceof BigInteger)
            return new Value(((BigInteger)lnum).xor((BigInteger)rnum));
        if (lnum instanceof Long)
            return new Value(((Long)lnum).longValue() ^ ((Long)rnum).longValue());
        return new Value(((Integer)lnum).intValue() ^ ((Integer)rnum).intValue());
    }

    /**
     * Compare to another value, returning boolean.
     *
     * @param session current session
     * @param mask bit mask with bits {@link #LT}, {@link #GT}, and/or {@link #EQ}
     */
    public Value compare(Session session, Value that, int mask) {
        final Number lnum = this.promoteNumeric(session, "comparison", that);
        final Number rnum = that.promoteNumeric(session, "comparison", this);
        int result;
        if (lnum instanceof BigDecimal)
            result = ((BigDecimal)lnum).compareTo((BigDecimal)rnum);
        else if (lnum instanceof Double)
            result = Double.compare((Double)lnum, (Double)rnum);
        else if (lnum instanceof Float)
            result = Float.compare((Float)lnum, (Float)rnum);
        else if (lnum instanceof BigInteger)
            result = ((BigInteger)lnum).compareTo((BigInteger)rnum);
        else if (lnum instanceof Long)
            result = Long.compare((Long)lnum, (Long)rnum);
        else if (lnum instanceof Integer)
            result = Integer.compare((Integer)lnum, (Integer)rnum);
        else {
            throw new EvalException("invalid comparison operation between values of type "
              + lnum.getClass().getName() + " and " + rnum.getClass().getName());
        }
        result = result < 0 ? LT : result > 0 ? GT : EQ;
        return new Value((result & mask) != 0);
    }

    /**
     * Verify this instance has a {@link Setter}.
     *
     * @param operation description of operation for error messages
     */
    public void verifySetter(String operation) {
        if (this.setter == null)
            throw new EvalException("invalid " + operation + " operation on non-assignable value");
    }

    /**
     * Promote this value to the widest numeric type among itself and the provided values, with integer as a lower bound.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @param args one or more other values
     * @throws IllegalArgumentException if any argument is not numeric
     */
    Number promoteNumeric(final Session session, final String operation, Value... args) {

        // Promote any byte, char, or short up to int
        final Object value = this.get(session);
        Number num = value instanceof Character ? (int)(Character)value : this.checkNumeric(session, operation);
        if (num instanceof Byte || num instanceof Short)
            num = num.intValue();

        // Promote to widest type of any argument
        if (args.length != 0) {
            final ArrayList<Number> nums = new ArrayList<Number>(1 + args.length);
            nums.add(num);
            nums.addAll(Lists.transform(Arrays.asList(args), new Function<Value, Number>() {
                @Override
                public Number apply(Value value) {
                    return value.promoteNumeric(session, operation);
                }
            }));
            if (Iterables.find(nums, new InstancePredicate(BigDecimal.class), null) != null)
                num = Value.toBigDecimal(num);
            else if (Iterables.find(nums, new InstancePredicate(Double.class), null) != null)
                num = num.doubleValue();
            else if (Iterables.find(nums, new InstancePredicate(Float.class), null) != null)
                num = num.floatValue();
            else if (Iterables.find(nums, new InstancePredicate(BigInteger.class), null) != null)
                num = Value.toBigInteger(num);
            else if (Iterables.find(nums, new InstancePredicate(Long.class), null) != null)
                num = num.longValue();
            else
                num = num.intValue();
        }

        // Done
        return num;
    }

    /**
     * Convert a {@link Number} to {@link BigDecimal}.
     *
     * @param num number
     */
    static BigDecimal toBigDecimal(Number num) {
        if (num instanceof BigDecimal)
            return (BigDecimal)num;
        if (num instanceof BigInteger)
            return new BigDecimal((BigInteger)num);
        if (num instanceof Double || num instanceof Float)
            return new BigDecimal(num.doubleValue());
        if (num instanceof Long || num instanceof Integer || num instanceof Short || num instanceof Byte)
            return new BigDecimal(num.longValue());
        throw new EvalException("can't convert value of type " + num.getClass().getName() + " to BigDecimal");
    }

    /**
     * Convert a {@link Number} to {@link BigInteger}.
     *
     * @param num number
     */
    static BigInteger toBigInteger(Number num) {
        return Value.toBigDecimal(num).toBigInteger();
    }
}

