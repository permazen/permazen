
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.Set;

import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.util.InstancePredicate;
import org.jsimpledb.util.NavigableSets;

/**
 * An evaluation-time value used in an expression.
 */
public class Value {

    /**
     * Special value that can be used to indicate "no value" in certain situations, such as return value
     * from a method returning void. Actually evaluates to null.
     */
    public static final Value NO_VALUE = new Value(null);

    static final int LT = 0x01;
    static final int GT = 0x02;
    static final int EQ = 0x04;

    private final Object obj;
    private final Setter setter;

    /**
     * Constructor for a constant, non L-Value instance.
     */
    public Value(Object obj) {
        this(obj, null);
    }

    /**
     * Primary constructor.
     *
     * @param obj value to be returned by {@link #get get()} (unless overridden by subclass)
     * @param setter callback for changing this instance's value, or null if this instance is not an L-Value
     */
    public Value(Object obj, Setter setter) {
        this.obj = obj;
        this.setter = this instanceof DynamicValue ? (DynamicValue)this : setter;
    }

    /**
     * Get the actual value.
     */
    public Object get(ParseSession session) {
        return this.obj;
    }

    /**
     * Get the {@link Setter} associated with this value, if any.
     *
     * @return associated {@link Setter}, or null if this is not an L-Value
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
    public Object checkNotNull(ParseSession session, String operation) {
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
    public boolean checkBoolean(ParseSession session, String operation) {
        return this.checkType(session, operation, Boolean.class);
    }

    /**
     * Verify this instance has a numeric type (i.e., {@link Number}).
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @return numeric value
     */
    public Number checkNumeric(ParseSession session, String operation) {
        return Value.checkNumeric(session, this.get(session), operation);
    }

    /**
     * Verify this instance has a numeric type that is integral, i.e., byte, char, short, or int.
     * Return its integer value.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @return integer value
     */
    public int checkIntegral(ParseSession session, String operation) {
        final Object value = this.checkNotNull(session, operation);
        if (value instanceof Character)
            return (int)(Character)value;
        if (!(value instanceof Byte) && !(value instanceof Short) && !(value instanceof Integer))
            throw new EvalException("invalid " + operation + " operation on " + Value.describeType(value, "non-integral"));
        return ((Number)value).intValue();
    }

    /**
     * Verify this instance has the specified type.
     * Return its value cast to that type.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @return typed value
     */
    public <T> T checkType(ParseSession session, String operation, Class<T> type) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        final Object value = this.checkNotNull(session, operation);
        if (!type.isInstance(value)) {
            throw new EvalException("invalid " + operation + " operation on "
              + Value.describeType(value, "non-" + type.getSimpleName()));
        }
        return type.cast(value);
    }

    /**
     * Increment/decrement this value. Instance must have a {@link Setter}.
     *
     * @param session current session
     * @param operation description of operation for error messages
     * @param increment true to increment, false to decrement
     * @return adjusted value
     */
    public Value xxcrement(ParseSession session, String operation, boolean increment) {
        this.verifySetter(operation);
        final int amount = increment ? 1 : -1;
        final Object value = this.get(session);
        Object num = value instanceof Character ? value : Value.checkNumeric(session, value, operation);
        if (num instanceof Byte)
            num = (byte)((Byte)num + (byte)amount);
        else if (num instanceof Character)
            num = (char)((Character)num + (char)amount);
        else if (num instanceof Short)
            num = (short)((Short)num + (short)amount);
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
            throw new EvalException("invalid " + operation + " operation on " + Value.describeType(num));
        final Value result = new Value(num);
        this.setter.set(session, result);
        return result;
    }

    /**
     * Negate this value.
     *
     * @param session current session
     */
    public Value negate(ParseSession session) {
        final Number num = Value.promoteNumeric(session, this.get(session), "negate");
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
        throw new EvalException("invalid negate operation on " + Value.describeType(num));
    }

    /**
     * Bitwise invert this value.
     *
     * @param session current session
     */
    public Value invert(ParseSession session) {
        final Number num = Value.promoteNumeric(session, this.get(session), "invert");
        if (num instanceof Integer)
            return new Value(~(Integer)num);
        if (num instanceof Long)
            return new Value(~(Long)num);
        throw new EvalException("invalid invert operation on " + Value.describeType(num));
    }

    /**
     * Multiply this value.
     *
     * @param session current session
     */
    public Value multiply(ParseSession session, Value that) {
        final Object thisValue = this.get(session);
        final Object thatValue = that.get(session);
        final Number lnum = Value.promoteNumeric(session, thisValue, "multiply", thatValue);
        final Number rnum = Value.promoteNumeric(session, thatValue, "multiply", thisValue);
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
    public Value divide(ParseSession session, Value that) {
        final Object thisValue = this.get(session);
        final Object thatValue = that.get(session);
        final Number lnum = Value.promoteNumeric(session, thisValue, "divide", thatValue);
        final Number rnum = Value.promoteNumeric(session, thatValue, "divide", thisValue);
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
    public Value mod(ParseSession session, Value that) {
        final Object thisValue = this.get(session);
        final Object thatValue = that.get(session);
        final Number lnum = Value.promoteNumeric(session, thisValue, "modulo", thatValue);
        final Number rnum = Value.promoteNumeric(session, thatValue, "modulo", thisValue);
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
    public Value add(ParseSession session, Value that) {

        // Handle String concatenation
        final Object thisValue = this.get(session);
        final Object thatValue = that.get(session);
        if (thisValue == null || thatValue == null || thisValue instanceof String || thatValue instanceof String)
            return new Value(String.valueOf(thisValue) + String.valueOf(thatValue));

        // Handle numeric
        final Number lnum = Value.promoteNumeric(session, thisValue, "add", thatValue);
        final Number rnum = Value.promoteNumeric(session, thatValue, "add", thisValue);
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
    @SuppressWarnings("unchecked")
    public Value subtract(ParseSession session, Value that) {

        // Handle NavigableSet with equal comparators XXX might not have compatible elements
        final Object thisValue = this.get(session);
        final Object thatValue = that.get(session);
        if (thisValue instanceof NavigableSet
          && thatValue instanceof NavigableSet
          && (((NavigableSet<?>)thisValue).comparator() != null ?
           ((NavigableSet<?>)thisValue).comparator().equals(((NavigableSet<?>)thatValue).comparator()) :
           ((NavigableSet<?>)thatValue).comparator() == null))
            return new Value(NavigableSets.difference((NavigableSet<Object>)thisValue, (NavigableSet<Object>)thatValue));

        // Handle Set
        if (thisValue instanceof Set && thatValue instanceof Set)
            return new Value(Sets.difference((Set<Object>)thisValue, (Set<Object>)thatValue));

        // Handle numeric
        final Number lnum = Value.promoteNumeric(session, thisValue, "subtract", thatValue);
        final Number rnum = Value.promoteNumeric(session, thatValue, "subtract", thisValue);
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
    public Value lshift(ParseSession session, Value arg) {
        final Object thisValue = this.get(session);
        final Object thatValue = arg.get(session);
        final Number target = Value.promoteNumeric(session, thisValue, "left shift");
        final Number shift = Value.promoteNumeric(session, thatValue, "left shift");
        if (!(target instanceof Integer) && !(target instanceof Long))
            throw new EvalException("invalid left shift target " + Value.describeType(target));
        if (!(shift instanceof Integer) && !(shift instanceof Long))
            throw new EvalException("invalid left shift " + Value.describeType(shift));
        if (target instanceof Long)
            return new Value((Long)target << (shift.intValue() & 0x1f));
        if (target instanceof Integer)
            return new Value((Integer)target << (shift.intValue() & 0x3f));
        throw new EvalException("invalid left shift operation on "
          + Value.describeType(target) + " and " + Value.describeType(shift));
    }

    /**
     * Right shift this value.
     *
     * @param session current session
     */
    public Value rshift(ParseSession session, Value arg) {
        final Object thisValue = this.get(session);
        final Object thatValue = arg.get(session);
        final Number target = Value.promoteNumeric(session, thisValue, "right shift");
        final Number shift = Value.promoteNumeric(session, thatValue, "right shift");
        if (!(target instanceof Integer) && !(target instanceof Long))
            throw new EvalException("invalid right shift target " + Value.describeType(target));
        if (!(shift instanceof Integer) && !(shift instanceof Long))
            throw new EvalException("invalid right shift " + Value.describeType(shift));
        if (target instanceof Long)
            return new Value((Long)target >> (shift.intValue() & 0x1f));
        if (target instanceof Integer)
            return new Value((Integer)target >> (shift.intValue() & 0x3f));
        throw new EvalException("invalid right shift operation on "
          + Value.describeType(target) + " and " + Value.describeType(shift));
    }

    /**
     * Unsigned right shift this value.
     *
     * @param session current session
     */
    public Value urshift(ParseSession session, Value arg) {
        final Object thisValue = this.get(session);
        final Object thatValue = arg.get(session);
        final Number target = Value.promoteNumeric(session, thisValue, "unsigned right shift");
        final Number shift = Value.promoteNumeric(session, thatValue, "unsigned right shift");
        if (!(target instanceof Integer) && !(target instanceof Long))
            throw new EvalException("invalid unsigned right shift target " + Value.describeType(target));
        if (!(shift instanceof Integer) && !(shift instanceof Long))
            throw new EvalException("invalid unsigned right shift " + Value.describeType(shift));
        if (target instanceof Long)
            return new Value((Long)target >>> (shift.intValue() & 0x1f));
        if (target instanceof Integer)
            return new Value((Integer)target >>> (shift.intValue() & 0x3f));
        throw new EvalException("invalid unsigned right shift operation on "
          + Value.describeType(target) + " and " + Value.describeType(shift));
    }

    /**
     * And this value.
     *
     * @param session current session
     */
    @SuppressWarnings("unchecked")
    public Value and(ParseSession session, Value that) {

        // Handle boolean
        final Object thisValue = this.get(session);
        final Object thatValue = that.get(session);
        if (thisValue instanceof Boolean && thatValue instanceof Boolean)
            return new Value((Boolean)thisValue & (Boolean)thatValue);

        // Handle NavigableSet with equal comparators XXX might not have compatible elements
        if (thisValue instanceof NavigableSet
          && thatValue instanceof NavigableSet
          && (((NavigableSet<?>)thisValue).comparator() != null ?
           ((NavigableSet<?>)thisValue).comparator().equals(((NavigableSet<?>)thatValue).comparator()) :
           ((NavigableSet<?>)thatValue).comparator() == null))
            return new Value(NavigableSets.intersection((NavigableSet<Object>)thisValue, (NavigableSet<Object>)thatValue));

        // Handle Set
        if (thisValue instanceof Set && thatValue instanceof Set)
            return new Value(Sets.intersection((Set<Object>)thisValue, (Set<Object>)thatValue));

        // Handle numeric
        final Number lnum = Value.promoteNumeric(session, thisValue, "`and'", thatValue);
        final Number rnum = Value.promoteNumeric(session, thatValue, "`and'", thisValue);
        if (!(lnum instanceof Integer) && !(lnum instanceof Long) && !(lnum instanceof BigInteger))
            throw new EvalException("invalid `and' operation on " + Value.describeType(lnum));
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
    @SuppressWarnings("unchecked")
    public Value or(ParseSession session, Value that) {

        // Handle boolean
        final Object thisValue = this.get(session);
        final Object thatValue = that.get(session);
        if (thisValue instanceof Boolean && thatValue instanceof Boolean)
            return new Value((Boolean)thisValue | (Boolean)thatValue);

        // Handle NavigableSet with equal comparators XXX might not have compatible elements
        if (thisValue instanceof NavigableSet
          && thatValue instanceof NavigableSet
          && (((NavigableSet<?>)thisValue).comparator() != null ?
           ((NavigableSet<?>)thisValue).comparator().equals(((NavigableSet<?>)thatValue).comparator()) :
           ((NavigableSet<?>)thatValue).comparator() == null))
            return new Value(NavigableSets.union((NavigableSet<Object>)thisValue, (NavigableSet<Object>)thatValue));

        // Handle Set
        if (thisValue instanceof Set && thatValue instanceof Set)
            return new Value(Sets.union((Set<Object>)thisValue, (Set<Object>)thatValue));

        // Handle numeric
        final Number lnum = Value.promoteNumeric(session, thisValue, "`or'", thatValue);
        final Number rnum = Value.promoteNumeric(session, thatValue, "`or'", thisValue);
        if (!(lnum instanceof Integer) && !(lnum instanceof Long) && !(lnum instanceof BigInteger))
            throw new EvalException("invalid `or' operation on " + Value.describeType(lnum));
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
    @SuppressWarnings("unchecked")
    public Value xor(ParseSession session, Value that) {

        // Handle boolean
        final Object thisValue = this.get(session);
        final Object thatValue = that.get(session);
        if (thisValue instanceof Boolean && thatValue instanceof Boolean)
            return new Value((Boolean)thisValue ^ (Boolean)thatValue);

        // Handle NavigableSet with equal comparators XXX might not have compatible elements
        if (thisValue instanceof NavigableSet
          && thatValue instanceof NavigableSet
          && (((NavigableSet<?>)thisValue).comparator() != null ?
           ((NavigableSet<?>)thisValue).comparator().equals(((NavigableSet<?>)thatValue).comparator()) :
           ((NavigableSet<?>)thatValue).comparator() == null))
            return new Value(NavigableSets.symmetricDifference((NavigableSet<Object>)thisValue, (NavigableSet<Object>)thatValue));

        // Handle Set
        if (thisValue instanceof Set && thatValue instanceof Set)
            return new Value(Sets.symmetricDifference((Set<Object>)thisValue, (Set<Object>)thatValue));

        // Handle numeric
        final Number lnum = Value.promoteNumeric(session, thisValue, "exclusive `or'", thatValue);
        final Number rnum = Value.promoteNumeric(session, thatValue, "exclusive `or'", thisValue);
        if (!(lnum instanceof Integer) && !(lnum instanceof Long) && !(lnum instanceof BigInteger))
            throw new EvalException("invalid exclusive `or' operation on " + Value.describeType(lnum));
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
    public Value compare(ParseSession session, Value that, int mask) {
        final Object thisValue = this.get(session);
        final Object thatValue = that.get(session);
        final Number lnum = Value.promoteNumeric(session, thisValue, "comparison", thatValue);
        final Number rnum = Value.promoteNumeric(session, thatValue, "comparison", thisValue);
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
            throw new EvalException("invalid comparison operation between "
              + Value.describeType(lnum) + " and " + Value.describeType(rnum));
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
     * Promote the given object to the widest numeric type among itself and the provided objects, with integer as a lower bound.
     *
     * @param session current session
     * @param obj Java value
     * @param operation description of operation for error messages
     * @param args zero or more other Java values
     * @throws IllegalArgumentException if any argument is not numeric
     */
    static Number promoteNumeric(final ParseSession session, final Object obj, final String operation, Object... args) {

        // Promote any byte, char, or short up to int
        Number num = obj instanceof Character ? (int)(Character)obj : Value.checkNumeric(session, obj, operation);
        if (num instanceof Byte || num instanceof Short)
            num = num.intValue();

        // Promote to widest type of any argument
        if (args.length > 0) {
            final ArrayList<Number> nums = new ArrayList<Number>(1 + args.length);
            nums.add(num);
            nums.addAll(Lists.transform(Arrays.asList(args), new Function<Object, Number>() {
                @Override
                public Number apply(Object value) {
                    return Value.promoteNumeric(session, value, operation);
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
     * Verify object has a numeric type (i.e., {@link Number}).
     *
     * @param session current session
     * @param object Java value
     * @param operation description of operation for error messages
     * @return numeric value
     */
    static Number checkNumeric(ParseSession session, Object obj, String operation) {
        if (!(obj instanceof Number))
            throw new EvalException("invalid " + operation + " operation on " + Value.describeType(obj, "non-numeric"));
        return (Number)obj;
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
        throw new EvalException("can't convert " + Value.describeType(num) + " to BigDecimal");
    }

    /**
     * Convert a {@link Number} to {@link BigInteger}.
     *
     * @param num number
     */
    static BigInteger toBigInteger(Number num) {
        return Value.toBigDecimal(num).toBigInteger();
    }

    static String describeType(Object obj, String... prefix) {
        if (obj == null)
            return "null value";
        return (prefix.length > 0 ? prefix[0] + " " : "") + "value of type " + obj.getClass().getName();
    }

    /**
     * Function that invokes {@link #get Value.get()} on its argument.
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

