
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
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
 * {@link Value} implementation superclass with implementations for all methods other than {@link #get get()}.
 */
public abstract class AbstractValue implements Value {

    protected AbstractValue() {
    }

    @Override
    public Class<?> getType(ParseSession session) {
        return Object.class;
    }

    @Override
    public Object checkNotNull(ParseSession session, String operation) {
        final Object value = this.get(session);
        if (value == null)
            throw new EvalException("invalid " + operation + " operation on null value");
        return value;
    }

    @Override
    public boolean checkBoolean(ParseSession session, String operation) {
        return this.checkType(session, operation, Boolean.class);
    }

    @Override
    public Number checkNumeric(ParseSession session, String operation) {
        return AbstractValue.checkNumeric(session, this.get(session), operation);
    }

    @Override
    public int checkIntegral(ParseSession session, String operation) {
        final Object value = this.checkNotNull(session, operation);
        if (value instanceof Character)
            return (int)(Character)value;
        if (!(value instanceof Byte) && !(value instanceof Short) && !(value instanceof Integer))
            throw new EvalException("invalid " + operation + " operation on " + AbstractValue.describeType(value, "non-integral"));
        return ((Number)value).intValue();
    }

    @Override
    public <T> T checkType(ParseSession session, String operation, Class<T> type) {
        Preconditions.checkArgument(type != null, "null type");
        final Object value = this.checkNotNull(session, operation);
        if (!type.isInstance(value)) {
            throw new EvalException("invalid " + operation + " operation on "
              + AbstractValue.describeType(value, "non-" + type.getSimpleName()));
        }
        return type.cast(value);
    }

    @Override
    public Value xxcrement(ParseSession session, String operation, boolean increment) {
        final int amount = increment ? 1 : -1;
        final LValue thisLValue = this.asLValue(operation);
        final Object value = this.get(session);
        Object num = value instanceof Character ? value : AbstractValue.checkNumeric(session, value, operation);
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
            throw new EvalException("invalid " + operation + " operation on " + AbstractValue.describeType(num));
        final Value result = new ConstValue(num);
        thisLValue.set(session, result);
        return result;
    }

    @Override
    public Value negate(ParseSession session) {
        final Number num = AbstractValue.promoteNumeric(session, this.get(session), "negate");
        if (num instanceof BigDecimal)
            return new ConstValue(((BigDecimal)num).negate());
        if (num instanceof Double)
            return new ConstValue(-(Double)num);
        if (num instanceof Float)
            return new ConstValue(-(Float)num);
        if (num instanceof BigInteger)
            return new ConstValue(((BigInteger)num).negate());
        if (num instanceof Long)
            return new ConstValue(-(Long)num);
        if (num instanceof Integer)
            return new ConstValue(-(Integer)num);
        throw new EvalException("invalid negate operation on " + AbstractValue.describeType(num));
    }

    @Override
    public Value invert(ParseSession session) {
        final Number num = AbstractValue.promoteNumeric(session, this.get(session), "invert");
        if (num instanceof Integer)
            return new ConstValue(~(Integer)num);
        if (num instanceof Long)
            return new ConstValue(~(Long)num);
        throw new EvalException("invalid invert operation on " + AbstractValue.describeType(num));
    }

    @Override
    public Value multiply(ParseSession session, Value that) {
        final Object thisValue = this.checkNotNull(session, "multiply");
        final Object thatValue = that.checkNotNull(session, "multiply");
        final Number lnum = AbstractValue.promoteNumeric(session, thisValue, "multiply", thatValue);
        final Number rnum = AbstractValue.promoteNumeric(session, thatValue, "multiply", thisValue);
        if (lnum instanceof BigDecimal)
            return new ConstValue(((BigDecimal)lnum).multiply((BigDecimal)rnum));
        if (lnum instanceof Double)
            return new ConstValue((Double)lnum * (Double)rnum);
        if (lnum instanceof Float)
            return new ConstValue((Float)lnum * (Float)rnum);
        if (lnum instanceof BigInteger)
            return new ConstValue(((BigInteger)lnum).multiply((BigInteger)rnum));
        if (lnum instanceof Long)
            return new ConstValue((Long)lnum * (Long)rnum);
        if (lnum instanceof Integer)
            return new ConstValue((Integer)lnum * (Integer)rnum);
        throw new EvalException("invalid multiply operation on values of type "
          + lnum.getClass().getName() + " and " + rnum.getClass().getName());
    }

    @Override
    public Value divide(ParseSession session, Value that) {
        final Object thisValue = this.checkNotNull(session, "divide");
        final Object thatValue = that.checkNotNull(session, "divide");
        final Number lnum = AbstractValue.promoteNumeric(session, thisValue, "divide", thatValue);
        final Number rnum = AbstractValue.promoteNumeric(session, thatValue, "divide", thisValue);
        if (lnum instanceof BigDecimal)
            return new ConstValue(((BigDecimal)lnum).divide((BigDecimal)rnum));
        if (lnum instanceof Double)
            return new ConstValue((Double)lnum / (Double)rnum);
        if (lnum instanceof Float)
            return new ConstValue((Float)lnum / (Float)rnum);
        if (lnum instanceof BigInteger)
            return new ConstValue(((BigInteger)lnum).divide((BigInteger)rnum));
        if (lnum instanceof Long)
            return new ConstValue((Long)lnum / (Long)rnum);
        if (lnum instanceof Integer)
            return new ConstValue((Integer)lnum / (Integer)rnum);
        throw new EvalException("invalid divide operation on values of type "
          + lnum.getClass().getName() + " and " + rnum.getClass().getName());
    }

    @Override
    public Value mod(ParseSession session, Value that) {
        final Object thisValue = this.checkNotNull(session, "modulo");
        final Object thatValue = that.checkNotNull(session, "modulo");
        final Number lnum = AbstractValue.promoteNumeric(session, thisValue, "modulo", thatValue);
        final Number rnum = AbstractValue.promoteNumeric(session, thatValue, "modulo", thisValue);
        if (lnum instanceof Double)
            return new ConstValue((Double)lnum % (Double)rnum);
        if (lnum instanceof Float)
            return new ConstValue((Float)lnum % (Float)rnum);
        if (lnum instanceof BigInteger)
            return new ConstValue(((BigInteger)lnum).mod((BigInteger)rnum));
        if (lnum instanceof Long)
            return new ConstValue((Long)lnum % (Long)rnum);
        if (lnum instanceof Integer)
            return new ConstValue((Integer)lnum % (Integer)rnum);
        throw new EvalException("invalid modulo operation on values of type "
          + lnum.getClass().getName() + " and " + rnum.getClass().getName());
    }

    @Override
    public Value add(ParseSession session, Value that) {

        // Handle String concatenation
        final Object thisValue = this.get(session);
        final Object thatValue = that.get(session);
        if (thisValue == null || thatValue == null || thisValue instanceof String || thatValue instanceof String)
            return new ConstValue(String.valueOf(thisValue) + String.valueOf(thatValue));

        // Handle numeric
        final Number lnum = AbstractValue.promoteNumeric(session, thisValue, "add", thatValue);
        final Number rnum = AbstractValue.promoteNumeric(session, thatValue, "add", thisValue);
        if (lnum instanceof BigDecimal)
            return new ConstValue(((BigDecimal)lnum).add((BigDecimal)rnum));
        if (lnum instanceof Double)
            return new ConstValue((Double)lnum + (Double)rnum);
        if (lnum instanceof Float)
            return new ConstValue((Float)lnum + (Float)rnum);
        if (lnum instanceof BigInteger)
            return new ConstValue(((BigInteger)lnum).add((BigInteger)rnum));
        if (lnum instanceof Long)
            return new ConstValue((Long)lnum + (Long)rnum);
        if (lnum instanceof Integer)
            return new ConstValue((Integer)lnum + (Integer)rnum);
        throw new EvalException("invalid add operation on values of type "
          + lnum.getClass().getName() + " and " + rnum.getClass().getName());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Value subtract(ParseSession session, Value that) {

        // Handle NavigableSet with equal comparators XXX might not have compatible elements
        final Object thisValue = this.checkNotNull(session, "subtract");
        final Object thatValue = that.checkNotNull(session, "subtract");
        if (thisValue instanceof NavigableSet
          && thatValue instanceof NavigableSet
          && (((NavigableSet<?>)thisValue).comparator() != null ?
           ((NavigableSet<?>)thisValue).comparator().equals(((NavigableSet<?>)thatValue).comparator()) :
           ((NavigableSet<?>)thatValue).comparator() == null))
            return new ConstValue(NavigableSets.difference((NavigableSet<Object>)thisValue, (NavigableSet<Object>)thatValue));

        // Handle Set
        if (thisValue instanceof Set && thatValue instanceof Set)
            return new ConstValue(Sets.difference((Set<Object>)thisValue, (Set<Object>)thatValue));

        // Handle numeric
        final Number lnum = AbstractValue.promoteNumeric(session, thisValue, "subtract", thatValue);
        final Number rnum = AbstractValue.promoteNumeric(session, thatValue, "subtract", thisValue);
        if (lnum instanceof BigDecimal)
            return new ConstValue(((BigDecimal)lnum).subtract((BigDecimal)rnum));
        if (lnum instanceof Double)
            return new ConstValue((Double)lnum - (Double)rnum);
        if (lnum instanceof Float)
            return new ConstValue((Float)lnum - (Float)rnum);
        if (lnum instanceof BigInteger)
            return new ConstValue(((BigInteger)lnum).subtract((BigInteger)rnum));
        if (lnum instanceof Long)
            return new ConstValue((Long)lnum - (Long)rnum);
        if (lnum instanceof Integer)
            return new ConstValue((Integer)lnum - (Integer)rnum);
        throw new EvalException("invalid subtract operation on values of type "
          + lnum.getClass().getName() + " and " + rnum.getClass().getName());
    }

    @Override
    public Value lshift(ParseSession session, Value arg) {
        final Object thisValue = this.checkNotNull(session, "left shift");
        final Object thatValue = arg.checkNotNull(session, "left shift");
        final Number target = AbstractValue.promoteNumeric(session, thisValue, "left shift");
        final Number shift = AbstractValue.promoteNumeric(session, thatValue, "left shift");
        if (!(target instanceof Integer) && !(target instanceof Long))
            throw new EvalException("invalid left shift target " + AbstractValue.describeType(target));
        if (!(shift instanceof Integer) && !(shift instanceof Long))
            throw new EvalException("invalid left shift " + AbstractValue.describeType(shift));
        if (target instanceof Long)
            return new ConstValue((Long)target << (shift.intValue() & 0x3f));
        if (target instanceof Integer)
            return new ConstValue((Integer)target << (shift.intValue() & 0x1f));
        throw new EvalException("invalid left shift operation on "
          + AbstractValue.describeType(target) + " and " + AbstractValue.describeType(shift));
    }

    @Override
    public Value rshift(ParseSession session, Value arg) {
        final Object thisValue = this.checkNotNull(session, "right shift");
        final Object thatValue = arg.checkNotNull(session, "right shift");
        final Number target = AbstractValue.promoteNumeric(session, thisValue, "right shift");
        final Number shift = AbstractValue.promoteNumeric(session, thatValue, "right shift");
        if (!(target instanceof Integer) && !(target instanceof Long))
            throw new EvalException("invalid right shift target " + AbstractValue.describeType(target));
        if (!(shift instanceof Integer) && !(shift instanceof Long))
            throw new EvalException("invalid right shift " + AbstractValue.describeType(shift));
        if (target instanceof Long)
            return new ConstValue((Long)target >> (shift.intValue() & 0x3f));
        if (target instanceof Integer)
            return new ConstValue((Integer)target >> (shift.intValue() & 0x1f));
        throw new EvalException("invalid right shift operation on "
          + AbstractValue.describeType(target) + " and " + AbstractValue.describeType(shift));
    }

    @Override
    public Value urshift(ParseSession session, Value arg) {
        final Object thisValue = this.checkNotNull(session, "unsigned right shift");
        final Object thatValue = arg.checkNotNull(session, "unsigned right shift");
        final Number target = AbstractValue.promoteNumeric(session, thisValue, "unsigned right shift");
        final Number shift = AbstractValue.promoteNumeric(session, thatValue, "unsigned right shift");
        if (!(target instanceof Integer) && !(target instanceof Long))
            throw new EvalException("invalid unsigned right shift target " + AbstractValue.describeType(target));
        if (!(shift instanceof Integer) && !(shift instanceof Long))
            throw new EvalException("invalid unsigned right shift " + AbstractValue.describeType(shift));
        if (target instanceof Long)
            return new ConstValue((Long)target >>> (shift.intValue() & 0x3f));
        if (target instanceof Integer)
            return new ConstValue((Integer)target >>> (shift.intValue() & 0x1f));
        throw new EvalException("invalid unsigned right shift operation on "
          + AbstractValue.describeType(target) + " and " + AbstractValue.describeType(shift));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Value and(ParseSession session, Value that) {

        // Handle boolean
        final Object thisValue = this.checkNotNull(session, "`and'");
        final Object thatValue = that.checkNotNull(session, "`and'");
        if (thisValue instanceof Boolean && thatValue instanceof Boolean)
            return new ConstValue((Boolean)thisValue & (Boolean)thatValue);

        // Handle NavigableSet with equal comparators XXX might not have compatible elements
        if (thisValue instanceof NavigableSet
          && thatValue instanceof NavigableSet
          && (((NavigableSet<?>)thisValue).comparator() != null ?
           ((NavigableSet<?>)thisValue).comparator().equals(((NavigableSet<?>)thatValue).comparator()) :
           ((NavigableSet<?>)thatValue).comparator() == null))
            return new ConstValue(NavigableSets.intersection((NavigableSet<Object>)thisValue, (NavigableSet<Object>)thatValue));

        // Handle Set
        if (thisValue instanceof Set && thatValue instanceof Set)
            return new ConstValue(Sets.intersection((Set<Object>)thisValue, (Set<Object>)thatValue));

        // Handle numeric
        final Number lnum = AbstractValue.promoteNumeric(session, thisValue, "`and'", thatValue);
        final Number rnum = AbstractValue.promoteNumeric(session, thatValue, "`and'", thisValue);
        if (!(lnum instanceof Integer) && !(lnum instanceof Long) && !(lnum instanceof BigInteger))
            throw new EvalException("invalid `and' operation on " + AbstractValue.describeType(lnum));
        if (lnum instanceof BigInteger)
            return new ConstValue(((BigInteger)lnum).and((BigInteger)rnum));
        if (lnum instanceof Long)
            return new ConstValue(((Long)lnum).longValue() & ((Long)rnum).longValue());
        return new ConstValue(((Integer)lnum).intValue() & ((Integer)rnum).intValue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Value or(ParseSession session, Value that) {

        // Handle boolean
        final Object thisValue = this.checkNotNull(session, "`or'");
        final Object thatValue = that.checkNotNull(session, "`or'");
        if (thisValue instanceof Boolean && thatValue instanceof Boolean)
            return new ConstValue((Boolean)thisValue | (Boolean)thatValue);

        // Handle NavigableSet with equal comparators XXX might not have compatible elements
        if (thisValue instanceof NavigableSet
          && thatValue instanceof NavigableSet
          && (((NavigableSet<?>)thisValue).comparator() != null ?
           ((NavigableSet<?>)thisValue).comparator().equals(((NavigableSet<?>)thatValue).comparator()) :
           ((NavigableSet<?>)thatValue).comparator() == null))
            return new ConstValue(NavigableSets.union((NavigableSet<Object>)thisValue, (NavigableSet<Object>)thatValue));

        // Handle Set
        if (thisValue instanceof Set && thatValue instanceof Set)
            return new ConstValue(Sets.union((Set<Object>)thisValue, (Set<Object>)thatValue));

        // Handle numeric
        final Number lnum = AbstractValue.promoteNumeric(session, thisValue, "`or'", thatValue);
        final Number rnum = AbstractValue.promoteNumeric(session, thatValue, "`or'", thisValue);
        if (!(lnum instanceof Integer) && !(lnum instanceof Long) && !(lnum instanceof BigInteger))
            throw new EvalException("invalid `or' operation on " + AbstractValue.describeType(lnum));
        if (lnum instanceof BigInteger)
            return new ConstValue(((BigInteger)lnum).or((BigInteger)rnum));
        if (lnum instanceof Long)
            return new ConstValue(((Long)lnum).longValue() | ((Long)rnum).longValue());
        return new ConstValue(((Integer)lnum).intValue() | ((Integer)rnum).intValue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Value xor(ParseSession session, Value that) {

        // Handle boolean
        final Object thisValue = this.checkNotNull(session, "exclusive `or'");
        final Object thatValue = that.checkNotNull(session, "exclusive `or'");
        if (thisValue instanceof Boolean && thatValue instanceof Boolean)
            return new ConstValue((Boolean)thisValue ^ (Boolean)thatValue);

        // Handle NavigableSet with equal comparators XXX might not have compatible elements
        if (thisValue instanceof NavigableSet
          && thatValue instanceof NavigableSet
          && (((NavigableSet<?>)thisValue).comparator() != null ?
           ((NavigableSet<?>)thisValue).comparator().equals(((NavigableSet<?>)thatValue).comparator()) :
           ((NavigableSet<?>)thatValue).comparator() == null)) {
            return new ConstValue(NavigableSets.symmetricDifference((NavigableSet<Object>)thisValue,
              (NavigableSet<Object>)thatValue));
        }

        // Handle Set
        if (thisValue instanceof Set && thatValue instanceof Set)
            return new ConstValue(Sets.symmetricDifference((Set<Object>)thisValue, (Set<Object>)thatValue));

        // Handle numeric
        final Number lnum = AbstractValue.promoteNumeric(session, thisValue, "exclusive `or'", thatValue);
        final Number rnum = AbstractValue.promoteNumeric(session, thatValue, "exclusive `or'", thisValue);
        if (!(lnum instanceof Integer) && !(lnum instanceof Long) && !(lnum instanceof BigInteger))
            throw new EvalException("invalid exclusive `or' operation on " + AbstractValue.describeType(lnum));
        if (lnum instanceof BigInteger)
            return new ConstValue(((BigInteger)lnum).xor((BigInteger)rnum));
        if (lnum instanceof Long)
            return new ConstValue(((Long)lnum).longValue() ^ ((Long)rnum).longValue());
        return new ConstValue(((Integer)lnum).intValue() ^ ((Integer)rnum).intValue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Value compare(ParseSession session, Value that, int mask) {

        // Try numeric comparison
        final Object thisValue = this.checkNotNull(session, "comparison");
        final Object thatValue = that.checkNotNull(session, "comparison");
        while (thisValue instanceof Number && thatValue instanceof Number) {
            final Number lnum = AbstractValue.promoteNumeric(session, thisValue, "comparison", thatValue);
            final Number rnum = AbstractValue.promoteNumeric(session, thatValue, "comparison", thisValue);
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
            else
                break;
            result = result < 0 ? LT : result > 0 ? GT : EQ;
            return new ConstValue((result & mask) != 0);
        }

        // Try natural ordering using Comparable
        while (thisValue instanceof Comparable && thatValue instanceof Comparable) {
            int result;
            try {
                result = ((Comparable<Object>)thisValue).compareTo(thatValue);
            } catch (ClassCastException e) {
                break;
            }
            result = result < 0 ? LT : result > 0 ? GT : EQ;
            return new ConstValue((result & mask) != 0);
        }

        // We don't know how to order these
        throw new EvalException("invalid comparison operation between "
          + AbstractValue.describeType(thisValue) + " and " + AbstractValue.describeType(thatValue));
    }

    @Override
    public LValue asLValue(String operation) {
        try {
            return (LValue)this;
        } catch (ClassCastException e) {
            throw new EvalException("invalid " + operation + " operation on non-assignable value");
        }
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
        Number num = obj instanceof Character ? (int)(Character)obj : AbstractValue.checkNumeric(session, obj, operation);
        if (num instanceof Byte || num instanceof Short)
            num = num.intValue();

        // Promote to widest type of any argument
        if (args.length > 0) {
            final ArrayList<Number> nums = new ArrayList<Number>(1 + args.length);
            nums.add(num);
            nums.addAll(Lists.transform(Arrays.asList(args), new Function<Object, Number>() {
                @Override
                public Number apply(Object value) {
                    return AbstractValue.promoteNumeric(session, value, operation);
                }
            }));
            if (Iterables.find(nums, new InstancePredicate(BigDecimal.class), null) != null)
                num = AbstractValue.toBigDecimal(num);
            else if (Iterables.find(nums, new InstancePredicate(Double.class), null) != null)
                num = num.doubleValue();
            else if (Iterables.find(nums, new InstancePredicate(Float.class), null) != null)
                num = num.floatValue();
            else if (Iterables.find(nums, new InstancePredicate(BigInteger.class), null) != null)
                num = AbstractValue.toBigInteger(num);
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
            throw new EvalException("invalid " + operation + " operation on " + AbstractValue.describeType(obj, "non-numeric"));
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
        throw new EvalException("can't convert " + AbstractValue.describeType(num) + " to BigDecimal");
    }

    /**
     * Convert a {@link Number} to {@link BigInteger}.
     *
     * @param num number
     */
    static BigInteger toBigInteger(Number num) {
        return AbstractValue.toBigDecimal(num).toBigInteger();
    }

    static String describeType(Object obj, String... prefix) {
        if (obj == null)
            return "null value";
        return (prefix.length > 0 ? prefix[0] + " " : "") + "value of type " + obj.getClass().getName();
    }
}

