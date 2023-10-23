
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.tuple;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Arrays;

/**
 * Tuple utility methods.
 */
public final class Tuples {

    private Tuples() {
    }

    /**
     * Create a {@link Tuple} of the appropriate cardinality for the given number of objects.
     *
     * @param values tuple values
     * @return newly created tuple
     * @throws IllegalArgumentException if {@code values} is null or empty
     * @throws UnsupportedOperationException if {@code values} has more elements than exist concrete {@link Tuple} classes
     */
    public static Tuple createTuple(Iterable<?> values) {
        Preconditions.checkArgument(values != null, "null values");
        return Tuples.createTuple(Lists.newArrayList(values).toArray());
    }

    /**
     * Create a {@link Tuple} of the appropriate cardinality for the given number of objects.
     *
     * @param values tuple values
     * @return newly created tuple
     * @throws IllegalArgumentException if {@code values} is null or empty
     * @throws UnsupportedOperationException if {@code values} has more elements than exist concrete {@link Tuple} classes
     */
    public static Tuple createTuple(Object... values) {
        Preconditions.checkArgument(values != null, "null values");
        final Class<? extends Tuple> tupleClass = Tuples.tupleClassForSize(values.length);
        final Class<?>[] parameterTypes = new Class<?>[values.length];
        Arrays.fill(parameterTypes, Object.class);
        try {
            return tupleClass.getConstructor(parameterTypes).newInstance(values);
        } catch (Exception e) {
            throw new UnsupportedOperationException("can't instantiate " + tupleClass, e);
        }
    }

    /**
     * Get the {@link Tuple} subinterface having the specified cardinality.
     *
     * @param size number of objects in the tuple
     * @return {@link Tuple} subinterface with cardinality {@code size}
     * @throws IllegalArgumentException if {@code size} is less than one
     * @throws UnsupportedOperationException if {@code size} is more than exist concrete {@link Tuple} classes
     */
    public static Class<? extends Tuple> tupleClassForSize(int size) {
        Preconditions.checkArgument(size >= 1, "invalid non-positive size");
        final String name = Tuple.class.getName() + size;
        try {
            return Class.forName(name, false, Tuple.class.getClassLoader()).asSubclass(Tuple.class);
        } catch (Exception e) {
            throw new UnsupportedOperationException("can't find class \"" + name + "\" of size " + size, e);
        }
    }

    /**
     * Get the cardinality of the given {@link Tuple} subtype.
     *
     * @param tupleClass tuple class
     * @return cardinality of {@code tupleClass}
     * @throws IllegalArgumentException if {@code tupleClass} is not a recognized tuple class
     */
    public static int tupleSizeOf(Class<? extends Tuple> tupleClass) {
        for (int size = 1; true; size++) {
            try {
                if (Tuples.tupleClassForSize(size).isAssignableFrom(tupleClass))
                    return size;
            } catch (UnsupportedOperationException e) {
                throw new IllegalArgumentException("not a tuple (sub)class: " + tupleClass);
            }
        }
    }
}

