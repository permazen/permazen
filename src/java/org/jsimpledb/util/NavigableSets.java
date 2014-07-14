
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.NavigableSet;

/**
 * Utility methods relating to {@link NavigableSet}.
 */
public final class NavigableSets {

    private NavigableSets() {
    }

    /**
     * Create a read-only view of the intersection of two or more {@link NavigableSet}s that have a consistent sort order.
     * The {@linkplain NavigableSet#comparator Comparator}s of the sets must all be {@linkplain Comparator#equals equal}
     * (or else all null, for natural ordering).
     *
     * <p>
     * The returned intersection interates efficiently: a complete iteration takes time <i>O(N * M)</i> where
     * <i>N</i> is the size of the smallest set, and <i>M</i> is the number of sets.
     * </p>
     *
     * @param sets the sets to intersect
     * @throws IllegalArgumentException if the {@code sets} do not have equal {@link Comparator}s
     */
    public static <E> NavigableSet<E> intersection(Iterable<? extends NavigableSet<E>> sets) {
        return new IntersectionNavigableSet<E>(sets);
    }

    /**
     * Create a read-only view of the intersection of two or more {@link NavigableSet}s that have a consistent sort order.
     * The {@linkplain NavigableSet#comparator Comparator}s of the sets must all be {@linkplain Comparator#equals equal}
     * (or else all null, for natural ordering).
     *
     * <p>
     * The returned intersection interates efficiently: a complete iteration takes time <i>O(N * M)</i> where
     * <i>N</i> is the size of the smallest set, and <i>M</i> is the number of sets.
     * </p>
     *
     * @param sets the sets to intersect
     * @throws IllegalArgumentException if the {@code sets} do not have equal {@link Comparator}s
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <E> NavigableSet<E> intersection(NavigableSet<E>... sets) {
        return new IntersectionNavigableSet<E>(Arrays.asList(sets));
    }

    /**
     * Create a read-only view of the union of two or more {@link NavigableSet}s that have a consistent sort order.
     * The {@linkplain NavigableSet#comparator Comparator}s of the sets must all be {@linkplain Comparator#equals equal}
     * (or else all null, for natural ordering).
     *
     * @param sets the sets to union
     * @throws IllegalArgumentException if the {@code sets} do not have equal {@link Comparator}s
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <E> NavigableSet<E> union(NavigableSet<E>... sets) {
        return new UnionNavigableSet<E>(Arrays.asList(sets));
    }

    /**
     * Create a read-only view of the union of two or more {@link NavigableSet}s that have a consistent sort order.
     * The {@linkplain NavigableSet#comparator Comparator}s of the sets must all be {@linkplain Comparator#equals equal}
     * (or else all null, for natural ordering).
     *
     * @param sets the sets to union
     * @throws IllegalArgumentException if the {@code sets} do not have equal {@link Comparator}s
     */
    public static <E> NavigableSet<E> union(Iterable<? extends NavigableSet<E>> sets) {
        return new UnionNavigableSet<E>(sets);
    }

    /**
     * Create a read-only view of the difference of two {@link NavigableSet}s that have a consistent sort order.
     * That is, a set containing all elements contained in the first set but not the second.
     * The {@linkplain NavigableSet#comparator Comparator}s of the sets must all be {@linkplain Comparator#equals equal}
     * (or else all null, for natural ordering).
     *
     * @param set1 original set
     * @param set2 set of elements to exclude from {@code set1}
     * @throws IllegalArgumentException if the {@code set1} and {@code set2} do not have equal {@link Comparator}s
     */
    public static <E> NavigableSet<E> difference(NavigableSet<E> set1, NavigableSet<E> set2) {
        return new DifferenceNavigableSet<E>(set1, set2);
    }

    /**
     * Create a read-only view of the symmetric difference of two {@link NavigableSet}s that have a consistent sort order.
     * That is, the set containing all elements contained in the first set or the second set, but not in both sets.
     * The {@linkplain NavigableSet#comparator Comparator}s of the sets must all be {@linkplain Comparator#equals equal}
     * (or else all null, for natural ordering).
     *
     * @param set1 first set
     * @param set2 second set
     * @throws IllegalArgumentException if the {@code set1} and {@code set2} do not have equal {@link Comparator}s
     */
    public static <E> NavigableSet<E> symmetricDifference(NavigableSet<E> set1, NavigableSet<E> set2) {
        return NavigableSets.difference(NavigableSets.union(set1, set2), NavigableSets.intersection(set1, set2));
    }

    /**
     * Create a {@link NavigableSet} containing a single element and natural ordering.
     *
     * @param value singleton value
     * @throws IllegalArgumentException if {@code value} is null or
     *  {@code value}'s class has no natural ordering (i.e., does not implement {@link Comparable})
     */
    public static <E> NavigableSet<E> singleton(E value) {
        return NavigableSets.<E>singleton(null, value);
    }

    /**
     * Create a {@link NavigableSet} containing a single element.
     *
     * @param comparator comparator, or null for natural ordering
     * @param value singleton value (possibly null)
     * @throws IllegalArgumentException if {@code comparator} is null and either {@code value} is null or
     *  {@code value}'s class has no natural ordering (i.e., does not implement {@link Comparable})
     */
    public static <E> NavigableSet<E> singleton(Comparator<? super E> comparator, E value) {
        return new SingletonNavigableSet<E>(comparator, value);
    }

    /**
     * Create an empty {@link NavigableSet}.
     * The returned set's {@link NavigableSet#comparator comparator()} method will return null.
     */
    public static <E> NavigableSet<E> empty() {
        return NavigableSets.<E>empty(null);
    }

    /**
     * Create an empty {@link NavigableSet} with a specified {@link Comparator}.
     *
     * @param comparator comparator, or null for natural ordering
     */
    public static <E> NavigableSet<E> empty(Comparator<? super E> comparator) {
        return new EmptyNavigableSet<E>(comparator);
    }
}

