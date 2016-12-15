
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

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
     * The returned intersection iterates efficiently: a complete iteration requires <i>O(N * M)</i> queries, where
     * <i>N</i> is the size of the smallest set, and <i>M</i> is the number of sets.
     *
     * @param sets the sets to intersect
     * @param <E> element type
     * @return the intersection of all {@code sets}
     * @throws IllegalArgumentException if the {@code sets} do not have equal {@link Comparator}s
     * @throws IllegalArgumentException if {@code sets} is null
     */
    public static <E> NavigableSet<E> intersection(Iterable<? extends NavigableSet<E>> sets) {
        Preconditions.checkArgument(sets != null, "null sets");
        if (Iterables.find(sets, Predicates.instanceOf(EmptyNavigableSet.class), null) != null)
            return new EmptyNavigableSet<>(null);
        return new IntersectionNavigableSet<>(sets);
    }

    /**
     * Create a read-only view of the intersection of two or more {@link NavigableSet}s that have a consistent sort order.
     * The {@linkplain NavigableSet#comparator Comparator}s of the sets must all be {@linkplain Comparator#equals equal}
     * (or else all null, for natural ordering).
     *
     * <p>
     * The returned intersection iterates efficiently: a complete iteration takes time <i>O(N * M)</i> where
     * <i>N</i> is the size of the smallest set, and <i>M</i> is the number of sets.
     *
     * @param sets the sets to intersect
     * @param <E> element type
     * @return the intersection of all {@code sets}
     * @throws IllegalArgumentException if the {@code sets} do not have equal {@link Comparator}s
     * @throws IllegalArgumentException if {@code sets} is null
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <E> NavigableSet<E> intersection(NavigableSet<E>... sets) {
        Preconditions.checkArgument(sets != null, "null sets");
        return NavigableSets.<E>intersection(Arrays.asList(sets));
    }

    /**
     * Create a read-only view of the union of two or more {@link NavigableSet}s that have a consistent sort order.
     * The {@linkplain NavigableSet#comparator Comparator}s of the sets must all be {@linkplain Comparator#equals equal}
     * (or else all null, for natural ordering).
     *
     * @param sets the sets to union
     * @param <E> element type
     * @return the union of all {@code sets}
     * @throws IllegalArgumentException if the {@code sets} do not have equal {@link Comparator}s
     * @throws IllegalArgumentException if {@code sets} is null
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <E> NavigableSet<E> union(NavigableSet<E>... sets) {
        Preconditions.checkArgument(sets != null, "null sets");
        return NavigableSets.<E>union(Arrays.asList(sets));
    }

    /**
     * Create a read-only view of the union of two or more {@link NavigableSet}s that have a consistent sort order.
     * The {@linkplain NavigableSet#comparator Comparator}s of the sets must all be {@linkplain Comparator#equals equal}
     * (or else all null, for natural ordering).
     *
     * @param sets the sets to union
     * @param <E> element type
     * @return the union of all {@code sets}
     * @throws IllegalArgumentException if the {@code sets} do not have equal {@link Comparator}s
     * @throws IllegalArgumentException if {@code sets} is null
     */
    public static <E> NavigableSet<E> union(Iterable<? extends NavigableSet<E>> sets) {
        Preconditions.checkArgument(sets != null, "null sets");
        if (!(sets = Iterables.filter(sets, Predicates.not(Predicates.instanceOf(EmptyNavigableSet.class)))).iterator().hasNext())
            return new EmptyNavigableSet<>(null);
        return new UnionNavigableSet<>(sets);
    }

    /**
     * Create a read-only view of the difference of two {@link NavigableSet}s that have a consistent sort order.
     * That is, a set containing all elements contained in the first set but not the second.
     * The {@linkplain NavigableSet#comparator Comparator}s of the sets must all be {@linkplain Comparator#equals equal}
     * (or else all null, for natural ordering).
     *
     * @param set1 original set
     * @param set2 set of elements to exclude from {@code set1}
     * @param <E> element type
     * @return the difference of {@code set1} and {@code set2}
     * @throws IllegalArgumentException if the {@code set1} and {@code set2} do not have equal {@link Comparator}s
     * @throws IllegalArgumentException if either parameter is is null
     */
    public static <E> NavigableSet<E> difference(NavigableSet<E> set1, NavigableSet<E> set2) {
        Preconditions.checkArgument(set1 != null, "null set1");
        Preconditions.checkArgument(set2 != null, "null set2");
        if (set1 instanceof EmptyNavigableSet || set2 instanceof EmptyNavigableSet)
            return set1;
        return new DifferenceNavigableSet<>(set1, set2);
    }

    /**
     * Create a read-only view of the symmetric difference of two {@link NavigableSet}s that have a consistent sort order.
     * That is, the set containing all elements contained in the first set or the second set, but not in both sets.
     * The {@linkplain NavigableSet#comparator Comparator}s of the sets must all be {@linkplain Comparator#equals equal}
     * (or else all null, for natural ordering).
     *
     * @param set1 first set
     * @param set2 second set
     * @param <E> element type
     * @return the symmetric difference of {@code set1} and {@code set2}
     * @throws IllegalArgumentException if the {@code set1} and {@code set2} do not have equal {@link Comparator}s
     * @throws IllegalArgumentException if either parameter is is null
     */
    public static <E> NavigableSet<E> symmetricDifference(NavigableSet<E> set1, NavigableSet<E> set2) {
        Preconditions.checkArgument(set1 != null, "null set1");
        Preconditions.checkArgument(set2 != null, "null set2");
        if (set1 instanceof EmptyNavigableSet)
            return set2;
        if (set2 instanceof EmptyNavigableSet)
            return set1;
        return NavigableSets.difference(NavigableSets.union(set1, set2), NavigableSets.intersection(set1, set2));
    }

    /**
     * Create a {@link NavigableSet} containing a single element and natural ordering.
     *
     * @param value singleton value
     * @param <E> element type
     * @return singleton set
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
     * @param <E> element type
     * @return singleton set
     * @throws IllegalArgumentException if {@code comparator} is null and either {@code value} is null or
     *  {@code value}'s class has no natural ordering (i.e., does not implement {@link Comparable})
     */
    public static <E> NavigableSet<E> singleton(Comparator<? super E> comparator, E value) {
        return new SingletonNavigableSet<>(comparator, value);
    }

    /**
     * Create an empty {@link NavigableSet}.
     * The returned set's {@link NavigableSet#comparator comparator()} method will return null.
     *
     * @param <E> element type
     * @return empty set
     */
    public static <E> NavigableSet<E> empty() {
        return NavigableSets.<E>empty(null);
    }

    /**
     * Create an empty {@link NavigableSet} with a specified {@link Comparator}.
     *
     * @param comparator comparator, or null for natural ordering
     * @param <E> element type
     * @return empty set
     */
    public static <E> NavigableSet<E> empty(Comparator<? super E> comparator) {
        return new EmptyNavigableSet<>(comparator);
    }

    /**
     * Get a non-null {@link Comparator} that sorts consistently with, and optionally reversed from, the given {@link Comparator}.
     *
     * @param comparator comparator, or null for natural ordering
     * @param reversed whether to return a reversed {@link Comparator}
     * @param <T> compared type
     * @return a non-null {@link Comparator}
     */
    @SuppressWarnings("unchecked")
    static <T> Comparator<T> getComparator(Comparator<T> comparator, boolean reversed) {
        if (comparator == null)
            comparator = (Comparator<T>)Comparator.naturalOrder();
        return reversed ? comparator.reversed() : comparator;
    }
}

