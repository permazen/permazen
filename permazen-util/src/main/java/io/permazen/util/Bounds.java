
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Preconditions;

import java.util.Comparator;
import java.util.Objects;

/**
 * Utility class used by {@link AbstractNavigableSet} and {@link AbstractNavigableMap} to define
 * the (optional) upper and lower bounds of a restricted range.
 *
 * <p>
 * Instances are immutable.
 *
 * @param <T> Java type of range bounds
 */
public class Bounds<T> {

    private final T lowerBound;
    private final T upperBound;
    private final BoundType lowerBoundType;
    private final BoundType upperBoundType;

    /**
     * Convenience constructor to create a new instance with no upper or lower bounds.
     */
    public Bounds() {
        this(null, BoundType.NONE, null, BoundType.NONE);
    }

    /**
     * Create a one-sided bound.
     *
     * @param bound bound restriction value; ignored if {@code boundType} is {@link BoundType#NONE}
     * @param boundType type of bound for {@code bound}, or {@link BoundType#NONE} if there is no lower bound
     * @param upper true to create an upper bound, false to create a lower bound
     * @throws IllegalArgumentException if {@code boundType} is null
     */
    public Bounds(T bound, BoundType boundType, boolean upper) {
        this(upper ? null : bound, upper ? BoundType.NONE : boundType, upper ? bound : null, upper ? boundType : BoundType.NONE);
    }

    /**
     * Create two-sided bounds with inclusive lower bound and exclusive upper bound.
     *
     * @param lowerBound lower bound restriction value (inclusive)
     * @param upperBound upper bound restriction value (exclusive)
     */
    public Bounds(T lowerBound, T upperBound) {
        this(lowerBound, BoundType.INCLUSIVE, upperBound, BoundType.EXCLUSIVE);
    }

    /**
     * Create two-sided bounds.
     *
     * @param lowerBound lower bound restriction value (inclusive)
     * @param lowerInclusive true if {@code lowerBound} is inclusive, false if {@code lowerBound} is exclusive
     * @param upperBound upper bound restriction value (exclusive)
     * @param upperInclusive true if {@code upperBound} is inclusive, false if {@code upperBound} is exclusive
     */
    public Bounds(T lowerBound, boolean lowerInclusive, T upperBound, boolean upperInclusive) {
        this(lowerBound, lowerInclusive ? BoundType.INCLUSIVE : BoundType.EXCLUSIVE,
             upperBound, upperInclusive ? BoundType.INCLUSIVE : BoundType.EXCLUSIVE);
    }

    /**
     * Primary constructor.
     *
     * @param lowerBound lower bound restriction value; ignored if {@code lowerBoundType} is {@link BoundType#NONE}
     * @param lowerBoundType type of bound for {@code lowerBound}, or {@link BoundType#NONE} if there is no lower bound
     * @param upperBound upper bound restriction value; ignored if {@code upperBoundType} is {@link BoundType#NONE}
     * @param upperBoundType type of bound for {@code upperBound}, or {@link BoundType#NONE} if there is no upper bound
     * @throws IllegalArgumentException if {@code lowerBoundType} or {@code upperBoundType} is null
     */
    public Bounds(T lowerBound, BoundType lowerBoundType, T upperBound, BoundType upperBoundType) {
        Preconditions.checkArgument(lowerBoundType != null, "null lowerBoundType");
        Preconditions.checkArgument(upperBoundType != null, "null upperBoundType");
        this.lowerBound = lowerBoundType != BoundType.NONE ? lowerBound : null;
        this.upperBound = upperBoundType != BoundType.NONE ? upperBound : null;
        this.lowerBoundType = lowerBoundType;
        this.upperBoundType = upperBoundType;
    }

    /**
     * Determine whether this instance has a lower bound.
     *
     * @return false if this instance's {@linkplain #getLowerBoundType lower bound type} is {@link BoundType#NONE}, otherwise true
     */
    public boolean hasLowerBound() {
        return this.lowerBoundType != BoundType.NONE;
    }

    /**
     * Determine whether this instance has an upper bound.
     *
     * @return false if this instance's {@linkplain #getUpperBoundType upper bound type} is {@link BoundType#NONE}, otherwise true
     */
    public boolean hasUpperBound() {
        return this.upperBoundType != BoundType.NONE;
    }

    /**
     * Get the Java value corresponding to the lower bound restriction, if any.
     * Returns null when {@link #getLowerBoundType} returns {@link BoundType#NONE}.
     *
     * @return lower bound Java value, or null if there is no lower bound
     */
    public T getLowerBound() {
        return this.lowerBound;
    }

    /**
     * Get the Java value corresponding to the upper bound restriction, if any.
     * Returns null when {@link #getUpperBoundType} returns {@link BoundType#NONE}.
     *
     * @return upper bound Java value, or null if there is no upper bound
     */
    public T getUpperBound() {
        return this.upperBound;
    }

    /**
     * Get the type of the lower bound that corresponds to {@link #getLowerBound}.
     *
     * @return lower bound restriction type, never null
     */
    public BoundType getLowerBoundType() {
        return this.lowerBoundType;
    }

    /**
     * Get the type of the upper bound that corresponds to {@link #getUpperBound}.
     *
     * @return upper bound restriction type, never null
     */
    public BoundType getUpperBoundType() {
        return this.upperBoundType;
    }

    /**
     * Create an instance like this instance but with the upper and lower bounds reversed.
     * Obviously, the result is only sensible when a reversed {@link Comparator} is also used.
     *
     * @return reversal of this instance
     */
    public Bounds<T> reverse() {
        return new Bounds<>(this.upperBound, this.upperBoundType, this.lowerBound, this.lowerBoundType);
    }

    /**
     * Create an instance like this instance but with a different lower bound.
     *
     * @param newLowerBound new lower bound restriction value; ignored if {@code newLowerBoundType} is {@link BoundType#NONE}
     * @param newLowerBoundType type of bound for {@code newLowerBound}, or {@link BoundType#NONE} if there is no lower bound
     * @return instance with new lower bound
     * @throws IllegalArgumentException if {@code newLowerBoundType} is null
     */
    public Bounds<T> withLowerBound(T newLowerBound, BoundType newLowerBoundType) {
        return new Bounds<>(newLowerBound, newLowerBoundType, this.upperBound, this.upperBoundType);
    }

    /**
     * Create an instance like this instance but with a different upper bound.
     *
     * @param newUpperBound new upper bound restriction value; ignored if {@code newUpperBoundType} is {@link BoundType#NONE}
     * @param newUpperBoundType type of bound for {@code newUpperBound}, or {@link BoundType#NONE} if there is no upper bound
     * @return instance with new upper bound
     * @throws IllegalArgumentException if {@code newUpperBoundType} is null
     */
    public Bounds<T> withUpperBound(T newUpperBound, BoundType newUpperBoundType) {
        return new Bounds<>(this.lowerBound, this.lowerBoundType, newUpperBound, newUpperBoundType);
    }

    /**
     * Create an instance like this instance but with the lower bound removed.
     *
     * @return new instance
     */
    public Bounds<T> withoutLowerBound() {
        return new Bounds<>(null, BoundType.NONE, this.upperBound, this.upperBoundType);
    }

    /**
     * Create an instance like this instance but with the upper bound removed.
     *
     * @return new instance
     */
    public Bounds<T> withoutUpperBound() {
        return new Bounds<>(this.lowerBound, this.lowerBoundType, null, BoundType.NONE);
    }

    /**
     * Check whether the given value is within the lower bound of this instance.
     *
     * @param comparator comparator used to compare values, or null for natural ordering
     * @param value value to check
     * @return true if {@code value} is within bounds, false otherwise
     */
    public boolean isWithinLowerBound(Comparator<? super T> comparator, T value) {
        return this.isWithinBound(comparator, value, true, false);
    }

    /**
     * Check whether the given value is within the upper bound of this instance.
     *
     * @param comparator comparator used to compare values, or null for natural ordering
     * @param value value to check
     * @return true if {@code value} is within bounds, false otherwise
     */
    public boolean isWithinUpperBound(Comparator<? super T> comparator, T value) {
        return this.isWithinBound(comparator, value, true, true);
    }

    /**
     * Check whether the given value is within the bounds of this instance.
     *
     * <p>
     * Equivalent to:
     * <blockquote><pre>
     * isWithinLowerBound(comparator, value) &amp;&amp; isWithinUpperBound(comparator, value)
     * </pre></blockquote>
     *
     * @param comparator comparator used to compare values, or null for natural ordering
     * @param value value to check
     * @return true if {@code value} is within bounds, false otherwise
     */
    public boolean isWithinBounds(Comparator<? super T> comparator, T value) {
        return this.isWithinLowerBound(comparator, value) && this.isWithinUpperBound(comparator, value);
    }

    /**
     * Determine whether the given {@link Bounds} are within of the bounds of this instance.
     *
     * @param comparator comparator used to compare values, or null for natural ordering
     * @param other other bounds
     * @return true if {@code other} is in range, false otherwise
     * @throws IllegalArgumentException if {@code other} is null
     */
    public boolean isWithinBounds(Comparator<? super T> comparator, Bounds<? extends T> other) {
        Preconditions.checkArgument(other != null, "null other");

        // Check lower bound, if any
        if (other.lowerBoundType != BoundType.NONE
          && !this.isWithinBound(comparator, other.lowerBound, other.lowerBoundType.isInclusive(), false))
            return false;

        // Check upper bound, if any
        if (other.upperBoundType != BoundType.NONE
          && !this.isWithinBound(comparator, other.upperBound, other.upperBoundType.isInclusive(), true))
            return false;

        // OK
        return true;
    }

    /**
     * Consolidate the ranges of values implied by this instance and the given instance, if possible.
     *
     * <p>
     * If the value ranges implied by the two bounds are not overlapping or adjacent, then null is returned.
     * Otherwise an instance is returned that corresponds to the union of the two implied value ranges.
     *
     * <p>
     * Some cases not handled, e.g., when one bound has an inclusive upper bound with value <i>x</i> and the
     * other has an inclusive lower bound with value <i>x + 1</i>, because this method can't detect adjacent values.
     *
     * @param comparator comparator used to compare values, or null for natural ordering
     * @param other instance to attempt consolidation with this one
     * @return consolidated bounds, or null if none is possible
     * @throws IllegalArgumentException if {@code other} is null
     */
    public Bounds<T> union(Comparator<? super T> comparator, Bounds<T> other) {

        // Sanity check
        Preconditions.checkArgument(other != null, "null other");

        // Allow an instance to consolidate with itself or any empty instance
        comparator = this.comparatorOrNatural(comparator);
        if (this.equals(other) || other.isEmpty(comparator))
            return this;
        if (this.isEmpty(comparator))
            return other;

        // Compare min's to max's
        int diff;
        Bounds<T> a = this;
        Bounds<T> b = other;
        final boolean leAMinBMax = !b.hasUpperBound()
          || !a.hasLowerBound()
          || (diff = comparator.compare(a.lowerBound, b.upperBound)) < 0
          || (diff == 0 && !(a.lowerBoundType == BoundType.EXCLUSIVE && b.upperBoundType == BoundType.EXCLUSIVE));
        final boolean leBMinAMax = !a.hasUpperBound()
          || !b.hasLowerBound()
          || (diff = comparator.compare(b.lowerBound, a.upperBound)) < 0
          || (diff == 0 && !(b.lowerBoundType == BoundType.EXCLUSIVE && a.upperBoundType == BoundType.EXCLUSIVE));

        // Two ranges abut/overlap if and only if min_a <= max_b and min_b <= max_a
        if (!(leAMinBMax && leBMinAMax))
            return null;

        // Determine the lower of the two lower bounds, and the higher of the two upper bounds
        final boolean leAMinBMin = !a.hasLowerBound()
          || (b.hasLowerBound()
            && ((diff = comparator.compare(a.lowerBound, b.lowerBound)) < 0
              || (diff == 0 && !(a.lowerBoundType == BoundType.EXCLUSIVE && b.lowerBoundType == BoundType.INCLUSIVE))));
        final boolean geAMaxBMax = !a.hasUpperBound()
          || (b.hasUpperBound()
            && ((diff = comparator.compare(a.upperBound, b.upperBound)) > 0
              || (diff == 0 && !(a.upperBoundType == BoundType.EXCLUSIVE && b.upperBoundType == BoundType.INCLUSIVE))));

        // Build new bounds
        final Bounds<T> lower = leAMinBMin ? a : b;
        final Bounds<T> upper = geAMaxBMax ? a : b;
        return new Bounds<>(lower.lowerBound, lower.lowerBoundType, upper.upperBound, upper.upperBoundType);
    }

    /**
     * Determine if this instance is provably empty, i.e., no values can possibly be within the bounds.
     *
     * @param comparator comparator used to compare values, or null for natural ordering
     * @return true if this instance is backwards
     */
    public boolean isEmpty(Comparator<? super T> comparator) {
        if (!this.hasLowerBound() || !this.hasUpperBound())
            return false;
        final int diff = this.comparatorOrNatural(comparator).compare(this.lowerBound, this.upperBound);
        if (diff > 0)
            return true;
        if (diff < 0)
            return false;
        return !(this.lowerBoundType == BoundType.INCLUSIVE && this.upperBoundType == BoundType.INCLUSIVE);
    }

    /**
     * Determine if this instance has bounds that are "inverted" with respect to the given {@link Comparator}.
     *
     * <p>
     * This instance is "inverted" if it has both lower and upper bounds and
     * the lower bound's value is strictly greater than the upper bound's value.
     * The bounds' types (i.e., whether {@link BoundType#INCLUSIVE} or {@link BoundType#EXCLUSIVE}) is not considered.
     *
     * <p>
     * An inverted instance is always {@linkplain #isEmpty empty}, but the reverse is not necessarily true.
     *
     * @param comparator comparator used to compare values, or null for natural ordering
     * @return true if this instance is backwards
     */
    public boolean isInverted(Comparator<? super T> comparator) {
        if (!this.hasLowerBound() || !this.hasUpperBound())
            return false;
        return this.comparatorOrNatural(comparator).compare(this.lowerBound, this.upperBound) > 0;
    }

    /**
     * Check whether the given value is outside one of the bounds of this instance.
     *
     * @param comparator comparator used to compare values, or null for natural ordering
     * @param value value to check
     * @param requireInclusive whether value should not be considered included when it is exactly equal to an exclusive bound
     * @param upper true to check against upper bound, false to check against lower bound
     */
    private boolean isWithinBound(Comparator<? super T> comparator, T value, boolean requireInclusive, boolean upper) {

        // Check bound type
        final BoundType boundType = upper ? this.upperBoundType : this.lowerBoundType;
        if (boundType == BoundType.NONE)
            return true;

        // Compare value to bound
        final T bound = upper ? this.upperBound : this.lowerBound;
        final int diff = this.comparatorOrNatural(comparator).compare(value, bound);

        // Handle value equal to bound
        if (diff == 0)
            return boundType == BoundType.INCLUSIVE || !requireInclusive;

        // Value is either inside or outside bound
        return upper ? diff < 0 : diff > 0;
    }

    @SuppressWarnings("unchecked")
    private Comparator<? super T> comparatorOrNatural(Comparator<? super T> comparator) {
        return comparator != null ? comparator : (x, y) -> ((Comparable<T>)x).compareTo(y);
    }

// Static Methods

    /**
     * Create an instance with bounds that allow only the given value.
     *
     * @param value unique bounded value
     * @param <T> Java type of range bounds
     * @return bounds containing exactly {@code value}
     */
    public static <T> Bounds<T> eq(T value) {
        return new Bounds<>(value, BoundType.INCLUSIVE, value, BoundType.INCLUSIVE);
    }

    /**
     * Create an instance with an inclusive lower bound.
     *
     * @param lowerBound inclusive lower bound
     * @param <T> Java type of range bounds
     * @return bounds containing all values greater than or equal to {@code value}
     */
    public static <T> Bounds<T> ge(T lowerBound) {
        return new Bounds<>(lowerBound, BoundType.INCLUSIVE, false);
    }

    /**
     * Create an instance with an exclusive lower bound.
     *
     * @param lowerBound exclusive lower bound
     * @param <T> Java type of range bounds
     * @return bounds containing all values greater than {@code value}
     */
    public static <T> Bounds<T> gt(T lowerBound) {
        return new Bounds<>(lowerBound, BoundType.EXCLUSIVE, false);
    }

    /**
     * Create an instance with an inclusive upper bound.
     *
     * @param upperBound inclusive upper bound
     * @param <T> Java type of range bounds
     * @return bounds containing all values less than or equal to {@code value}
     */
    public static <T> Bounds<T> le(T upperBound) {
        return new Bounds<>(upperBound, BoundType.INCLUSIVE, true);
    }

    /**
     * Create an instance with an exclusive upper bound.
     *
     * @param upperBound exclusive upper bound
     * @param <T> Java type of range bounds
     * @return bounds containing all values less than {@code value}
     */
    public static <T> Bounds<T> lt(T upperBound) {
        return new Bounds<>(upperBound, BoundType.EXCLUSIVE, true);
    }

// Object

    @Override
    public String toString() {
        return "Bounds[lower(" + this.lowerBoundType + ")=" + this.lowerBound
          + ",upper(" + this.upperBoundType + ")=" + this.upperBound + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final Bounds<?> that = (Bounds<?>)obj;
        return this.lowerBoundType == that.lowerBoundType
          && this.upperBoundType == that.upperBoundType
          && Objects.equals(this.lowerBound, that.lowerBound)
          && Objects.equals(this.upperBound, that.upperBound);
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode()
          ^ this.lowerBoundType.hashCode()
          ^ this.upperBoundType.hashCode()
          ^ Objects.hashCode(this.lowerBound)
          ^ Objects.hashCode(this.upperBound);
    }
}
