
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
     * Determine whether the given new {@link Bounds} is within of the bounds of this instance.
     *
     * @param comparator comparator used to compare values, or null for natural ordering
     * @param newBounds new bounds
     * @return true if {@code newBounds} is in range, false otherwise
     * @throws IllegalArgumentException if {@code newBounds} is null
     */
    public boolean isWithinBounds(Comparator<? super T> comparator, Bounds<? extends T> newBounds) {
        Preconditions.checkArgument(newBounds != null, "null newBounds");

        // Check lower bound, if any
        if (newBounds.lowerBoundType != BoundType.NONE
          && !this.isWithinBound(comparator, newBounds.lowerBound, newBounds.lowerBoundType.isInclusive(), false))
            return false;

        // Check upper bound, if any
        if (newBounds.upperBoundType != BoundType.NONE
          && !this.isWithinBound(comparator, newBounds.upperBound, newBounds.upperBoundType.isInclusive(), true))
            return false;

        // OK
        return true;
    }

    /**
     * Check whether the given value is outside one of the bounds of this instance.
     *
     * @param comparator comparator used to compare values, or null for natural ordering
     * @param value value to check
     * @param requireInclusive whether value should not be considered included when it is exactly equal to an exclusive bound
     * @param upper true to check against upper bound, false to check against lower bound
     */
    @SuppressWarnings("unchecked")
    private boolean isWithinBound(Comparator<? super T> comparator, T value, boolean requireInclusive, boolean upper) {

        // Check bound type
        final BoundType boundType = upper ? this.upperBoundType : this.lowerBoundType;
        if (boundType == BoundType.NONE)
            return true;

        // Compare value to bound
        final T bound = upper ? this.upperBound : this.lowerBound;
        final int diff = comparator != null ? comparator.compare(value, bound) : ((Comparable<T>)value).compareTo(bound);

        // Handle value equal to bound
        if (diff == 0)
            return boundType == BoundType.INCLUSIVE || !requireInclusive;

        // Value is either inside or outside bound
        return upper ? diff < 0 : diff > 0;
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
        return this.lowerBoundType.hashCode()
          ^ this.upperBoundType.hashCode()
          ^ Objects.hashCode(this.lowerBound)
          ^ Objects.hashCode(this.upperBound);
    }
}
