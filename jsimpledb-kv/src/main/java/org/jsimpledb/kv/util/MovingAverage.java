
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.util;

import com.google.common.base.Preconditions;

/**
 * Exponential moving average calculator.
 */
class MovingAverage {

    private final double alpha;
    private double average = Double.NaN;

    /**
     * Constructor.
     *
     * @param alpha averaging parameter
     * @throws IllegalArgumentException if {@code alpha} is not a finite number in the range 0.0 to 1.0 (inclusive)
     */
    MovingAverage(double alpha) {
        Preconditions.checkArgument(Double.isFinite(alpha));
        Preconditions.checkArgument(alpha >= 0.0 && alpha <= 1.0);
        this.alpha = alpha;
    }

    /**
     * Constructor.
     *
     * @param alpha averaging parameter
     * @param initialValue initial value
     * @throws IllegalArgumentException if {@code alpha} is not a finite number in the range 0.0 to 1.0 (inclusive)
     * @throws IllegalArgumentException if {@code initialValue} is not a finite number
     */
    MovingAverage(double alpha, double initialValue) {
        this(alpha);
        this.add(initialValue);
    }

    /**
     * Get the current average.
     *
     * @return current average, or {@link Double#NaN} if no values have been added yet.
     */
    public double get() {
        return this.average;
    }

    /**
     * Add a value to the moving average.
     *
     * @param value value to add
     * @throws IllegalArgumentException if {@code value} is not a finite number
     */
    public void add(double value) {
        Preconditions.checkArgument(Double.isFinite(value));
        this.average = Double.isNaN(this.average) ? value : this.average + this.alpha * (value - this.average);
    }
}
