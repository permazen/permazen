
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import com.google.common.base.Preconditions;

import java.util.Comparator;

/**
 * Represents a relative timestamp in milliseconds.
 *
 * <p>
 * Values are stored as an unsigned 32 bit integer, which therefore recycles after 2<sup>32</sup> milliseconds (about 48 days).
 * Two values to be {@linkplain #compareTo compared} must have been generated within 24 days of each other to be ordered correctly.
 *
 * <p>
 * This class uses {@link System#nanoTime}, not {@link System#currentTimeMillis}, and so is immune to changes in the system clock.
 * To facilitate debugging, the zero mark is set at class initialization time.
 *
 * <p>
 * Instances are immutable.
 */
public class Timestamp implements Comparable<Timestamp> {

    /**
     * Sorts possibly null {@link Timestamp}s in chronological order, with null sorting first.
     *
     * <p>
     * All non-null {@link Timestamp}s must be contained within a single 2<sup>31</sup>-1 range; otherwise results are undefined.
     */
    public static final Comparator<Timestamp> NULL_FIRST_SORT = Comparator.nullsFirst(Comparator.naturalOrder());

    // Make timestamps start at zero to facilitate debugging
    private static final int TIME_BASE = Timestamp.milliTime();

    private final int millis;

    /**
     * Construtor returning the current time.
     */
    public Timestamp() {
        this(Timestamp.now());
    }

    /**
     * Constructor.
     *
     * @param millis relative milliseconds value from {@link #getMillis}
     */
    public Timestamp(final int millis) {
        this.millis = millis;
    }

    /**
     * Get the relative milliseconds value contained by this instance.
     *
     * @return relative millisecond value
     */
    public int getMillis() {
        return this.millis;
    }

    /**
     * Get the number of milliseconds this instance is offset from the current time.
     *
     * <p>
     * A positive offset means this instance is in the future.
     *
     * @return relative millisecond offset
     */
    public int offsetFromNow() {
        return this.millis - Timestamp.now();
    }

    /**
     * Get the number of milliseconds this instance is offset from the given instance.
     *
     * <p>
     * A positive offset means this instance is after {@code base}.
     *
     * @param base base timestamp
     * @return relative millisecond offset
     * @throws IllegalArgumentException if {@code base} is null
     */
    public int offsetFrom(Timestamp base) {
        Preconditions.checkArgument(base != null, "null base");
        return this.millis - base.millis;
    }

    /**
     * Return this timestamp offset by the given amount.
     *
     * @param offset offset in milliseconds
     * @return adjusted timestamp
     */
    public Timestamp offset(int offset) {
        return new Timestamp(this.millis + offset);
    }

    /**
     * Determine whether this timestamp is in the past or the future.
     *
     * @return true if this timestamp is in the past
     */
    public boolean hasOccurred() {
        return Timestamp.now() - this.millis >= 0;
    }

    /**
     * Determine whether this timestamp is so far in the past that it is in danger of rolling over to the future
     * as time continues to move forward.
     *
     * <p>
     * This returns true if this timestamp is within 5% of the roll-over point relative to the current time.
     * This would represent a time approximately 22.8 days in the past.
     *
     * @return true if this timestamp's offset is dangerously negative
     */
    public boolean isRolloverDanger() {
        return this.offsetFromNow() <= (int)(Integer.MIN_VALUE * 0.95f);
    }

// Internal methods

    private static int now() {
        return Timestamp.milliTime() - TIME_BASE;
    }

    private static int milliTime() {
        return (int)(System.nanoTime() / 1000000L);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final Timestamp that = (Timestamp)obj;
        return this.millis == that.millis;
    }

    @Override
    public int hashCode() {
        return this.millis;
    }

    @Override
    public String toString() {
        final long value = this.millis & 0xFFFFFFFFL;
        return String.format("%05d.%03d", value / 1000, value % 1000);
    }

// Comparable

    /**
     * Compare two instances, where "smaller" means earlier in time.
     *
     * <p>
     * Note: because timestamps recycle every 48 days, this method does <b>not</b> totally order instances.
     *
     * @param that timestamp to compare with
     * @throws IllegalArgumentException if this instance and {@code that} differ by exactly 2<sup>31</sup> milliseconds
     * @throws NullPointerException if {@code that} is null
     */
    @Override
    public int compareTo(Timestamp that) {
        Preconditions.checkArgument(this.millis != (that.millis ^ 0x80000000));
        final int diff = this.millis - that.millis;
        return diff < 0 ? -1 : diff > 0 ? 1 : 0;
    }
}

