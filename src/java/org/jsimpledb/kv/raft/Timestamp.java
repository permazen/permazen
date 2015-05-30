
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import com.google.common.base.Preconditions;

/**
 * Represents a relative timestamp in milliseconds. Stored as an unsigned integer that cycles after 2<sup>32</sup>
 * milliseconds (about 48 days). Two values to be compared must have been generated within 24 days of each other
 * to be ordered correctly.
 *
 * <p>
 * This class uses {@link System#nanoTime}, not {@link System#currentTimeMillis}, and so is immune to changes in the system clock.
 *
 * <p>
 * Instances are immutable.
 */
public class Timestamp implements Comparable<Timestamp> {

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
    public Timestamp(int millis) {
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
     * @return relative millisecond offset
     */
    public int offsetFromNow() {
        return this.millis - Timestamp.now();
    }

    /**
     * Get the number of milliseconds this instance is offset from the given instance.
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
     * Get a {@link Timestamp} in the distant past. The actual returned value is about 12 days ago.
     *
     * @return timestamp value in the distant past
     */
    public static Timestamp distantPast() {
        return new Timestamp(Timestamp.now() - 0x40000000);
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

    @Override
    public int compareTo(Timestamp that) {
        Preconditions.checkArgument(this.millis != (that.millis ^ 0x80000000));
        final int diff = this.millis - that.millis;
        return diff < 0 ? -1 : diff > 0 ? 1 : 0;
    }
}

