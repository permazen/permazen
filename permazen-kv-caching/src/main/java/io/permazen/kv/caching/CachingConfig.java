
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.caching;

import com.google.common.base.Preconditions;

/**
 * API for configuring caching behavior.
 */
public interface CachingConfig {

    /**
     * Default maximum number of contiguous ranges of key/value pairs to allow before we start purging the
     * least recently used ones ({@value #DEFAULT_MAX_RANGES}).
     *
     * @see #getMaxRanges
     */
    int DEFAULT_MAX_RANGES = 256;

    /**
     * Default maximum number of bytes to cache in a single contiguous range of key/value pairs
     * ({@value #DEFAULT_MAX_RANGE_BYTES}).
     *
     * @see #getMaxRangeBytes
     */
    long DEFAULT_MAX_RANGE_BYTES = 10 * 1024 * 1024;

    /**
     * Default maximum total number of bytes to cache including all ranges ({@value #DEFAULT_MAX_TOTAL_BYTES}).
     *
     * @see #getMaxTotalBytes
     */
    long DEFAULT_MAX_TOTAL_BYTES = 100 * 1024 * 1024;

    /**
     * Default wait factor ({@value #DEFAULT_WAIT_FACTOR}).
     *
     * @see #getWaitFactor
     */
    double DEFAULT_WAIT_FACTOR = 1.5;

    /**
     * Default for whether read-ahead is enabled.
     *
     * @see #isReadAhead
     */
    boolean DEFAULT_READ_AHEAD = true;

    /**
     * Get the maximum number of bytes to cache in a single contiguous range of key/value pairs.
     *
     * <p>
     * Default is {@value #DEFAULT_MAX_RANGE_BYTES}.
     *
     * @return maximum bytes in any one range
     */
    long getMaxRangeBytes();

    /**
     * Configure the maximum number of bytes to cache in a single contiguous range of key/value pairs.
     *
     * <p>
     * Default is {@value #DEFAULT_MAX_RANGE_BYTES}.
     *
     * @param maxRangeBytes maximum bytes in any one range
     * @throws IllegalArgumentException if {@code maxRangeBytes <= 0}
     */
    void setMaxRangeBytes(long maxRangeBytes);

    /**
     * Get the total number of bytes to cache. This is an overal maximum incluging all ranges.
     *
     * <p>
     * Default is {@value #DEFAULT_MAX_TOTAL_BYTES}.
     *
     * @return maximum cached ranges
     */
    long getMaxTotalBytes();

    /**
     * Configure the total number of bytes to cache. This is an overal maximum incluging all ranges.
     *
     * <p>
     * Default is {@value #DEFAULT_MAX_TOTAL_BYTES}.
     *
     * @param maxTotalBytes maximum cached ranges
     * @throws IllegalArgumentException if {@code maxTotalBytes <= 0}
     */
    void setMaxTotalBytes(long maxTotalBytes);

    /**
     * Get the maximum number of contiguous ranges of key/value pairs to allow before we start purging the
     * least recently used ones.
     *
     * <p>
     * Default is {@value #DEFAULT_MAX_RANGES}.
     *
     * @return maximum cached ranges
     */
    int getMaxRanges();

    /**
     * Configure the maximum number of contiguous ranges of key/value pairs to allow before we start purging the
     * least recently used ones.
     *
     * <p>
     * Default is {@value #DEFAULT_MAX_RANGES}.
     *
     * @param maxRanges maximum cached ranges
     * @throws IllegalArgumentException if {@code maxRanges <= 0}
     */
    void setMaxRanges(int maxRanges);

    /**
     * Get the wait factor.
     *
     * <p>
     * The wait factor, when multiplied by the estimated amount of time we expect to have to wait for incoming data that's
     * already on its way, is how long we will actually wait for that data before initiating a new query. A value of zero
     * means never wait, i.e., always start a new query immediately; a value of 1.0 would mean to wait up to the estimated
     * amount of time; a very high value would mean wait until the outstanding query either produces the data or completes.
     *
     * <p>
     * Default is {@value #DEFAULT_WAIT_FACTOR}.
     *
     * @return wait factor
     */
    double getWaitFactor();

    /**
     * Set the wait factor.
     *
     * <p>
     * Default is {@value #DEFAULT_WAIT_FACTOR}.
     *
     * @param waitFactor wait factor
     * @throws IllegalArgumentException if {@code waitFactor} is negative or not finite
     */
    void setWaitFactor(double waitFactor);

    /**
     * Get whether this instance is configured to perform read-ahead.
     *
     * <p>
     * Default is true.
     *
     * @return true if read-ahead is enabled, otherwise false
     */
    boolean isReadAhead();

    /**
     * Configure whether read-ahead is enabled.
     *
     * <p>
     * Default is {@value #DEFAULT_READ_AHEAD}.
     *
     * @param readAhead true to enable read-ahead, false to disable
     */
    void setReadAhead(boolean readAhead);

    /**
     * Copy config parameters.
     *
     * @param dest destination for copied caching parameters
     * @throws IllegalArgumentException if {@code dest} is null
     */
    default void copyCachingConfigTo(CachingConfig dest) {
        Preconditions.checkArgument(dest != null);
        dest.setMaxRangeBytes(this.getMaxRangeBytes());
        dest.setMaxTotalBytes(this.getMaxTotalBytes());
        dest.setMaxRanges(this.getMaxRanges());
        dest.setReadAhead(this.isReadAhead());
    }
}
