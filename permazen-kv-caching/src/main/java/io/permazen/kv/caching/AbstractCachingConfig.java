
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.caching;

import com.google.common.base.Preconditions;

/**
 * Support superclass for {@link CachingConfig} implementations.
 */
public abstract class AbstractCachingConfig implements CachingConfig {

    int maxRanges = DEFAULT_MAX_RANGES;
    long maxRangeBytes = DEFAULT_MAX_RANGE_BYTES;
    long maxTotalBytes = DEFAULT_MAX_TOTAL_BYTES;
    double waitFactor = DEFAULT_WAIT_FACTOR;
    boolean readAhead = DEFAULT_READ_AHEAD;

    /**
     * Constructor.
     */
    protected AbstractCachingConfig() {
    }

    @Override
    public synchronized long getMaxRangeBytes() {
        return this.maxRangeBytes;
    }

    @Override
    public synchronized void setMaxRangeBytes(long maxRangeBytes) {
        Preconditions.checkArgument(maxRanges > 0, "maxRangeBytes <= 0");
        this.maxRangeBytes = maxRangeBytes;
    }

    @Override
    public synchronized long getMaxTotalBytes() {
        return this.maxTotalBytes;
    }

    @Override
    public synchronized void setMaxTotalBytes(long maxTotalBytes) {
        Preconditions.checkArgument(maxTotalBytes > 0, "maxTotalBytes <= 0");
        this.maxTotalBytes = maxTotalBytes;
    }

    @Override
    public synchronized int getMaxRanges() {
        return this.maxRanges;
    }

    @Override
    public synchronized void setMaxRanges(int maxRanges) {
        Preconditions.checkArgument(maxRanges > 0, "maxRanges <= 0");
        this.maxRanges = maxRanges;
    }

    @Override
    public double getWaitFactor() {
        return this.waitFactor;
    }

    @Override
    public void setWaitFactor(double waitFactor) {
        Preconditions.checkArgument(Double.isFinite(waitFactor), "non-finite waitFactor");
        Preconditions.checkArgument(waitFactor >= 0, "waitFactor < 0");
        this.waitFactor = waitFactor;
    }

    @Override
    public synchronized boolean isReadAhead() {
        return this.readAhead;
    }

    @Override
    public synchronized void setReadAhead(boolean readAhead) {
        this.readAhead = readAhead;
    }
}
