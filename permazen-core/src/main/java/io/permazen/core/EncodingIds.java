
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

/**
 * {@link EncodingId}'s for Permazen built-in types.
 *
 * @see EncodingId
 */
public final class EncodingIds {

    /**
     * Prefix for Permazen built-in encodings.
     */
    public static final String PERMAZEN_PREFIX = "urn:fdc:permazen.io:2020:";

    private EncodingIds() {
    }

    /**
     * Build an encoding ID for a built-in Permazen type using the given suffix.
     *
     * @param suffix ID suffix
     * @throws IllegalArgumentException if {@code suffix} is null or invalid
     */
    public static EncodingId builtin(String suffix) {
        Preconditions.checkArgument(suffix != null, "null suffix");
        return new EncodingId(PERMAZEN_PREFIX + suffix);
    }
}
