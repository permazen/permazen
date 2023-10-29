
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

/**
 * {@link EncodingId}'s for Permazen built-in encodings.
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
     * Build an encoding ID for a built-in Permazen encoding, using the given suffix.
     *
     * @param suffix ID suffix
     * @throws IllegalArgumentException if {@code suffix} is null or invalid
     */
    public static EncodingId builtin(String suffix) {
        Preconditions.checkArgument(suffix != null, "null suffix");
        Preconditions.checkArgument(EncodingIds.isValidBuiltinSuffix(suffix), "invalid suffix");
        return new EncodingId(PERMAZEN_PREFIX + suffix);
    }

    /**
     * Determine if the given string is a valid Permazen built-in encoding ID suffix.
     *
     * <p>
     * Valid suffixes must start with an ASCII letter; contain only ASCII letters,
     * digits, underscores, and dashes; not end with a dash; and not have any
     * consecutive dashes. They may have up to 255 trailing {@code []} array dimensions.
     *
     * @param suffix ID suffix
     * @return true if {@code suffix} is a valid suffix, false otherwise
     * @throws IllegalArgumentException if {@code suffix} is null
     */
    public static boolean isValidBuiltinSuffix(String suffix) {
        Preconditions.checkArgument(suffix != null, "null suffix");
        return suffix.matches("\\p{Alpha}(-?[\\p{Alnum}_]+)*(\\[\\]){0,255}");
    }
}
