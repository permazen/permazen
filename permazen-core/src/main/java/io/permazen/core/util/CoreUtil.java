
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.util;

import com.google.common.base.Preconditions;

import io.permazen.encoding.Encoding;
import io.permazen.kv.KeyRange;
import io.permazen.util.BoundType;
import io.permazen.util.Bounds;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;

/**
 * Miscellaneous utility methods.
 */
public final class CoreUtil {

    private CoreUtil() {
    }

    /**
     * Calculate the {@link KeyRange} for the given {@link Encoding} that includes exactly those encoded values
     * that lie within the given value bounds.
     *
     * @param encoding encoding for values
     * @param bounds bounds to impose
     * @return {@link KeyRange} corresponding to {@code bounds}
     * @throws IllegalArgumentException if {@code encoding} or {@code bounds} is null
     */
    public static <T> KeyRange getKeyRange(Encoding<T> encoding, Bounds<? extends T> bounds) {

        // Sanity check
        Preconditions.checkArgument(encoding != null, "null encoding");
        Preconditions.checkArgument(bounds != null, "null bounds");

        // Get inclusive byte[] lower bound
        byte[] lowerBound = ByteUtil.EMPTY;
        final BoundType lowerBoundType = bounds.getLowerBoundType();
        if (!BoundType.NONE.equals(lowerBoundType)) {
            final ByteWriter writer = new ByteWriter();
            try {
                encoding.write(writer, bounds.getLowerBound());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(
                  "invalid lower bound %s for %s", bounds.getLowerBound(), encoding), e);
            }
            lowerBound = writer.getBytes();
            if (!lowerBoundType.isInclusive())
                lowerBound = ByteUtil.getNextKey(lowerBound);
        }

        // Get exclusive byte[] upper bound
        byte[] upperBound = null;
        final BoundType upperBoundType = bounds.getUpperBoundType();
        if (!BoundType.NONE.equals(upperBoundType)) {
            final ByteWriter writer = new ByteWriter();
            try {
                encoding.write(writer, bounds.getUpperBound());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(
                  "invalid upper bound %s for %s", bounds.getUpperBound(), encoding), e);
            }
            upperBound = writer.getBytes();
            if (upperBoundType.isInclusive())
                upperBound = ByteUtil.getNextKey(upperBound);
        }

        // Done
        return new KeyRange(lowerBound, upperBound);
    }
}
