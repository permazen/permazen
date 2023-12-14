
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Preconditions;

import java.util.stream.Stream;

/**
 * {@link Stream} utility methods.
 */
public final class Streams {

    private Streams() {
    }

    /**
     * Read and discard everything from the given {@link Stream}.
     *
     * <p>
     * This method also ensures that the stream is always closed, even if an exception is thrown.
     *
     * @param stream stream to exhaust
     * @throws IllegalArgumentException if {@code stream} is null
     */
    public static void exhaust(Stream<?> stream) {
        Preconditions.checkArgument(stream != null, "null stream");
        try (Stream<?> stream2 = stream) {
            stream2.iterator().forEachRemaining(s -> { });
        }
    }
}
