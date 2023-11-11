
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * {@link Stream} utility methods.
 */
public final class Streams {

    private Streams() {
    }

    /**
     * Iterate over the items in the given stream, in order (if the stream is ordered),
     * one-at-a-time, in the current thread.
     *
     * @param other instance to copy
     * @see <a href="https://github.com/google/guava/issues/6831">Guava issue #6831</a>
     * @throws IllegalArgumentException if either parameter is null
     */
    public static <T> void iterate(Stream<T> stream, Consumer<? super T> action) {
        Preconditions.checkArgument(stream != null, "null stream");
        Preconditions.checkArgument(action != null, "null action");
        for (Iterator<T> i = stream.iterator(); i.hasNext(); )
            action.accept(i.next());
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
            Streams.iterate(stream2, s -> { });
        }
    }
}
