
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

import io.permazen.tuple.Tuple2;

import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Caches parsed {@link ReferencePath}s.
 */
class ReferencePathCache {

    private final LoadingCache<Key, ReferencePath> cache;

    /**
     * Constructor.
     *
     * @param pdb {@link Permazen} against which to resolve object and field names
     * @throws IllegalArgumentException if {@code pdb} is null
     */
    ReferencePathCache(final Permazen pdb) {
        Preconditions.checkArgument(pdb != null, "null pdb");
        this.cache = CacheBuilder.newBuilder().softValues().build(new CacheLoader<Key, ReferencePath>() {
            @Override
            public ReferencePath load(Key key) {
                return new ReferencePath(pdb, key.getStartTypes(), key.getPath());
            }
        });
    }

    /**
     * Get/create a {@link ReferencePath}.
     *
     * @see ReferencePath#ReferencePath
     */
    public ReferencePath get(Set<PermazenClass<?>> startTypes, String path) {
        Throwable cause;
        try {
            return this.cache.get(new Key(startTypes, path));
        } catch (ExecutionException e) {
            cause = e.getCause() != null ? e.getCause() : e;
        } catch (UncheckedExecutionException e) {
            cause = e.getCause() != null ? e.getCause() : e;
        }
        Throwables.throwIfUnchecked(cause);
        throw new RuntimeException("internal error", cause);
    }

// Key

    private static final class Key extends Tuple2<Set<PermazenClass<?>>, String> {

        Key(Set<PermazenClass<?>> startTypes, String path) {
            super(startTypes, path);
        }

        public Set<PermazenClass<?>> getStartTypes() {
            return this.getValue1();
        }

        public String getPath() {
            return this.getValue2();
        }
    }
}
