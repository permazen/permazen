
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

import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * Caches parsed {@link ReferencePath}s.
 */
class ReferencePathCache {

    private final Permazen jdb;
    private final LoadingCache<Key, ReferencePath> cache;

    /**
     * Constructor.
     *
     * @param jdb {@link Permazen} against which to resolve object and field names
     * @throws IllegalArgumentException if {@code jdb} is null
     */
    ReferencePathCache(Permazen jdb) {
        Preconditions.checkArgument(jdb != null, "null jdb");
        this.jdb = jdb;
        this.cache = CacheBuilder.newBuilder().softValues().build(new CacheLoader<Key, ReferencePath>() {
            @Override
            public ReferencePath load(Key key) {
                return new ReferencePath(ReferencePathCache.this.jdb,
                  key.getStartType(), key.getPath(), key.isWithTargetField(), key.isLastIsSubField());
            }
        });
    }

    /**
     * Get/create a {@link ReferencePath}.
     *
     * @see ReferencePath#ReferencePath
     */
    public ReferencePath get(Class<?> startType, String path, boolean withTargetField, Boolean lastIsSubField) {
        Throwable cause;
        try {
            return this.cache.get(new Key(startType, path, withTargetField, lastIsSubField));
        } catch (ExecutionException e) {
            cause = e.getCause() != null ? e.getCause() : e;
        } catch (UncheckedExecutionException e) {
            cause = e.getCause() != null ? e.getCause() : e;
        }
        Throwables.throwIfUnchecked(cause);
        throw new RuntimeException("internal error", cause);
    }

// Key

    private static class Key {

        private final Class<?> startType;
        private final String path;
        private final boolean withTargetField;
        private final Boolean lastIsSubField;

        Key(Class<?> startType, String path, boolean withTargetField, Boolean lastIsSubField) {
            this.startType = startType;
            this.path = path;
            this.withTargetField = withTargetField;
            this.lastIsSubField = lastIsSubField;
        }

        public Class<?> getStartType() {
            return this.startType;
        }

        public String getPath() {
            return this.path;
        }

        public boolean isWithTargetField() {
            return this.withTargetField;
        }

        public Boolean isLastIsSubField() {
            return this.lastIsSubField;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final Key that = (Key)obj;
            return this.startType.equals(that.startType)
              && this.path.equals(that.path)
              && this.withTargetField == that.withTargetField
              && Objects.equals(this.lastIsSubField, that.lastIsSubField);
        }

        @Override
        public int hashCode() {
            return this.startType.hashCode()
              ^ this.path.hashCode()
              ^ (withTargetField ? 1 : 0)
              ^ Objects.hashCode(this.lastIsSubField);
        }
    }
}
