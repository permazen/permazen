
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVStore;
import io.permazen.kv.KeyRange;
import io.permazen.kv.util.KeyWatchTracker;
import io.permazen.util.ByteData;

/**
 * Represents an outstanding {@link SimpleKVTransaction} mutation.
 */
abstract class Mutation extends KeyRange {

    protected Mutation(ByteData min, ByteData max) {
        super(min, max);
        Preconditions.checkArgument(max == null || !min.equals(max), "empty range");
    }

    protected Mutation(ByteData value) {
        super(value);
    }

    public abstract void apply(KVStore kv);

    public boolean trigger(KeyWatchTracker keyWatchTracker) {
        return keyWatchTracker.trigger(this);
    }

    public static Mutation key(ByteData value) {
        return new Mutation(value) {
            @Override
            public void apply(KVStore kv) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
