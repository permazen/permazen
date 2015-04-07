
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.simple;

import java.util.Arrays;

import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyRange;

/**
 * Represents an outstanding {@link SimpleKVTransaction} mutation.
 */
abstract class Mutation extends KeyRange {

    protected Mutation(byte[] min, byte[] max) {
        super(min, max);
        if (max != null && Arrays.equals(min, max))
            throw new IllegalArgumentException("empty range");
    }

    protected Mutation(byte[] value) {
        super(value);
    }

    public abstract void apply(KVStore kv);

    public static Mutation key(byte[] value) {
        return new Mutation(value) {
            @Override
            public void apply(KVStore kv) {
                throw new UnsupportedOperationException();
            }
        };
    }
}

