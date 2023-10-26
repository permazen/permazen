
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import io.permazen.kv.KVStore;

/**
 * Represents the deletion of a range of key/value pairs in a {@link SimpleKVTransaction}.
 */
class Del extends Mutation {

    Del(byte[] min) {
        super(min);
    }

    Del(byte[] min, byte[] max) {
        super(min, max);
    }

    @Override
    public void apply(KVStore kv) {
        kv.removeRange(this.min, this.max);
    }
}
