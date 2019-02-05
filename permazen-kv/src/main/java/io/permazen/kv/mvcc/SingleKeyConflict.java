
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteUtil;

import java.util.Arrays;

abstract class SingleKeyConflict extends Conflict {

    private final String type;
    private final byte[] key;

    SingleKeyConflict(String type, byte[] key) {
        Preconditions.checkArgument(key != null, "null key");
        this.type = type;
        this.key = key;
    }

    /**
     * Get the key at which the conflict occurred.
     *
     * <p>
     * Note: the returned {@code key} is not a copy, so the caller should not modify the data therein.
     *
     * @return the affected key
     */
    public byte[] getKey() {
        return this.key;
    }

// Object

    @Override
    public int hashCode() {
        return this.getClass().hashCode() ^ Arrays.hashCode(this.key);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final SingleKeyConflict that = (SingleKeyConflict)obj;
        return Arrays.equals(this.key, that.key);
    }

    @Override
    public String toString() {
        return this.type + " conflict at " + ByteUtil.toString(this.key);
    }
}
