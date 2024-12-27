
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import java.util.Objects;

abstract class SingleKeyConflict extends Conflict {

    private final String type;
    private final ByteData key;

    SingleKeyConflict(String type, ByteData key) {
        Preconditions.checkArgument(type != null, "null type");
        Preconditions.checkArgument(key != null, "null key");
        this.type = type;
        this.key = key;
    }

    /**
     * Get the key at which the conflict occurred.
     *
     * @return the affected key
     */
    public ByteData getKey() {
        return this.key;
    }

// Object

    @Override
    public int hashCode() {
        return this.getClass().hashCode()
          ^ this.type.hashCode()
          ^ this.key.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final SingleKeyConflict that = (SingleKeyConflict)obj;
        return Objects.equals(this.type, that.type)
          && this.type.equals(that.type)
          && this.key.equals(that.key);
    }

    @Override
    public String toString() {
        return this.type + " conflict at " + ByteUtil.toString(this.key);
    }
}
