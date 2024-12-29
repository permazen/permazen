
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import com.google.common.base.Preconditions;

import io.permazen.core.CompositeIndex;
import io.permazen.core.Field;
import io.permazen.core.ObjId;
import io.permazen.kv.KVStore;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.ParseContext;

import java.util.Objects;

/**
 * Represents a point of corruption or inconsistency in a Permazen key/value database.
 *
 * <p>
 * {@link Issue}s are specific to a single key/value pair in the database.
 */
public abstract class Issue {

    private static final int HEX_STRING_LIMIT = 100;

    private final String description;
    private final ByteData key;
    private final ByteData oldValue;
    private final ByteData newValue;

    private String detail;

    /**
     * Constructor.
     *
     * @param description short description of the issue
     * @param key key of invalid key/value pair
     * @param oldValue the original, invalid value for {@code key}, or null if {@code key} was missing or don't care
     * @param newValue the corrected replacement value for {@code key}, or null if {@code key} should be deleted
     * @throws IllegalArgumentException if {@code key} is null
     * @throws IllegalArgumentException if {@code oldValue} and {@code newValue} are equal
     */
    protected Issue(String description, ByteData key, ByteData oldValue, ByteData newValue) {
        Preconditions.checkArgument(description != null, "null description");
        Preconditions.checkArgument(key != null, "null key");
        Preconditions.checkArgument(!Objects.equals(oldValue, newValue) || oldValue == null, "newValue = oldValue");
        this.description = description;
        this.key = key;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    /**
     * Get the databse key.
     *
     * @return key of this issue
     */
    public ByteData getKey() {
        return this.key;
    }

    /**
     * Get the original value, if any.
     *
     * @return original value, null if key/value pair was missing
     */
    public ByteData getOldValue() {
        return this.oldValue;
    }

    /**
     * Get the replacement value, if any.
     *
     * @return replacement value, null if key/value pair was missing or should be deleted
     */
    public ByteData getNewValue() {
        return this.newValue;
    }

    /**
     * Apply the fix for this issue to the given {@link KVStore}.
     *
     * <p>
     * This overwrites the key/value pair with the new value, or deletes it if {@link #getNewValue} is null.
     *
     * @param kvstore target key/value store
     * @throws IllegalArgumentException if {@code kvstore} is null
     */
    public void apply(KVStore kvstore) {
        Preconditions.checkArgument(kvstore != null, "null kvstore");
        if (this.newValue != null)
            kvstore.put(this.key, this.newValue);
        else
            kvstore.remove(this.key);
    }

    public Issue setDetail(String format, Object... args) {
        this.detail = String.format(format, args);
        return this;
    }

    public Issue setDetail(ObjId id, String format, Object... args) {
        return this.setDetail("for object %s: ", id, String.format(format, args));
    }

    public Issue setDetail(ObjId id, Field<?> field, String format, Object... args) {
        return this.setDetail("for object %s %s: %s", id, field, String.format(format, args));
    }

    public Issue setDetail(ObjId id, CompositeIndex index, String format, Object... args) {
        return this.setDetail("for object %s %s: %s", id, index, String.format(format, args));
    }

    public Issue setDetail(Index<?> index, String format, Object... args) {
        return this.setDetail("for %s: %s", index, String.format(format, args));
    }

// Object

    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append(this.description)
          .append(": key [")
          .append(ParseContext.truncate(ByteUtil.toString(this.key), HEX_STRING_LIMIT))
          .append(']');
        if (this.oldValue != null && this.newValue != null) {
            buf.append(": incorrect value [")
              .append(ParseContext.truncate(ByteUtil.toString(this.oldValue), HEX_STRING_LIMIT))
              .append("]; corrected value [")
              .append(ParseContext.truncate(ByteUtil.toString(this.newValue), HEX_STRING_LIMIT))
              .append(']');
        } else if (this.oldValue != null) {
            buf.append(", value [")
              .append(ParseContext.truncate(ByteUtil.toString(this.oldValue), HEX_STRING_LIMIT))
              .append(']');
        } else if (this.newValue != null && !this.newValue.isEmpty()) {
            buf.append(", corrected value [")
              .append(ParseContext.truncate(ByteUtil.toString(this.newValue), HEX_STRING_LIMIT))
              .append(']');
        }
        if (this.detail != null) {
            buf.append(": ")
              .append(this.detail);
        }
        return buf.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final Issue that = (Issue)obj;
        return Objects.equals(this.key, that.key)
          && Objects.equals(this.oldValue, that.oldValue)
          && Objects.equals(this.newValue, that.newValue)
          && Objects.equals(this.description, that.description)
          && Objects.equals(this.detail, that.detail);
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode()
          ^ Objects.hashCode(this.key)
          ^ Objects.hashCode(this.oldValue)
          ^ Objects.hashCode(this.newValue)
          ^ Objects.hashCode(this.description)
          ^ Objects.hashCode(this.detail);
    }
}
