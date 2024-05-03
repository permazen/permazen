
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.io.Serializable;
import java.util.Objects;

/**
 * Support superclass for {@link Encoding} implementations.
 *
 * <p>
 * Instances are {@link Serializable} if their default values are (typically the default value is null, making this the case).
 *
 * @param <T> The associated Java type
 * @see EncodingRegistry
 */
public abstract class AbstractEncoding<T> implements Encoding<T>, Serializable {

    private static final long serialVersionUID = 917908384411157979L;

    protected final EncodingId encodingId;
    protected final TypeToken<T> typeToken;
    @SuppressWarnings("serial")
    private final T defaultValue;

    private transient byte[] defaultValueBytes;

// Constructors

    /**
     * Constructor.
     *
     * @param encodingId encoding ID for this encoding, or null to be anonymous
     * @param typeToken Java type for this encoding's values
     * @param defaultValue default value for this encoding; must be null if this encoding supports nulls
     * @throws IllegalArgumentException if {@code typeToken} is null
     */
    protected AbstractEncoding(EncodingId encodingId, TypeToken<T> typeToken, T defaultValue) {
        Preconditions.checkArgument(typeToken != null, "null typeToken");
        this.encodingId = encodingId;
        this.typeToken = typeToken;
        this.defaultValue = defaultValue;
    }

    /**
     * Constructor.
     *
     * @param encodingId encoding ID for this encoding, or null to be anonymous
     * @param type Java type for this encoding's values
     * @param defaultValue default value for this encoding; must be null if this encoding supports nulls
     * @throws IllegalArgumentException if {@code type} is null
     */
    protected AbstractEncoding(EncodingId encodingId, Class<T> type, T defaultValue) {
        this(encodingId, TypeToken.of(AbstractEncoding.noNull(type, "type")), defaultValue);
    }

    static <T> T noNull(T value, String name) {
        if (value == null)
            throw new IllegalArgumentException("null " + name);
        return value;
    }

// Public methods

    @Override
    public final EncodingId getEncodingId() {
        return this.encodingId;
    }

    @Override
    public final TypeToken<T> getTypeToken() {
        return this.typeToken;
    }

    @Override
    public final T getDefaultValue() {
        Preconditions.checkState(this.supportsNull() || this.defaultValue != null, "invalid null default value");
        return this.defaultValue;
    }

    @Override
    public byte[] getDefaultValueBytes() {
        if (this.defaultValueBytes == null)
            this.defaultValueBytes = Encoding.super.getDefaultValueBytes();
        return this.defaultValueBytes.clone();
    }

// Object

    @Override
    public String toString() {
        String description;
        if (this.encodingId == null)
            description = "anonymous";
        else if ((description = this.encodingId.getId()).startsWith(EncodingIds.PERMAZEN_PREFIX))
            description = "\"" + description.substring(EncodingIds.PERMAZEN_PREFIX.length()) + "\"";
        else
            description = "\"" + description + "\"";
        return "encoding " + description + " <" + this.typeToken + ">";
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode()
          ^ Objects.hashCode(this.encodingId)
          ^ this.typeToken.hashCode()
          ^ (Boolean.hashCode(this.supportsNull()) << 1)
          ^ (Boolean.hashCode(this.hasPrefix0x00()) << 2)
          ^ (Boolean.hashCode(this.hasPrefix0xff()) << 3);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final AbstractEncoding<?> that = (AbstractEncoding<?>)obj;
        return Objects.equals(this.encodingId, that.encodingId)
          && this.typeToken.equals(that.typeToken)
          && this.supportsNull() == that.supportsNull()
          && this.hasPrefix0x00() == that.hasPrefix0x00()
          && this.hasPrefix0xff() == that.hasPrefix0xff();
    }
}
