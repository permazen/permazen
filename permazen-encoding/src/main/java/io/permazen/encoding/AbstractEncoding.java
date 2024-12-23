
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.util.ByteData;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Support superclass for {@link Encoding} implementations.
 *
 * <p>
 * Instances are {@link Serializable} if their default value suppliers are.
 *
 * @param <T> The associated Java type
 * @see EncodingRegistry
 */
public abstract class AbstractEncoding<T> implements Encoding<T>, Serializable {

    private static final long serialVersionUID = 917908384411157979L;

    protected final EncodingId encodingId;
    protected final TypeToken<T> typeToken;

    @SuppressWarnings("serial")
    private final Supplier<? extends T> defaultValueSupplier;

    private transient ByteData defaultValueBytes;

// Constructors

    /**
     * Constructor.
     *
     * @param encodingId encoding ID for this encoding, or null to be anonymous
     * @param typeToken Java type for this encoding's values
     * @param defaultValueSupplier supplies the default value for this encoding; must supply null if this encoding supports nulls;
     *  may be null if this encoding has no default value
     * @throws IllegalArgumentException if {@code typeToken} is null
     */
    protected AbstractEncoding(EncodingId encodingId, TypeToken<T> typeToken, Supplier<? extends T> defaultValueSupplier) {
        Preconditions.checkArgument(typeToken != null, "null typeToken");
        this.encodingId = encodingId;
        this.typeToken = typeToken;
        this.defaultValueSupplier = defaultValueSupplier;
    }

    /**
     * Constructor.
     *
     * @param encodingId encoding ID for this encoding, or null to be anonymous
     * @param type Java type for this encoding's values
     * @param defaultValueSupplier supplies the default value for this encoding; must supply null if this encoding supports nulls;
     *  may be null if this encoding has no default value
     * @throws IllegalArgumentException if {@code type} is null
     */
    protected AbstractEncoding(EncodingId encodingId, Class<T> type, Supplier<? extends T> defaultValueSupplier) {
        this(encodingId, TypeToken.of(AbstractEncoding.noNull(type, "type")), defaultValueSupplier);
    }

    /**
     * Constructor for anonymous encodings with no default value.
     *
     * @param type Java type for this encoding's values
     * @throws IllegalArgumentException if {@code type} is null
     */
    protected AbstractEncoding(TypeToken<T> type) {
        this(null, type, null);
    }

    /**
     * Constructor for anonymous encodings with no default value.
     *
     * @param type Java type for this encoding's values
     * @throws IllegalArgumentException if {@code type} is null
     */
    protected AbstractEncoding(Class<T> type) {
        this(null, type, null);
    }

    static <T> T noNull(T value, String name) {
        if (value == null)
            throw new IllegalArgumentException(String.format("null %s", name));
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
        if (this.defaultValueSupplier == null) {
            final StringBuilder buf = new StringBuilder();
            buf.append("encoding");
            if (this.encodingId != null)
                buf.append(" \"").append(this.encodingId).append('"');
            buf.append(" for ").append(this.typeToken).append(" has no default value");
            throw new UnsupportedOperationException(buf.toString());
        }
        return this.defaultValueSupplier.get();
    }

    @Override
    public ByteData getDefaultValueBytes() {
        if (this.defaultValueBytes == null)
            this.defaultValueBytes = Encoding.super.getDefaultValueBytes();
        return this.defaultValueBytes;
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
