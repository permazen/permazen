
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.io.Serializable;
import java.util.Objects;

/**
 * Support superclass for {@link FieldType} implementations.
 *
 * <p>
 * Instances are {@link Serializable} if their default values are (typically the default value is null, making this the case).
 *
 * @param <T> The associated Java type
 * @see FieldTypeRegistry
 */
public abstract class AbstractFieldType<T> implements FieldType<T>, Serializable {

    private static final long serialVersionUID = 917908384411157979L;

    protected final EncodingId encodingId;
    protected final TypeToken<T> typeToken;

    private final T defaultValueObject;

    private transient byte[] defaultValue;

// Constructors

    /**
     * Constructor.
     *
     * @param encodingId encoding ID, or null to be anonymous
     * @param typeToken Java type for the field's values
     * @param defaultValue default value for this type
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     */
    protected AbstractFieldType(EncodingId encodingId, TypeToken<T> typeToken, T defaultValue) {
        Preconditions.checkArgument(typeToken != null, "null typeToken");
        this.encodingId = encodingId;
        this.typeToken = typeToken;
        this.defaultValueObject = defaultValue;
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
    public final T getDefaultValueObject() {
        return this.defaultValueObject;
    }

// Object

    @Override
    public String toString() {
        String description;
        if (this.encodingId == null)
            description = "anonymous";
        else if ((description = this.encodingId.getId()).startsWith(EncodingIds.PERMAZEN_PREFIX))
            description = "\"" + description.substring(EncodingIds.PERMAZEN_PREFIX.length()) + "\"";
        return "field type " + description + " <" + this.typeToken + ">";
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode()
          ^ Objects.hashCode(this.encodingId)
          ^ this.typeToken.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final AbstractFieldType<?> that = (AbstractFieldType<?>)obj;
        return Objects.equals(this.encodingId, that.encodingId)
          && this.typeToken.equals(that.typeToken);
    }
}
