
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.reflect.TypeToken;

import io.permazen.core.type.EnumValueFieldType;

import java.util.List;

/**
 * A registry of {@link FieldType}s.
 *
 * <p>
 * {@link FieldType}s in a {@link FieldTypeRegistry} can be looked up by {@link EncodingId} or by Java type.
 * Multiple {@link FieldType}s may support the same Java type, so only the {@link EncodingId} lookup is
 * guaranteed to be unique.
 *
 * <p>
 * Note: {@link Enum} types are not directly handled in the core API layer; instead, the appropriate
 * {@link EnumValueFieldType} must be used to encode values as {@link EnumValue}s.
 *
 * <p>
 * Instances must be thread safe.
 */
public interface FieldTypeRegistry {

    /**
     * Get the {@link FieldType} with the given encoding ID in this registry.
     *
     * @param encodingId encoding ID
     * @return corresponding {@link FieldType}, if any, otherwise null
     * @throws IllegalArgumentException if {@code encodingId} is null
     */
    FieldType<?> getFieldType(EncodingId encodingId);

    /**
     * Get all of the {@link FieldType}s in this registry that supports values of the given Java type.
     *
     * <p>
     * The Java type must exactly match the {@link FieldType}'s {@linkplain FieldType#getTypeToken supported Java type}.
     *
     * @param typeToken field value type
     * @param <T> field value type
     * @return unmodifiable list of {@link FieldType}s supporting Java values of type {@code typeToken}, possibly empty
     * @throws IllegalArgumentException if {@code typeToken} is null
     */
    <T> List<FieldType<T>> getFieldTypes(TypeToken<T> typeToken);

    /**
     * Get the unique {@link FieldType} in this registry that supports values of the given Java type.
     *
     * <p>
     * The Java type must exactly match the {@link FieldType}'s {@linkplain FieldType#getTypeToken supported Java type}
     * and there must be exactly one such {@link FieldType}, otherwise an {@link IllegalArgumentException} is thrown.
     *
     * @param typeToken field value type
     * @param <T> field value type
     * @return {@link FieldType} supporting Java values of type {@code typeToken}
     * @throws IllegalArgumentException if {@code typeToken} is null
     * @throws IllegalArgumentException if no {@link FieldType}s supports {@code typeToken}
     * @throws IllegalArgumentException if more than one {@link FieldType} supports {@code typeToken}
     */
    default <T> FieldType<T> getFieldType(TypeToken<T> typeToken) {
        final List<FieldType<T>> fieldTypes = this.getFieldTypes(typeToken);
        switch (fieldTypes.size()) {
        case 0:
            throw new IllegalArgumentException("no types support values of type " + typeToken);
        case 1:
            return fieldTypes.get(0);
        default:
            throw new IllegalArgumentException("multiple types support values of type " + typeToken + ": " + fieldTypes);
        }
    }
}
