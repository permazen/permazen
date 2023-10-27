
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
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
     * Get the encoding ID corresponding to the given alias (or "nickname"), if any.
     *
     * <p>
     * See {@link #aliasForId aliasForId()} for details on aliases.
     *
     * <p>
     * If {@code alias} is a valid alias, this method should return the corresponding
     * {@link EncodingId}. If {@code alias} is not a valid alias, but is a valid encoding
     * ID in string form, this method should return the corresponding {@link EncodingId}
     * as if by {@code new EncodingId(alias)}. Otherwise, this method should throw
     * {@link IllegalArgumentException}.
     *
     * <p>
     * Note: the {@link EncodingId} corresponding to an alias does not need to be actually
     * registered with this instance in order for the alias to be valid.
     *
     * <p>
     * The implementation in {@link FieldTypeRegistry} just invokes {@code new EncodingId(alias)}.
     *
     * @param alias encoding ID alias
     * @return corresponding encoding ID, never null
     * @throws IllegalArgumentException if {@code alias} is not a valid alias
     */
    default EncodingId idForAlias(String alias) {
        Preconditions.checkArgument(alias != null, "null alias");
        return new EncodingId(alias);
    }

    /**
     * Get the alias (or "nickname") for the given encoding ID in this registry, if any.
     *
     * <p>
     * A {@link FieldTypeRegistry} may support aliases for some of its encoding ID's.
     * Aliases are simply more friendly names for encoding IDs, which are officially
     * expressed as Uniform Resource Names (URNs).
     *
     * <p>
     * Whereas {@link EncodingId}'s are globally unique, aliases are only meaningful
     * to the particular {@link FieldTypeRegistry} instance being queried.
     * When a schema is recorded in a database, actual {@link EncodingId}'s are always used.
     *
     * <p>
     * In Permazen's {@link DefaultFieldTypeRegistry}, the built-in types all have aliases;
     * for example, {@code "int"} is an alias for {@code "urn:fdc:permazen.io:2020:int"}.
     *
     * <p>
     * If no alias is known for {@code encodingId}, this method should return {@link EncodingId#getId}.
     *
     * <p>
     * Note: an {@link EncodingId} does not need to be actually registered with this instance
     * in order for it to have an alias.
     *
     * <p>
     * The implementation in {@link FieldTypeRegistry} always returns {@link EncodingId#getId}.
     *
     * @param name encoding ID
     * @return corresponding alias, if any, otherwise {@link EncodingId#getId}
     * @throws IllegalArgumentException if {@code alias} is null
     * @see idForAlias
     */
    default String aliasForId(EncodingId encodingId) {
        Preconditions.checkArgument(encodingId != null, "null encodingId");
        return encodingId.getId();
    }

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
