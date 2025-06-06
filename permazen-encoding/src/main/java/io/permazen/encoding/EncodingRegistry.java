
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.util.List;

/**
 * A registry of {@link Encoding}s.
 *
 * <p>
 * {@link Encoding}s in an {@link EncodingRegistry} can be looked up by {@link EncodingId} or by Java type.
 * Multiple {@link Encoding}s may support the same Java type, so only the {@link EncodingId} lookup is
 * guaranteed to be unique.
 *
 * <p>
 * Note: {@link Enum} types are not directly handled in the core API layer; instead, the appropriate
 * {@link io.permazen.core.EnumValueEncoding} must be used to encode values as {@link io.permazen.core.EnumValue}s.
 *
 * <p>
 * Instances must be thread safe.
 */
public interface EncodingRegistry {

    /**
     * Get the {@link Encoding} with the given encoding ID in this registry.
     *
     * @param encodingId encoding ID
     * @return corresponding {@link Encoding}, if any, otherwise null
     * @throws IllegalArgumentException if {@code encodingId} is null
     */
    Encoding<?> getEncoding(EncodingId encodingId);

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
     * The implementation in {@link EncodingRegistry} just invokes {@code new EncodingId(alias)}.
     *
     * @param alias encoding ID alias
     * @return corresponding encoding ID, never null
     * @throws IllegalArgumentException if {@code alias} is null or not a valid alias
     */
    default EncodingId idForAlias(String alias) {
        Preconditions.checkArgument(alias != null, "null alias");
        return new EncodingId(alias);
    }

    /**
     * Get the alias (or "nickname") for the given encoding ID in this registry, if any.
     *
     * <p>
     * An {@link EncodingRegistry} may support aliases for some of its encoding ID's.
     * Aliases are simply more friendly names for {@link EncodingId} strings, which are
     * formatted as Uniform Resource Names (URNs).
     *
     * <p>
     * Whereas {@link EncodingId}'s are globally unique, aliases are only meaningful
     * to the particular {@link EncodingRegistry} instance being queried.
     * When a schema is recorded in a database, actual {@link EncodingId}'s are always used.
     *
     * <p>
     * In Permazen's {@link DefaultEncodingRegistry}, the built-in encodings all have aliases;
     * for example, {@code "int"} is an alias for {@code "urn:fdc:permazen.io:2020:int"}.
     * Permazen's built-in encoding aliases are available via {@link EncodingIds#aliasForId EncodingIds.aliasForId()}
     * and {@link EncodingIds#idForAlias EncodingIds.idForAlias()}.
     *
     * <p>
     * If no alias is known for {@code encodingId}, this method should return {@link EncodingId#getId}.
     *
     * <p>
     * Note: an {@link EncodingId} does not need to be actually registered with this instance
     * in order for it to have an alias.
     *
     * <p>
     * The implementation in {@link EncodingRegistry} always returns {@link EncodingId#getId}.
     *
     * @param encodingId encoding ID
     * @return corresponding alias, if any, otherwise {@link EncodingId#getId}
     * @throws IllegalArgumentException if {@code encodingId} is null
     * @see idForAlias
     */
    default String aliasForId(EncodingId encodingId) {
        Preconditions.checkArgument(encodingId != null, "null encodingId");
        return encodingId.getId();
    }

    /**
     * Get all of the {@link Encoding}s in this registry that supports values of the given Java type.
     *
     * <p>
     * The Java type must exactly match the {@link Encoding}'s {@linkplain Encoding#getTypeToken supported Java type}.
     *
     * @param typeToken encoding value type
     * @param <T> encoding value type
     * @return unmodifiable list of {@link Encoding}s supporting Java values of type {@code typeToken}, possibly empty
     * @throws IllegalArgumentException if {@code typeToken} is null
     */
    <T> List<Encoding<T>> getEncodings(TypeToken<T> typeToken);

    /**
     * Get the unique {@link Encoding} in this registry that supports values of the given Java type.
     *
     * <p>
     * The Java type must exactly match the {@link Encoding}'s {@linkplain Encoding#getTypeToken supported Java type}
     * and there must be exactly one such {@link Encoding}, otherwise an {@link IllegalArgumentException} is thrown.
     *
     * @param typeToken encoding value type
     * @param <T> encoding value type
     * @return {@link Encoding} supporting Java values of type {@code typeToken}
     * @throws IllegalArgumentException if {@code typeToken} is null
     * @throws IllegalArgumentException if no {@link Encoding}s supports {@code typeToken}
     * @throws IllegalArgumentException if more than one {@link Encoding} supports {@code typeToken}
     */
    default <T> Encoding<T> getEncoding(TypeToken<T> typeToken) {
        final List<Encoding<T>> encodings = this.getEncodings(typeToken);
        switch (encodings.size()) {
        case 0:
            throw new IllegalArgumentException(String.format("no encodings support values of type %s", typeToken));
        case 1:
            return encodings.get(0);
        default:
            throw new IllegalArgumentException(String.format(
              "multiple encodings support values of type %s: %s", typeToken, encodings));
        }
    }

    /**
     * Get the unique {@link Encoding} in this registry that supports values of the given Java type.
     *
     * <p>
     * This is a convenience method, equivalent to: {@code getEncoding(TypeToken.of(type))}.
     *
     * @param type encoding value type
     * @param <T> encoding value type
     * @return {@link Encoding} supporting Java values of type {@code type}
     * @throws IllegalArgumentException if {@code type} is null
     * @throws IllegalArgumentException if no {@link Encoding}s supports {@code type}
     * @throws IllegalArgumentException if more than one {@link Encoding} supports {@code type}
     */
    default <T> Encoding<T> getEncoding(Class<T> type) {
        Preconditions.checkArgument(type != null, "null type");
        return this.getEncoding(TypeToken.of(type));
    }
}
