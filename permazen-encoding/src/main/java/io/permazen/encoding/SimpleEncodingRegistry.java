
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.dellroad.stuff.java.Primitive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A straightforward {@link EncodingRegistry} implementation that creates object array types on demand.
 *
 * <p>
 * The {@link #add add()} method only accepts non-array types and primitive array types. All other
 * array types are automatically created on demand via {@link #buildArrayEncoding buildArrayEncoding()}.
 */
public class SimpleEncodingRegistry implements EncodingRegistry {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    final HashMap<EncodingId, Encoding<?>> byId = new HashMap<>();
    final HashMap<TypeToken<?>, List<Encoding<?>>> byType = new HashMap<>();

// Public Methods

    /**
     * Add a new {@link Encoding} to this registry.
     *
     * <p>
     * The type's encoding ID must not contain any array dimensions except for single dimension primitive array types.
     *
     * @param encoding the {@link Encoding} to register
     * @return true if it was added, false if it was already registered
     * @throws IllegalArgumentException if {@code encoding} is null
     * @throws IllegalArgumentException if {@code encoding}'s encoding ID is null (i.e., {@code encoding} is anonymous)
     * @throws IllegalArgumentException if {@code encoding}'s encoding ID conflicts with an existing, but different, encoding
     * @throws IllegalArgumentException if {@code encoding}'s encoding ID has one or more array dimensions
     */
    public synchronized boolean add(Encoding<?> encoding) {
        Preconditions.checkArgument(encoding != null, "null encoding");
        final EncodingId encodingId = encoding.getEncodingId();
        Preconditions.checkArgument(encodingId != null, "encoding is anonymous");
        final TypeToken<?> elementTypeToken = encoding.getTypeToken().getComponentType();
        Preconditions.checkArgument((elementTypeToken == null) == (encodingId.getArrayDimensions() == 0),
          "inconsistent encoding ID \"" + encodingId + "\" for type " + encoding.getTypeToken());
        Preconditions.checkArgument(elementTypeToken == null || elementTypeToken.getRawType().isPrimitive(),
          "illegal array type \"" + encodingId + "\"");
        final Encoding<?> otherEncoding = this.byId.get(encodingId);
        if (otherEncoding != null) {
            if (!otherEncoding.equals(encoding)) {
                throw new IllegalArgumentException(
                  String.format("encoding ID \"%s\" for encoding %s conflicts with existing encoding %s",
                  encodingId, encoding, otherEncoding));
            }
            return false;
        }
        this.register(encodingId, encoding);
        return true;
    }

    /**
     * Add a null-safe version of the given non-null supporting {@link Encoding} to this registry.
     *
     * <p>
     * The {@code encoding} is wrapped in a {@link NullSafeEncoding} to add null value support.
     *
     * @param encodingId the ID for the newly added encoding
     * @param inner the inner non-null supporting {@link Encoding}
     * @return true if it was added, false if it was already registered
     * @throws IllegalArgumentException if {@code inner} or {@code encodingId} is null
     * @throws IllegalArgumentException if {@code inner}'s encoding ID conflicts with an existing, but different, encoding
     * @throws IllegalArgumentException if {@code inner}'s encoding ID has one or more array dimensions
     * @throws IllegalArgumentException if {@code inner} already supports null values
     */
    public <T> boolean addNullSafe(EncodingId encodingId, Encoding<T> inner) {
        return this.add(new NullSafeEncoding<>(encodingId, inner));
    }

    /**
     * Build an array encoding for the given element encoding using Permazen's default array encoding.
     *
     * <p>
     * The element encoding must represent a non-primitive type.
     * This method uses the generic array encoding provided by {@link ObjectArrayEncoding}, wrapped via {@link NullSafeEncoding}.
     *
     * <p>
     * If {@code elementEncoding} is anonymous, so will be the returned encoding, otherwise the returned encoding's
     * {@link EncodingId} will be equal to {@code elementEncoding.}{@link EncodingId#getArrayId getArrayId()}.
     *
     * @param elementEncoding element encoding
     * @return corresponding array encoding
     * @throws IllegalArgumentException if {@code elementEncoding} is null
     * @throws IllegalArgumentException if {@code elementEncoding} encodes a primitive type
     * @throws IllegalArgumentException if {@code elementEncoding} encodes an array type with 255 dimensions
     */
    @SuppressWarnings("unchecked")
    public static <E> Encoding<E[]> buildArrayEncoding(final Encoding<E> elementEncoding) {
        Preconditions.checkArgument(elementEncoding != null, "null elementEncoding");
        Preconditions.checkArgument(Primitive.get(elementEncoding.getTypeToken().getRawType()) == null, "primitive element type");
        final EncodingId encodingId = Optional.ofNullable(elementEncoding.getEncodingId()).map(EncodingId::getArrayId).orElse(null);
        return new NullSafeEncoding<>(encodingId, new ObjectArrayEncoding<>(null, elementEncoding));
    }

// EncodingRegistry

    @Override
    public synchronized Encoding<?> getEncoding(EncodingId encodingId) {

        // Sanity check
        Preconditions.checkArgument(encodingId != null, "null encodingId");

        // See if already registered
        Encoding<?> encoding = this.byId.get(encodingId);
        if (encoding != null)
            return encoding;

        // Auto-register array encodings using element encoding
        if (encodingId.getArrayDimensions() > 0) {
            final Encoding<?> elementEncoding = this.getEncoding(encodingId.getElementId());
            if (elementEncoding != null) {
                encoding = SimpleEncodingRegistry.buildArrayEncoding(elementEncoding);
                this.register(encodingId, encoding);
            }
        }

        // Done
        return encoding;
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized <T> List<Encoding<T>> getEncodings(TypeToken<T> typeToken) {

        // Sanity check
        Preconditions.checkArgument(typeToken != null, "null typeToken");

        // See if already registered
        List<Encoding<T>> encodingList = (List<Encoding<T>>)(Object)this.byType.get(typeToken);

        // If not found, and type is an array type, find element type(s) and (as found) auto-register array type(s)
        if (encodingList == null) {
            final TypeToken<?> elementTypeToken = typeToken.getComponentType();
            if (elementTypeToken != null) {
                this.getEncodings(elementTypeToken).stream()
                    .map(Encoding::getEncodingId)
                    .map(EncodingId::getArrayId)
                    .iterator()
                    .forEachRemaining(this::getEncoding);
                encodingList = (List<Encoding<T>>)(Object)this.byType.get(typeToken);
            }
        }

        // Return a copy of the list for safety
        return encodingList != null ? new ArrayList<>(encodingList) : Collections.emptyList();
    }

// Internal Methods

    /**
     * Register a new {@link Encoding}.
     *
     * @param encodingId encoding ID under which to register the new encoding
     * @param encoding the new encoding to register
     * @throws IllegalArgumentException if either parameter is null
     * @throws IllegalArgumentException if there is already a encoding registered under {@code encodingId}
     * @throws IllegalArgumentException if {@code encoding} is anonymous or has an encoding ID different from {@code encodingId}
     */
    protected synchronized void register(EncodingId encodingId, Encoding<?> encoding) {
        Preconditions.checkArgument(encodingId != null, "null encodingId");
        Preconditions.checkArgument(encoding != null, "null encoding");
        Preconditions.checkArgument(encoding.getEncodingId() != null, "encoding is anonymous");
        Preconditions.checkArgument(encoding.getEncodingId().equals(encodingId), "encoding ID mismatch");
        Preconditions.checkArgument(!this.byId.containsKey(encodingId), "encoding ID is already registered");
        this.log.debug("{}: registering encoding \"{}\" for type {}",
          this.getClass().getSimpleName(), encodingId, encoding.getTypeToken());
        this.byId.put(encodingId, encoding);
        this.byType.computeIfAbsent(encoding.getTypeToken(), typeToken -> new ArrayList<>(1)).add(encoding);
    }
}
