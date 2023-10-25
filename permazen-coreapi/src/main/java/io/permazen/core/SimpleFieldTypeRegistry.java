
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.core.type.NullSafeType;
import io.permazen.core.type.ObjectArrayType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.dellroad.stuff.java.Primitive;

/**
 * A straightforward {@link FieldTypeRegistry} implementation.
 *
 * <p>
 * The {@link #add add()} method only accepts non-array types and primitive array types. Other array types
 * are automatically created on demand via {@link #buildArrayType buildArrayType()}.
 */
public class SimpleFieldTypeRegistry implements FieldTypeRegistry {

    final HashMap<EncodingId, FieldType<?>> typesById = new HashMap<>();
    final HashMap<TypeToken<?>, List<FieldType<?>>> typesByType = new HashMap<>();

// Public Methods

    /**
     * Add a non-array {@link FieldType} to the registry.
     *
     * <p>
     * The type's encoding ID must not contain any array dimensions.
     *
     * @param fieldType the {@link FieldType} to registre
     * @return true if it was added, false if it was already registered
     * @throws IllegalArgumentException if {@code fieldType} is null
     * @throws IllegalArgumentException if {@code fieldType}'s encoding ID is null (i.e., {@code fieldType} is anonymous)
     * @throws IllegalArgumentException if {@code fieldType}'s encoding ID conflicts with an existing, but different, type
     * @throws IllegalArgumentException if {@code fieldType}'s encoding ID has one or more array dimensions
     */
    public synchronized boolean add(FieldType<?> fieldType) {
        Preconditions.checkArgument(fieldType != null, "null fieldType");
        final EncodingId encodingId = fieldType.getEncodingId();
        Preconditions.checkArgument(encodingId != null, "fieldType is anonymous");
        final TypeToken<?> elementType = fieldType.getTypeToken().getComponentType();
        Preconditions.checkArgument((elementType == null) == (encodingId.getArrayDimensions() == 0),
          "inconsistent encoding ID \"" + encodingId + "\" for type " + fieldType.getTypeToken());
        Preconditions.checkArgument(elementType == null || elementType.getRawType().isPrimitive(),
          "illegal array type \"" + encodingId + "\"");
        final FieldType<?> otherType = this.typesById.get(encodingId);
        if (otherType != null) {
            if (!otherType.equals(fieldType)) {
                throw new IllegalArgumentException(
                  String.format("encoding ID \"%s\" for field type %s conflicts with existing field type %s",
                  encodingId, fieldType, otherType));
            }
            return false;
        }
        this.register(encodingId, fieldType);
        return true;
    }

    /**
     * Build an array field type based on the given element field type.
     *
     * <p>
     * The element field type must represent a non-primitive type.
     * This uses the generic array encoding provided by {@link ObjectArrayType} wrapped via {@link NullSafeType}.
     *
     * @param elementType element field type
     * @throws IllegalArgumentException if {@code elementType} is null
     * @throws IllegalArgumentException if {@code elementType} represents a primitive type
     * @throws IllegalArgumentException if {@code elementType} represents an array type with 255 dimensions
     */
    @SuppressWarnings("unchecked")
    public static <E> FieldType<E[]> buildArrayType(final FieldType<E> elementType) {
        Preconditions.checkArgument(elementType != null, "null elementType");
        Preconditions.checkArgument(Primitive.get(elementType.getTypeToken().getRawType()) == null, "primitive elementType");
        return new NullSafeType<>(new ObjectArrayType<>(elementType));
    }

// FieldTypeRegistry

    @Override
    public synchronized FieldType<?> getFieldType(EncodingId encodingId) {

        // Sanity check
        Preconditions.checkArgument(encodingId != null, "null encodingId");

        // See if already registered
        FieldType<?> fieldType = this.typesById.get(encodingId);
        if (fieldType != null)
            return fieldType;

        // Auto-register array types using element type
        if (encodingId.getArrayDimensions() > 0) {
            final FieldType<?> elementType = this.getFieldType(encodingId.getElementId());
            if (elementType != null) {
                fieldType = SimpleFieldTypeRegistry.buildArrayType(elementType);
                this.register(encodingId, fieldType);
            }
        }

        // Done
        return fieldType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized <T> List<FieldType<T>> getFieldTypes(TypeToken<T> typeToken) {

        // Sanity check
        Preconditions.checkArgument(typeToken != null, "null typeToken");

        // See if already registered
        List<FieldType<T>> fieldTypeList = (List<FieldType<T>>)(Object)this.typesByType.get(typeToken);
        if (fieldTypeList != null)
            return fieldTypeList;

        // If not found and is an array type, Auto-register using element type and try again
        final TypeToken<?> elementTypeToken = typeToken.getComponentType();
        if (elementTypeToken != null) {
            this.getFieldTypes(elementTypeToken).stream()
              .map(FieldType::getEncodingId)
              .map(EncodingId::getArrayId)
              .map(this::getFieldType);
            fieldTypeList = (List<FieldType<T>>)(Object)this.typesByType.get(typeToken);
        }

        // Return a copy of the list for safety
        return fieldTypeList != null ? new ArrayList<>(fieldTypeList) : Collections.emptyList();
    }

// Internal Methods

    private void register(EncodingId encodingId, FieldType<?> fieldType) {
        assert Thread.holdsLock(this);
        this.typesById.put(encodingId, fieldType);
        this.typesByType.computeIfAbsent(fieldType.getTypeToken(), typeToken -> new ArrayList<>(1)).add(fieldType);
    }
}
