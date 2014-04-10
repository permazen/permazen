
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.dellroad.stuff.java.Primitive;
import org.dellroad.stuff.java.PrimitiveSwitch;

/**
 * A registry of {@link FieldType}s.
 */
public class FieldTypeRegistry {

    private final HashMap<String, FieldType<?>> typesByName = new HashMap<>();
    private final HashMap<TypeToken<?>, ArrayList<FieldType<?>>> typesByType = new HashMap<>();

    /**
     * Constructor. Creates an instance with all of the pre-defined {@link FieldType}s (e.g., for
     * types such as primitive types, {@link String}, etc.) already registered.
     */
    public FieldTypeRegistry() {
        this.add(FieldType.VOID_WRAPPER);
        this.add(FieldType.BOOLEAN);
        this.add(FieldType.BOOLEAN_WRAPPER);
        this.add(FieldType.BYTE);
        this.add(FieldType.BYTE_WRAPPER);
        this.add(FieldType.SHORT);
        this.add(FieldType.SHORT_WRAPPER);
        this.add(FieldType.CHARACTER);
        this.add(FieldType.CHARACTER_WRAPPER);
        this.add(FieldType.INTEGER);
        this.add(FieldType.INTEGER_WRAPPER);
        this.add(FieldType.FLOAT);
        this.add(FieldType.FLOAT_WRAPPER);
        this.add(FieldType.LONG);
        this.add(FieldType.LONG_WRAPPER);
        this.add(FieldType.DOUBLE);
        this.add(FieldType.DOUBLE_WRAPPER);
        this.add(FieldType.STRING);
        this.add(FieldType.DATE);
    }

    /**
     * Add a user-defined {@link FieldType} to the registry.
     *
     * @param type type to add
     * @throws IllegalArgumentException if {@code type} is null
     * @throws IllegalArgumentException if {@code type} has a name that conflicts with an existing type
     * @throws IllegalArgumentException if {@code type} has a name that ends with {@code []} (array name)
     */
    public synchronized void add(FieldType<?> type) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        if (type.name.endsWith(ArrayType.ARRAY_SUFFIX))
            throw new IllegalArgumentException("illegal array type name `" + type.name + "'");
        final FieldType<?> other = this.typesByName.get(type.name);
        if (other != null)
            throw new IllegalArgumentException("type name `" + type.name + "' conflicts with existing type " + other);
        this.typesByName.put(type.name, type);
        final TypeToken<?> typeToken = type.getTypeToken();
        if (!this.typesByType.containsKey(typeToken))
            this.typesByType.put(typeToken, Lists.newArrayList(Collections.<FieldType<?>>singleton(type)));
        else
            this.typesByType.get(typeToken).add(type);
    }

    /**
     * Get the {@link FieldType} with the given name in this registry.
     *
     * @param name type name
     * @return type with name {@code name}, or null if not found
     * @throws IllegalArgumentException if {@code name} is null
     */
    public synchronized FieldType<?> getFieldType(final String name) {

        // Sanity check
        if (name == null)
            throw new IllegalArgumentException("null name");

        // Handle array types
        if (name.endsWith(ArrayType.ARRAY_SUFFIX)) {
            final FieldType<?> elementType = this.getFieldType(name.substring(0, name.length() - ArrayType.ARRAY_SUFFIX.length()));
            return this.getArrayType(elementType);
        }

        // Handle non-array type
        return this.typesByName.get(name);
    }

    /**
     * Get the array {@link FieldType} with the given element type.
     *
     * @param elementType array element type
     * @throws IllegalArgumentException if {@code elementType} is null
     * @throws IllegalArgumentException if the resulting array type has too many dimensions
     * @return array type with element type {@code elementType}
     */
    public FieldType<?> getArrayType(final FieldType<?> elementType) {

        // Sanity check
        if (elementType == null)
            throw new IllegalArgumentException("null elementType");

        // Create array type
        final Primitive<?> primitive = Primitive.get(elementType.typeToken.getRawType());
        final ArrayType<?, ?> notNullType = primitive == null ? this.createObjectArrayType(elementType) :
          primitive.visit(new PrimitiveSwitch<ArrayType<?, ?>>() {
            @Override
            public ArrayType<?, ?> caseVoid() {
                throw new IllegalArgumentException("cannot create array of type `" + elementType.name + "'");
            }
            @Override
            public ArrayType<?, ?> caseBoolean() {
                return new BooleanArrayType();
            }
            @Override
            public ArrayType<?, ?> caseByte() {
                return new ByteArrayType();
            }
            @Override
            public ArrayType<?, ?> caseCharacter() {
                return new CharacterArrayType();
            }
            @Override
            public ArrayType<?, ?> caseShort() {
                return new ShortArrayType();
            }
            @Override
            public ArrayType<?, ?> caseInteger() {
                return new IntegerArrayType();
            }
            @Override
            public ArrayType<?, ?> caseFloat() {
                return new FloatArrayType();
            }
            @Override
            public ArrayType<?, ?> caseLong() {
                return new LongArrayType();
            }
            @Override
            public ArrayType<?, ?> caseDouble() {
                return new DoubleArrayType();
            }
        });
        return this.createNullSafeType(notNullType);
    }

    /**
     * Get the (unique) {@link FieldType} in this registry that supports values of the given Java type, which must
     * exactly match the {@link FieldType}'s {@linkplain FieldType#getTypeToken supported Java type}.
     * If there is no such {@link FieldType}, null is returned; if there is more than one such {@link FieldType},
     * an {@link IllegalArgumentException} is thrown.
     *
     * <p>
     * There are two special cases:
     * <ul>
     *  <li>Array types will be automatically created and registered on demand, assuming the base element
     *      type is already registered.</li>
     *  <li>If {@code typeToken} refers to an {@link Enum} class, and no {@link FieldType} is registered for that class,
     *      an appropriate {@link FieldType} is automatically created and registered on demand. See {@link EnumType} for
     *      important information about this type.</li>
     * </ul>
     * </p>
     *
     * <b>Arrays</b>
     *
     * <p>
     * Arrays are passed by value: i.e., the entire array is copied. Therefore, changes to array elements after
     * getting or setting a field have no effect on the field's value.
     * </p>
     *
     * <p>
     * Arrays of references are not currently supported.
     * </p>
     *
     * <b>Enums</b>
     *
     * <p>
     * Enums present a problem because they have both a {@link Enum#name name()} and an {@link Enum#ordinal ordinal()} value,
     * and either or both of these might change over time, perhaps due to code refactoring. Therefore,
     * it's possible to encounter an unknown value when decoding an enum constant. See {@link EnumType}
     * for details on how this situation is handled.
     * </p>
     *
     * @param typeToken supported Java value type
     * @return {@link FieldType} supporting Java values of type {@code typeToken}, or null if no such {@link FieldType} exists
     * @throws IllegalArgumentException if {@code typeToken} is null
     * @throws IllegalArgumentException if more than one {@link FieldType} supports {@code typeToken}
     */
    public synchronized FieldType<?> getFieldType(TypeToken<?> typeToken) {

        // Sanity check
        if (typeToken == null)
            throw new IllegalArgumentException("null typeToken");

        // Handle array types
        final TypeToken<?> elementTypeToken = typeToken.getComponentType();
        if (elementTypeToken != null)
            return this.getArrayType(this.getFieldType(elementTypeToken));

        // Handle non-array types
        final ArrayList<FieldType<?>> types = this.typesByType.get(typeToken);
        if (types != null) {
            switch (types.size()) {
            case 0:
                break;
            case 1:
                return types.get(0);
            default:
                throw new IllegalArgumentException("multiple registered types support values of type " + typeToken + ": " + types);
            }
        }

        // Auto-generate enum types on demand
        final Class<?> rawType = typeToken.getRawType();
        if (Enum.class.isAssignableFrom(rawType)) {
            final EnumType<?> fieldType = this.createEnumType(rawType);
            this.add(fieldType);
            return fieldType;
        }

        // None found
        return null;
    }

    /**
     * Get all types registered with this instance.
     *
     * @return mapping from type name to type
     */
    public synchronized Map<String, FieldType<?>> getAll() {
        return new HashMap<String, FieldType<?>>(this.typesByName);
    }

    // This method exists solely to bind the generic type parameters
    private <E> ObjectArrayType<E> createObjectArrayType(FieldType<E> elementType) {
        return new ObjectArrayType<E>(elementType);
    }

    // This method exists solely to bind the generic type parameters
    private <T> NullSafeType<T> createNullSafeType(FieldType<T> notNullType) {
        return new NullSafeType<T>(notNullType);
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private EnumType<?> createEnumType(Class<?> type) {
        return new EnumType(type.asSubclass(Enum.class));
    }
}

