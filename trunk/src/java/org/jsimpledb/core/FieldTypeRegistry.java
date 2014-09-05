
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dellroad.stuff.java.Primitive;
import org.dellroad.stuff.java.PrimitiveSwitch;

/**
 * A registry of {@link FieldType}s.
 *
 * <p>
 * All {@link FieldType}s in a {@link FieldTypeRegistry} are registered under a unique type name.
 * However, multiple registered {@link FieldType}s may support the same {@linkplain FieldType#getTypeToken Java type}.
 * </p>
 *
 * <p><b>Arrays</b></p>
 *
 * <p>
 * Array types will be automatically created and registered on demand, assuming the base element
 * type is already registered.
 * </p>
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
 * <p>
 * The following types are automatically registered (or generated on demand):
 * <ul>
 *  <li>Primitive types ({@code boolean}, {@code int}, etc.)</li>
 *  <li>Primitive wrapper types ({@link Boolean}, {@link Integer}, etc.)</li>
 *  <li>{@link String}</li>
 *  <li>Array types</li>
 *  <li>{@link Enum} types</li>
 *  <li>{@link java.util.Date}</li>
 *  <li>{@link java.util.UUID}</li>
 *  <li>{@link java.util.URI}</li>
 *  <li>{@link java.io.File}</li>
 *  <li>{@link java.util.regex.Pattern}</li>
 * </ul>
 * </p>
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
        this.add(FieldType.OBJ_ID);
        this.add(FieldType.STRING);
        this.add(FieldType.DATE);
        this.add(FieldType.ENUM_VALUE);
        this.add(FieldType.UUID);
        this.add(FieldType.URI);
        this.add(FieldType.FILE);
        this.add(FieldType.PATTERN);
    }

    /**
     * Add multiple user-defined {@link FieldType} to this registry, using newly created instances of the named classes.
     *
     * @param classNames names of classes that implement {@link FieldType}
     * @throws IllegalArgumentException if {@code classNames} is null
     * @throws IllegalArgumentException if {@code classNames} contains a null class or a class with invalid annotation(s)
     * @throws IllegalArgumentException if {@code classNames} contains an invalid {@link FieldType} class
     * @throws RuntimeException if instantiation of a class fails
     */
    public void addNamedClasses(Iterable<String> classNames) {
        if (classNames == null)
            throw new IllegalArgumentException("null classNames");
        this.addClasses(Iterables.transform(classNames, new Function<String, Class<? extends FieldType<?>>>() {
            @Override
            @SuppressWarnings("unchecked")
            public Class<? extends FieldType<?>> apply(String name) {
                final Class<?> c;
                try {
                    c = Class.forName(name, false, Thread.currentThread().getContextClassLoader());
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                try {
                    return (Class<? extends FieldType<?>>)(Object)c.asSubclass(FieldType.class);
                } catch (ClassCastException e) {
                    throw new IllegalArgumentException("class `" + name + "' does not implement " + FieldType.class, e);
                }
            }
        }));
    }

    /**
     * Add multiple user-defined {@link FieldType} to this registry, using newly created instances of the specified classes.
     *
     * @param classes implementations of types to add
     * @throws IllegalArgumentException if {@code classes} is null
     * @throws IllegalArgumentException if {@code classes} contains a null class or a class with invalid annotation(s)
     * @throws IllegalArgumentException if {@code classes} contains an invalid {@link FieldType} class
     */
    public void addClasses(Iterable<? extends Class<? extends FieldType<?>>> classes) {
        if (classes == null)
            throw new IllegalArgumentException("null classes");
        for (Class<? extends FieldType<?>> type : classes)
            this.addClass(type);
    }

    /**
     * Add a user-defined {@link FieldType} to this registry, using a newly created instance of the specified class.
     *
     * @param typeClass implementation of type to add
     * @throws IllegalArgumentException if {@code typeClass} is null
     * @throws IllegalArgumentException if {@code typeClass} cannot be instantiated
     * @throws IllegalArgumentException if the {@linkplain FieldType#getName type name} conflicts with an existing type
     * @throws IllegalArgumentException if the {@linkplain FieldType#getName type name} ends with {@code []} (array name)
     */
    public void addClass(Class<? extends FieldType<?>> typeClass) {

        // Sanity check
        if (typeClass == null)
            throw new IllegalArgumentException("null typeClass");

        // Instantiate class
        final FieldType<?> fieldType;
        try {
            fieldType = (FieldType<?>)typeClass.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("can't instantiate " + typeClass, e);
        }

        // Register it
        this.add(fieldType);
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
    @SuppressWarnings("unchecked")
    public <E> FieldType<E[]> getArrayType(final FieldType<E> elementType) {

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
        return (FieldType<E[]>)this.createNullSafeType(notNullType);
    }

    /**
     * Get all {@link FieldType}s in this registry that supports values of the given Java type, which must
     * exactly match the {@link FieldType}'s {@linkplain FieldType#getTypeToken supported Java type}.
     *
     * @param typeToken Java value type
     * @return unmodifiable list of {@link FieldType}s supporting Java values of type {@code typeToken}
     * @throws IllegalArgumentException if {@code typeToken} is null
     */
    @SuppressWarnings("unchecked")
    public synchronized <T> List<FieldType<T>> getFieldTypes(TypeToken<T> typeToken) {

        // Sanity check
        if (typeToken == null)
            throw new IllegalArgumentException("null typeToken");

        // Handle array types
        final TypeToken<?> elementTypeToken = typeToken.getComponentType();
        if (elementTypeToken != null) {
            final ArrayList<FieldType<T>> fieldTypes = new ArrayList<FieldType<T>>();
            for (FieldType<?> elementFieldType : this.getFieldTypes(elementTypeToken))
                fieldTypes.add((FieldType<T>)this.getArrayType(this.getFieldType(elementTypeToken)));
            return Collections.unmodifiableList(fieldTypes);
        }

        // Handle non-array types
        final ArrayList<FieldType<T>> fieldTypes = (ArrayList<FieldType<T>>)(Object)this.typesByType.get(typeToken);
        return fieldTypes != null ? Collections.unmodifiableList(fieldTypes) : Collections.<FieldType<T>>emptyList();
    }

    /**
     * Get the unique {@link FieldType} in this registry that supports values of the given Java type, which must
     * exactly match the {@link FieldType}'s {@linkplain FieldType#getTypeToken supported Java type}.
     * There must be exactly one such {@link FieldType}, otherwise an {@link IllegalArgumentException} is thrown.
     *
     * @param typeToken supported Java value type
     * @return {@link FieldType} supporting Java values of type {@code typeToken}
     * @throws IllegalArgumentException if {@code typeToken} is null
     * @throws IllegalArgumentException if no registered {@link FieldType}s supports {@code typeToken}
     * @throws IllegalArgumentException if more than one registered {@link FieldType} supports {@code typeToken}
     */
    public <T> FieldType<T> getFieldType(TypeToken<T> typeToken) {
        final List<FieldType<T>> fieldTypes = this.getFieldTypes(typeToken);
        switch (fieldTypes.size()) {
        case 0:
            throw new IllegalArgumentException("no registered types support values of type " + typeToken);
        case 1:
            return fieldTypes.get(0);
        default:
            throw new IllegalArgumentException("multiple registered types support values of type " + typeToken + ": " + fieldTypes);
        }
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
}

