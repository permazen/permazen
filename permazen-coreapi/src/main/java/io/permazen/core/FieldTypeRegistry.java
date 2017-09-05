
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.dellroad.stuff.java.Primitive;
import org.dellroad.stuff.java.PrimitiveSwitch;
import io.permazen.core.type.ArrayType;
import io.permazen.core.type.BigDecimalType;
import io.permazen.core.type.BigIntegerType;
import io.permazen.core.type.BitSetType;
import io.permazen.core.type.BooleanArrayType;
import io.permazen.core.type.BooleanType;
import io.permazen.core.type.ByteArrayType;
import io.permazen.core.type.ByteType;
import io.permazen.core.type.CharacterArrayType;
import io.permazen.core.type.CharacterType;
import io.permazen.core.type.DateType;
import io.permazen.core.type.DoubleArrayType;
import io.permazen.core.type.DoubleType;
import io.permazen.core.type.DurationType;
import io.permazen.core.type.FileType;
import io.permazen.core.type.FloatArrayType;
import io.permazen.core.type.FloatType;
import io.permazen.core.type.Inet4AddressType;
import io.permazen.core.type.Inet6AddressType;
import io.permazen.core.type.InetAddressType;
import io.permazen.core.type.InstantType;
import io.permazen.core.type.IntegerArrayType;
import io.permazen.core.type.IntegerType;
import io.permazen.core.type.InternetAddressType;
import io.permazen.core.type.LocalDateTimeType;
import io.permazen.core.type.LocalDateType;
import io.permazen.core.type.LocalTimeType;
import io.permazen.core.type.LongArrayType;
import io.permazen.core.type.LongType;
import io.permazen.core.type.MonthDayType;
import io.permazen.core.type.NullSafeType;
import io.permazen.core.type.ObjIdType;
import io.permazen.core.type.ObjectArrayType;
import io.permazen.core.type.OffsetDateTimeType;
import io.permazen.core.type.OffsetTimeType;
import io.permazen.core.type.PatternType;
import io.permazen.core.type.PeriodType;
import io.permazen.core.type.PrimitiveWrapperType;
import io.permazen.core.type.ReferenceFieldType;
import io.permazen.core.type.ShortArrayType;
import io.permazen.core.type.ShortType;
import io.permazen.core.type.StringType;
import io.permazen.core.type.URIType;
import io.permazen.core.type.UUIDType;
import io.permazen.core.type.UnsignedIntType;
import io.permazen.core.type.VoidType;
import io.permazen.core.type.YearMonthType;
import io.permazen.core.type.YearType;
import io.permazen.core.type.ZoneIdType;
import io.permazen.core.type.ZoneOffsetType;
import io.permazen.core.type.ZonedDateTimeType;

/**
 * A registry of {@link FieldType}s.
 *
 * <p>
 * All {@link FieldType}s in a {@link FieldTypeRegistry} are registered under a unique type name.
 * However, multiple registered {@link FieldType}s may support the same {@linkplain FieldType#getTypeToken Java type}.
 *
 * <p><b>Arrays</b></p>
 *
 * <p>
 * Array types will be automatically created and registered on demand, assuming the base element
 * type is already registered.
 *
 * <p>
 * Arrays are passed by value: i.e., the entire array is copied. Therefore, changes to array elements after
 * getting or setting a field have no effect on the field's value.
 *
 * <p>
 * Arrays of references are not currently supported.
 *
 * <p><b>Built-in Types</b></p>
 *
 * <p>
 * The following types are automatically registered (or generated on demand):
 * <ul>
 *  <li>Primitive types ({@code boolean}, {@code int}, etc.)</li>
 *  <li>Primitive wrapper types ({@link Boolean}, {@link Integer}, etc.)</li>
 *  <li>{@link String}</li>
 *  <li>Array types</li>
 *  <li>{@link Enum} types</li>
 *  <li>{@link java.math.BigDecimal}</li>
 *  <li>{@link java.math.BigInteger}</li>
 *  <li>{@link java.util.BitSet}</li>
 *  <li>{@link java.util.Date}</li>
 *  <li>{@link java.util.UUID}</li>
 *  <li>{@link java.net.URI}</li>
 *  <li>{@link java.io.File}</li>
 *  <li>{@link java.net.InetAddress}</li>
 *  <li>{@link java.net.Inet4Address}</li>
 *  <li>{@link java.net.Inet6Address}</li>
 *  <li>{@link java.util.regex.Pattern}</li>
 *  <li>{@link javax.mail.internet.InternetAddress}</li>
 *  <li>{@link java.time java.time.*}</li>
 * </ul>
 */
public class FieldTypeRegistry {

    /**
     * {@code void} primitive type.
     *
     * <p>
     * Completely useless, except perhaps as an invalid sentinel value.
     */
    public static final VoidType VOID = new VoidType();

    /**
     * {@code Void} primitive wrapper type.
     */
    public static final PrimitiveWrapperType<Void> VOID_WRAPPER = new PrimitiveWrapperType<>(new VoidType());

    /**
     * {@code boolean} primitive type (null values not allowed).
     */
    public static final BooleanType BOOLEAN = new BooleanType();

    /**
     * {@code Boolean} primitive wrapper type (null values allowed).
     */
    public static final PrimitiveWrapperType<Boolean> BOOLEAN_WRAPPER = new PrimitiveWrapperType<>(FieldTypeRegistry.BOOLEAN);

    /**
     * {@code byte} primitive type (null values not allowed).
     */
    public static final ByteType BYTE = new ByteType();

    /**
     * {@code Byte} primitive wrapper type (null values allowed).
     */
    public static final PrimitiveWrapperType<Byte> BYTE_WRAPPER = new PrimitiveWrapperType<>(FieldTypeRegistry.BYTE);

    /**
     * {@code char} primitive type (null values not allowed).
     */
    public static final CharacterType CHARACTER = new CharacterType();

    /**
     * {@code Character} primitive wrapper type (null values allowed).
     */
    public static final PrimitiveWrapperType<Character> CHARACTER_WRAPPER = new PrimitiveWrapperType<>(FieldTypeRegistry.CHARACTER);

    /**
     * {@code short} primitive type (null values not allowed).
     */
    public static final ShortType SHORT = new ShortType();

    /**
     * {@code Short} primitive wrapper type (null values allowed).
     */
    public static final PrimitiveWrapperType<Short> SHORT_WRAPPER = new PrimitiveWrapperType<>(FieldTypeRegistry.SHORT);

    /**
     * {@code int} primitive type (null values not allowed).
     */
    public static final IntegerType INTEGER = new IntegerType();

    /**
     * {@code Integer} primitive wrapper type (null values allowed).
     */
    public static final PrimitiveWrapperType<Integer> INTEGER_WRAPPER = new PrimitiveWrapperType<>(FieldTypeRegistry.INTEGER);

    /**
     * {@code float} primitive type (null values not allowed).
     */
    public static final FloatType FLOAT = new FloatType();

    /**
     * {@code Float} primitive wrapper type (null values allowed).
     */
    public static final PrimitiveWrapperType<Float> FLOAT_WRAPPER = new PrimitiveWrapperType<>(FieldTypeRegistry.FLOAT);

    /**
     * {@code long} primitive type (null values not allowed).
     */
    public static final LongType LONG = new LongType();

    /**
     * {@code Long} primitive wrapper type (null values allowed).
     */
    public static final PrimitiveWrapperType<Long> LONG_WRAPPER = new PrimitiveWrapperType<>(FieldTypeRegistry.LONG);

    /**
     * {@code double} primitive type (null values not allowed).
     */
    public static final DoubleType DOUBLE = new DoubleType();

    /**
     * {@code Double} primitive wrapper type (null values allowed).
     */
    public static final PrimitiveWrapperType<Double> DOUBLE_WRAPPER = new PrimitiveWrapperType<>(FieldTypeRegistry.DOUBLE);

    /**
     * Type for {@link ObjId}s (null values are not allowed).
     *
     * @see #REFERENCE
     */
    public static final ObjIdType OBJ_ID = new ObjIdType();

    /**
     * Type for unsigned integers encoded via {@link io.permazen.util.UnsignedIntEncoder}. Used internally.
     */
    public static final UnsignedIntType UNSIGNED_INT = new UnsignedIntType();

    /**
     * Type for object references with no restriction (null values are allowed).
     *
     * @see #OBJ_ID
     */
    public static final ReferenceFieldType REFERENCE = new ReferenceFieldType();

    /**
     * Type for {@link String}s.
     */
    public static final NullSafeType<String> STRING = new NullSafeType<>(new StringType());

    private final HashMap<Key, FieldType<?>> types = new HashMap<>();
    private final HashMap<TypeToken<?>, ArrayList<FieldType<?>>> typesByType = new HashMap<>();

    /**
     * Constructor. Creates an instance with all of the pre-defined {@link FieldType}s (e.g., for
     * types such as primitive types, {@link String}, etc.) already registered.
     */
    public FieldTypeRegistry() {

        // Public singleton types
        this.add(FieldTypeRegistry.REFERENCE);
        this.add(FieldTypeRegistry.VOID_WRAPPER);
        this.add(FieldTypeRegistry.BOOLEAN);
        this.add(FieldTypeRegistry.BOOLEAN_WRAPPER);
        this.add(FieldTypeRegistry.BYTE);
        this.add(FieldTypeRegistry.BYTE_WRAPPER);
        this.add(FieldTypeRegistry.SHORT);
        this.add(FieldTypeRegistry.SHORT_WRAPPER);
        this.add(FieldTypeRegistry.CHARACTER);
        this.add(FieldTypeRegistry.CHARACTER_WRAPPER);
        this.add(FieldTypeRegistry.INTEGER);
        this.add(FieldTypeRegistry.INTEGER_WRAPPER);
        this.add(FieldTypeRegistry.FLOAT);
        this.add(FieldTypeRegistry.FLOAT_WRAPPER);
        this.add(FieldTypeRegistry.LONG);
        this.add(FieldTypeRegistry.LONG_WRAPPER);
        this.add(FieldTypeRegistry.DOUBLE);
        this.add(FieldTypeRegistry.DOUBLE_WRAPPER);
        this.add(FieldTypeRegistry.STRING);

        // Other types
        this.add(new FileType());
        this.add(new NullSafeType<>(new BigDecimalType()));
        this.add(new NullSafeType<>(new BigIntegerType()));
        this.add(new NullSafeType<>(new BitSetType()));
        this.add(new NullSafeType<>(new DateType()));
        this.add(new NullSafeType<>(new DurationType()));
        this.add(new NullSafeType<>(new Inet4AddressType()));
        this.add(new NullSafeType<>(new Inet6AddressType()));
        this.add(new NullSafeType<>(new InetAddressType()));
        this.add(new NullSafeType<>(new InstantType()));
        this.add(new NullSafeType<>(new LocalDateTimeType()));
        this.add(new NullSafeType<>(new LocalDateType()));
        this.add(new NullSafeType<>(new LocalTimeType()));
        this.add(new NullSafeType<>(new MonthDayType()));
        this.add(new NullSafeType<>(new OffsetDateTimeType()));
        this.add(new NullSafeType<>(new OffsetTimeType()));
        this.add(new NullSafeType<>(new PeriodType()));
        this.add(new NullSafeType<>(new UUIDType()));
        this.add(new NullSafeType<>(new YearMonthType()));
        this.add(new NullSafeType<>(new YearType()));
        this.add(new NullSafeType<>(new ZoneOffsetType()));
        this.add(new NullSafeType<>(new ZonedDateTimeType()));
        this.add(new PatternType());
        this.add(new URIType());
        this.add(new ZoneIdType());

        // Types that require optional classpath components
        try {
            this.add(new InternetAddressType());
        } catch (NoClassDefFoundError e) {
            // ignore
        }
    }

    /**
     * Add multiple user-defined {@link FieldType} to this registry, using newly created instances of the named classes.
     *
     * @param classNames names of classes that implement {@link FieldType}
     * @throws IllegalArgumentException if {@code classNames} is null
     * @throws IllegalArgumentException if {@code classNames} contains an invalid {@link FieldType} class
     * @throws RuntimeException if instantiation of a class fails
     */
    @SuppressWarnings("unchecked")
    public void addNamedClasses(Iterable<String> classNames) {
        Preconditions.checkArgument(classNames != null, "null classNames");
        this.addClasses(StreamSupport.stream(classNames.spliterator(), false)
          .map(className -> {
            try {
                return Class.forName(className, false, Thread.currentThread().getContextClassLoader());
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
          })
          .map(cl -> {
            try {
                return (Class<? extends FieldType<?>>)(Object)cl.asSubclass(FieldType.class);
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(cl + " does not implement " + FieldType.class, e);
            }
          })
          .collect(Collectors.toList()));
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
        Preconditions.checkArgument(classes != null, "null classes");
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
        Preconditions.checkArgument(typeClass != null, "null typeClass");

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
     * @return true if type was added, false if type was already registered
     * @throws IllegalArgumentException if {@code type} is null
     * @throws IllegalArgumentException if {@code type} has a name that conflicts with an existing, but different, type
     * @throws IllegalArgumentException if {@code type} has a name that ends with {@code []} (array name)
     */
    public synchronized boolean add(FieldType<?> type) {
        Preconditions.checkArgument(type != null, "null type");
        Preconditions.checkArgument(!type.name.endsWith(ArrayType.ARRAY_SUFFIX), "illegal array type name `" + type.name + "'");
        final Key key = type.toKey();
        final FieldType<?> other = this.types.get(key);
        if (other != null) {
            if (other.equals(type))
                return false;
            throw new IllegalArgumentException("type name `" + type.name + "'"
              + (type.signature != 0 ? " and encoding signature " + type.signature : "")
              +  " conflicts with existing type " + other);
        }
        this.types.put(key, type);
        this.typesByType.computeIfAbsent(type.getTypeToken(), typeToken -> new ArrayList<>(1)).add(type);
        return true;
    }

    /**
     * Get the {@link FieldType} with the given name in this registry.
     *
     * <p>
     * Convenience method, equivalent to:
     * <blockquote><code>
     *  getFieldType(name, 0L)
     * </code></blockquote>
     *
     * @param name type name
     * @return type with name {@code name}, or null if not found
     * @throws IllegalArgumentException if {@code name} is null
     */
    public FieldType<?> getFieldType(final String name) {
        return this.getFieldType(name, 0);
    }

    /**
     * Get the {@link FieldType} with the given name and encoding signature in this registry.
     *
     * @param name type name
     * @param signature type {@linkplain FieldType#getEncodingSignature encoding signature}
     * @return type with name {@code name}, or null if not found
     * @throws IllegalArgumentException if {@code name} is null
     */
    public FieldType<?> getFieldType(final String name, long signature) {

        // Sanity check
        Preconditions.checkArgument(name != null, "null name");

        // Handle array types
        if (name.endsWith(ArrayType.ARRAY_SUFFIX)) {
            final String elementName = name.substring(0, name.length() - ArrayType.ARRAY_SUFFIX.length());
            final FieldType<?> elementType = this.getFieldType(elementName, signature);
            if (elementType == null)
                return null;
            return this.getArrayType(elementType);
        }

        // Handle non-array type
        synchronized (this) {
            return this.types.get(new Key(name, signature));
        }
    }

    /**
     * Get the array {@link FieldType} with the given element type.
     *
     * @param elementType array element type
     * @param <E> array element type
     * @throws IllegalArgumentException if {@code elementType} is null
     * @throws IllegalArgumentException if the resulting array type has too many dimensions
     * @return array type with element type {@code elementType}
     */
    @SuppressWarnings("unchecked")
    public <E> FieldType<E[]> getArrayType(final FieldType<E> elementType) {

        // Sanity check
        Preconditions.checkArgument(elementType != null, "null elementType");

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
     * @param typeToken field value type
     * @param <T> field value type
     * @return unmodifiable list of {@link FieldType}s supporting Java values of type {@code typeToken}
     * @throws IllegalArgumentException if {@code typeToken} is null
     */
    @SuppressWarnings("unchecked")
    public synchronized <T> List<FieldType<T>> getFieldTypes(TypeToken<T> typeToken) {

        // Sanity check
        Preconditions.checkArgument(typeToken != null, "null typeToken");

        // Handle array types
        final TypeToken<?> elementTypeToken = typeToken.getComponentType();
        if (elementTypeToken != null) {
            final ArrayList<FieldType<T>> fieldTypes = this.getFieldTypes(elementTypeToken).stream()
              .map(elementFieldType -> (FieldType<T>)this.getArrayType(this.getFieldType(elementTypeToken)))
              .collect(Collectors.toCollection(ArrayList::new));
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
     * @param typeToken field value type
     * @param <T> field value type
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
     * @return all registered types
     */
    public synchronized List<FieldType<?>> getAll() {
        return new ArrayList<>(this.types.values());
    }

    // This method exists solely to bind the generic type parameters
    private <E> ObjectArrayType<E> createObjectArrayType(FieldType<E> elementType) {
        return new ObjectArrayType<>(elementType);
    }

    // This method exists solely to bind the generic type parameters
    private <T> NullSafeType<T> createNullSafeType(FieldType<T> notNullType) {
        return new NullSafeType<>(notNullType);
    }

// Key

    static class Key {

        private final String name;
        private final long signature;

        Key(String name, long signature) {
            this.name = name;
            this.signature = signature;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final Key that = (Key)obj;
            return this.name.equals(that.name) && this.signature == that.signature;
        }

        @Override
        public int hashCode() {
            return this.name.hashCode() ^ Long.hashCode(this.signature);
        }
    }
}

