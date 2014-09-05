
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeToken;

import java.io.File;
import java.net.URI;
import java.util.Comparator;
import java.util.Date;
import java.util.UUID;
import java.util.regex.Pattern;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseUtil;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * Defines the encoding, ordering, and range of possible values for a {@link SimpleField}.
 *
 * <p>
 * {@link FieldType}s have these requirements and properties:
 * <ul>
 *  <li>They have a unique {@linkplain #getName name}; typically the same as their {@linkplain #getTypeToken supported type}.</li>
 *  <li>All possible values can be represented in Java as an instance of the associated Java type (possibly including null).</li>
 *  <li>Instances {@linkplain #compare totally order} the Java values. If the associated Java type implements {@link Comparable},
 *      then the two orderings do not necessarily have to match, but they should if possible.</li>
 *  <li>All possible values can be encoded/decoded into a self-delimiting binary string (i.e., {@code byte[]} array)
 *      without losing information, and these binary strings, when sorted lexicographically using unsigned comparison,
 *      sort consistently with the {@linkplain #compare total ordering} of the corresponding Java values.</li>
 *  <li>All possible values can be encoded/decoded to/from {@link String}s without losing information,
 *      with both a {@linkplain #toString(Object) regular string form} and a
 *      {@linkplain #toParseableString self-delimiting string form} (these may be the same).</li>
 *  <li>{@code null} may or may not be a supported value; if so, it must be handled by {@link #compare} and
 *      have binary and string encodings just like any other value. Typically, null sorts last.</li>
 *  <li>There is a {@linkplain #getDefaultValue default value}; it must be null for types that support null.</li>
 * </ul>
 * </p>
 *
 * <p>
 * Two {@link FieldType} instances should be equal according to {@link #equals equals()} if only if they behave identically
 * with respect to all of the above.
 * </p>
 *
 * <p>
 * A {@link FieldTypeRegistry} contains a registry of {@link FieldType}s indexed by name.
 * </p>
 *
 * @param <T> The associated Java type
 * @see FieldTypeRegistry
 */
public abstract class FieldType<T> implements Comparator<T> {

    /**
     * The regular expression that {@link FieldType} names must match. This pattern is the same as is required
     * for Java identifiers, except that the following additional characters are allowed after the first character:
     * dot (`.') and dash (`-'), and lastly up to 255 pairs of square brackets (`[]') to indicate an array type.
     */
    public static final String NAME_PATTERN = "\\p{javaJavaIdentifierStart}[-.\\p{javaJavaIdentifierPart}]*(\\[\\]){0,255}";

    /**
     * Type name for reference types.
     */
    public static final String REFERENCE_TYPE_NAME = "reference";

    /**
     * {@code void} primitive wrapper type (null values not allowed).
     */
    static final VoidType VOID = new VoidType();

    /**
     * {@code Void} primitive wrapper type (null values allowed).
     */
    static final PrimitiveWrapperType<Void> VOID_WRAPPER = new PrimitiveWrapperType<>(FieldType.VOID);

    /**
     * {@code boolean} primitive wrapper type (null values not allowed).
     */
    static final BooleanType BOOLEAN = new BooleanType();

    /**
     * {@code Boolean} primitive wrapper type (null values allowed).
     */
    static final PrimitiveWrapperType<Boolean> BOOLEAN_WRAPPER = new PrimitiveWrapperType<>(FieldType.BOOLEAN);

    /**
     * {@code byte} primitive wrapper type (null values not allowed).
     */
    static final ByteType BYTE = new ByteType();

    /**
     * {@code Byte} primitive wrapper type (null values allowed).
     */
    static final PrimitiveWrapperType<Byte> BYTE_WRAPPER = new PrimitiveWrapperType<>(FieldType.BYTE);

    /**
     * {@code char} primitive wrapper type (null values not allowed).
     */
    static final CharacterType CHARACTER = new CharacterType();

    /**
     * {@code Character} primitive wrapper type (null values allowed).
     */
    static final PrimitiveWrapperType<Character> CHARACTER_WRAPPER = new PrimitiveWrapperType<>(FieldType.CHARACTER);

    /**
     * {@code short} primitive wrapper type (null values not allowed).
     */
    static final ShortType SHORT = new ShortType();

    /**
     * {@code Short} primitive wrapper type (null values allowed).
     */
    static final PrimitiveWrapperType<Short> SHORT_WRAPPER = new PrimitiveWrapperType<>(FieldType.SHORT);

    /**
     * {@code int} primitive type (null values not allowed).
     */
    static final IntegerType INTEGER = new IntegerType();

    /**
     * {@code Integer} primitive wrapper type (null values allowed).
     */
    static final PrimitiveWrapperType<Integer> INTEGER_WRAPPER = new PrimitiveWrapperType<>(FieldType.INTEGER);

    /**
     * {@code float} primitive wrapper type (null values not allowed).
     */
    static final FloatType FLOAT = new FloatType();

    /**
     * {@code Float} primitive wrapper type (null values allowed).
     */
    static final PrimitiveWrapperType<Float> FLOAT_WRAPPER = new PrimitiveWrapperType<>(FieldType.FLOAT);

    /**
     * {@code long} primitive wrapper type (null values not allowed).
     */
    static final LongType LONG = new LongType();

    /**
     * {@code Long} primitive wrapper type (null values allowed).
     */
    static final PrimitiveWrapperType<Long> LONG_WRAPPER = new PrimitiveWrapperType<>(FieldType.LONG);

    /**
     * {@code double} primitive wrapper type (null values not allowed).
     */
    static final DoubleType DOUBLE = new DoubleType();

    /**
     * {@code Double} primitive wrapper type (null values allowed).
     */
    static final PrimitiveWrapperType<Double> DOUBLE_WRAPPER = new PrimitiveWrapperType<>(FieldType.DOUBLE);

    /**
     * Type for {@link ObjId}s.
     */
    static final NullSafeType<ObjId> OBJ_ID = new NullSafeType<>(new ObjIdType());

    /**
     * Type for {@link String}s.
     */
    static final NullSafeType<String> STRING = new NullSafeType<>(new StringType());

    /**
     * Type for {@link Date}s.
     */
    static final NullSafeType<Date> DATE = new NullSafeType<>(new DateType());

    /**
     * Type for {@link ListIndexEntry}s.
     */
    static final FieldType<ListIndexEntry> LIST_INDEX_ENTRY = new ListIndexEntryType();

    /**
     * Type for {@link EnumValue}s.
     */
    static final NullSafeType<EnumValue> ENUM_VALUE = new NullSafeType<>(new EnumValueType());

    /**
     * Type for {@link UUID}s.
     */
    static final NullSafeType<UUID> UUID = new NullSafeType<>(new UUIDType());

    /**
     * Type for {@link URI}s.
     */
    static final NullSafeType<URI> URI = new URIType();

    /**
     * Type for {@link File}s.
     */
    static final NullSafeType<File> FILE = new FileType();

    /**
     * Type for {@link Pattern}s.
     */
    static final NullSafeType<Pattern> PATTERN = new PatternType();

    final String name;
    final TypeToken<T> typeToken;

    /**
     * Constructor.
     *
     * @param name the name of this type
     * @param typeToken Java type for the field's values
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     */
    protected FieldType(String name, TypeToken<T> typeToken) {
        if (name == null)
            throw new IllegalArgumentException("null name");
        if (typeToken == null)
            throw new IllegalArgumentException("null typeToken");
        this.name = name;
        this.typeToken = typeToken;
    }

    /**
     * Constructor taking a {@link Class} object.
     * The {@linkplain #getName name} of this instance will be the {@linkplain Class#getName name} of the given class.
     *
     * @param type Java type for the field's values
     * @throws NullPointerException if {@code type} is null
     */
    protected FieldType(Class<T> type) {
        this(type.getName(), TypeToken.of(type));
    }

    /**
     * Get the name of this type. {@link FieldType} names must be unique in the registry.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the Java type corresponding to this type's values.
     */
    public TypeToken<T> getTypeToken() {
        return this.typeToken;
    }

    /**
     * Read a value from the given input.
     *
     * @param input byte input
     * @return field value (possibly null)
     * @throws IllegalArgumentException if invalid input is encountered
     * @throws IndexOutOfBoundsException if input is truncated
     */
    public abstract T read(ByteReader input);

    /**
     * Write a value to the given output.
     *
     * @param writer byte output
     * @param value value to write (possibly null)
     * @throws IllegalArgumentException if {@code value} is null and this type does not support null
     */
    public abstract void write(ByteWriter writer, T value);

    /**
     * Get the default value for this field encoded as a {@code byte[]} array.
     *
     * @return encoded default value
     */
    public abstract byte[] getDefaultValue();

    /**
     * Read and discard a value from the given input.
     *
     * @param reader byte input
     * @throws IllegalArgumentException if invalid input is encountered
     * @throws IndexOutOfBoundsException if input is truncated
     */
    public abstract void skip(ByteReader reader);

    /**
     * Encode a value as a {@link String} for later decoding by {@link #fromString fromString()}.
     *
     * <p>
     * Each of the characters in the returned {@link String} must be one of the valid XML characters
     * (tab, newline, carriage return, <code>&#92;u0020 - &#92;ud7ff</code>, and <code>&#92;ue000 - &#92;fffdf</code>).
     * </p>
     *
     * <p>
     * The implementation in {@link FieldType} delegates to {@link #toParseableString}.
     * Subclasses that override this method should also override {@link #fromString fromString()}.
     * </p>
     *
     * @param value actual value, never null
     * @return string encoding of {@code value}
     * @throws IllegalArgumentException if {@code value} is null
     * @see <a href="http://www.w3.org/TR/REC-xml/#charsets">The XML 1.0 Specification</a>
     */
    public String toString(T value) {
        if (value == null)
            throw new IllegalArgumentException("null value");
        return this.toParseableString(value);
    }

    /**
     * Parse a value previously enoded by {@link #toString(Object)}.
     *
     * <p>
     * The implementation in {@link FieldType} creates a new {@link ParseContext} based on {@code string},
     * delegates to {@link #toParseableString} to parse it, and verifies that all of {@code string} was consumed
     * during the parse. Subclasses that override this method should also override {@link #fromString toString()}.
     * </p>
     *
     * @param string string previously encoded by {@link #toString(Object)}
     * @return actual value
     * @throws IllegalArgumentException if the input is invalid
     */
    public T fromString(String string) {
        final ParseContext ctx = new ParseContext(string);
        final T value = this.fromParseableString(ctx);
        if (!ctx.isEOF()) {
            throw new IllegalArgumentException("found trailing garbage starting with `"
              + ParseUtil.truncate(ctx.getInput(), 20) + "'");
        }
        return value;
    }

    /**
     * Encode a possibly null value as a {@link String} for later decoding by {@link #fromParseableString fromParseableString()}.
     * The string value must be <i>self-delimiting</i>, i.e., decodable even when followed by arbitrary additional characters,
     * and must not start with whitespace or closing square bracket ({@code "]"}).
     *
     * <p>
     * In addition, each of the characters in the returned {@link String} must be one of the valid XML characters
     * (tab, newline, carriage return, <code>&#92;u0020 - &#92;ud7ff</code>, and <code>&#92;ue000 - &#92;fffdf</code>).
     * </p>
     *
     * @param value actual value (possibly null)
     * @return string encoding of {@code value}
     * @throws IllegalArgumentException if {@code value} is null and this type does not support null
     * @see <a href="http://www.w3.org/TR/REC-xml/#charsets">The XML 1.0 Specification</a>
     */
    public abstract String toParseableString(T value);

    /**
     * Parse a value previously encoded by {@link #toParseableString toParseableString()} as a self-delimited {@link String}
     * and positioned at the start of the given parsing context.
     *
     * @param context parse context starting with a string previously encoded via {@link #toParseableString toParseableString()}
     * @return actual value (possibly null)
     * @throws IllegalArgumentException if the input is invalid
     */
    public abstract T fromParseableString(ParseContext context);

    /**
     * Verify the given object is a valid instance of this {@link FieldType}'s Java type and cast it to that type.
     *
     * <p>
     * Note that this method must throw {@link IllegalArgumentException}, not {@link ClassCastException}
     * or {@code NullPointerException}, if {@code obj} does not have the correct type, or is an illegal null value.
     * </p>
     *
     * <p>
     * The implementation in {@link FieldType} simply casts the value using this instance's raw Java type.
     * Subclasses should override this method to implement any other restrictions, e.g., disallowing null values.
     * </p>
     *
     * @param obj object to validate
     * @throws IllegalArgumentException if {@code obj} in not of type T
     * @throws IllegalArgumentException if {@code obj} is null and this type does not support null values
     * @throws IllegalArgumentException if {@code obj} is in any other way not supported by this {@link FieldType}
     */
    @SuppressWarnings("unchecked")
    public T validate(Object obj) {
        try {
            return (T)this.typeToken.getRawType().cast(obj);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(this + " does not support values of type " + obj.getClass().getName());
        }
    }

    /**
     * Compare two values. This method must provide a total ordering of all supported Java values.
     * If null is a supported Java value, then this method must accept it without throwing an exception
     * (note, this is a stronger requirement than {@link Comparator} requires).
     *
     * @throws IllegalArgumentException if {@code value1} or {@code value2} is null and this type does not support null
     */
    @Override
    public abstract int compare(T value1, T value2);

    /**
     * Determine whether any of this field type's encoded values start with a {@code 0x00} byte.
     * Certain optimizations are possible when this is not the case. It is safe for this method to always return true.
     *
     * <p>
     * Note: changing the return value of this method usually means changing the binary encoding.
     * </p>
     *
     * <p>
     * The implementation in {@link FieldType} returns {@code true}.
     * </p>
     */
    public boolean hasPrefix0x00() {
        return true;
    }

    /**
     * Determine whether any of this field type's encoded values start with a {@code 0xff} byte.
     * Certain optimizations are possible when this is not the case. It is safe for this method to always return true.
     *
     * <p>
     * Note: changing the return value of this method usually means changing the binary encoding.
     * </p>
     *
     * <p>
     * The implementation in {@link FieldType} returns {@code true}.
     * </p>
     */
    public boolean hasPrefix0xff() {
        return true;
    }

    /**
     * Returns this instance's {@linkplain #getName name}.
     */
    @Override
    public String toString() {
        return this.name;
    }

    /**
     * Convenience method for generic type binding.
     *
     * @param obj object to validate
     * @throws IllegalArgumentException if {@code obj} in not of type T
     * @throws IllegalArgumentException if {@code obj} is null and this type does not support null values
     * @throws IllegalArgumentException if {@code obj} is in any other way not supported by this {@link FieldType}
     */
    void validateAndWrite(ByteWriter writer, Object obj) {
        this.write(writer, this.validate(obj));
    }

    @Override
    public int hashCode() {
        return this.name.hashCode() ^ this.typeToken.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final FieldType<?> that = (FieldType<?>)obj;
        return this.name.equals(that.name) && this.typeToken.equals(that.typeToken);
    }
}

