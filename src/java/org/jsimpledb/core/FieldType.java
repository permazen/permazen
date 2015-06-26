
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.util.Comparator;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseUtil;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * Defines the encoding, ordering, and range of possible values for a {@link SimpleField}.
 *
 * <p>
 * A {@link FieldType} maps between instances of its supported Java type and the self-delimited {@code byte[]} encoding of
 * those instances used in the database. The {@code byte[]} encoding also implicitly defines the database sort order
 * (via unsigned lexicographical ordering). This ordering is also reflected via {@link #compare compare()}.
 * </p>
 *
 * <p>
 * A {@link FieldType} also defines a mapping between Java instances and {@link String} values.
 * There are two separate {@link String} forms, a regular form and a self-delimiting form.
 * </p>
 *
 * <p>
 * {@link FieldType}s have these requirements and properties:
 * <ul>
 *  <li>They have a unique {@linkplain #getName name}; typically the same as their {@linkplain #getTypeToken supported type}.</li>
 *  <li>All possible values can be represented in Java as an instance of the associated Java type (possibly including null).</li>
 *  <li>Instances {@linkplain #compare totally order} their Java values. If the associated Java type implements {@link Comparable},
 *      then the two orderings do not necessarily have to match, but they should if possible.</li>
 *  <li>All possible values can be encoded/decoded into a self-delimiting binary string (i.e., {@code byte[]} array)
 *      without losing information, and these binary strings, when sorted lexicographically using unsigned comparison,
 *      sort consistently with the total ordering of the corresponding Java values defined by {@link #compare compare()}.</li>
 *  <li>All possible values can be encoded/decoded to/from {@link String}s without losing information,
 *      with both a {@linkplain #toString(Object) regular string form} for non-null values and a
 *      {@linkplain #toParseableString self-delimiting string form} for any value (these may be the same).</li>
 *  <li>{@code null} may or may not be a supported value; if so, it must be handled by {@link #compare compare()} (typically
 *      null values sort last) and have binary and string encodings just like any other value.</li>
 *  <li>There is a {@linkplain #getDefaultValue default value}. For types that support null, the default value must be null.</li>
 *  <li>An optional {@linkplain #getEncodingSignature encoding signature} protects against incompatible encodings
 *      when a {@link FieldType}'s binary or string encoding changes without changing the {@linkplain #getName name}.</li>
 * </ul>
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

    final String name;
    final TypeToken<T> typeToken;
    final long signature;

// Constructors

    /**
     * Constructor.
     *
     * @param name the name of this type
     * @param typeToken Java type for the field's values
     * @param signature binary encoding signature
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     */
    protected FieldType(String name, TypeToken<T> typeToken, long signature) {
        Preconditions.checkArgument(name != null, "null name");
        Preconditions.checkArgument(name.matches(FieldType.NAME_PATTERN), "invalid type name `" + name + "'");
        Preconditions.checkArgument(typeToken != null, "null typeToken");
        this.name = name;
        this.typeToken = typeToken;
        this.signature = signature;
    }

    /**
     * Constructor taking a {@link Class} object.
     * The {@linkplain #getName name} of this instance will be the {@linkplain Class#getName name} of the given class.
     *
     * @param type Java type for the field's values
     * @param signature binary encoding signature
     * @throws NullPointerException if {@code type} is null
     */
    protected FieldType(Class<T> type, long signature) {
        this(type.getName(), TypeToken.of(type), signature);
    }

// Public methods

    /**
     * Get the name of this type. {@link FieldType} names must be unique in the registry.
     *
     * @return this type's name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the Java type corresponding to this type's values.
     *
     * @return this type's Java type
     */
    public TypeToken<T> getTypeToken() {
        return this.typeToken;
    }

    /**
     * Get the binary encoding signature of this type.
     *
     * <p>
     * The binary encoding signature is analogous to the {@code serialVersionUID} used by Java serialization.
     * It represents a specific binary and/or {@link String} encoding for Java values. In the case that a {@link FieldType}
     * implementation changes its binary encoding, but not it's name, it <b>must</b> use a new, different binary encoding
     * signature to eliminate the possibility of mixing incompatible encodings in software vs. persistent storage.
     * Typically a value of zero is used until if/when such a change occurs.
     * </p>
     *
     * <p>
     * Note that another option when encodings change is simply to change the {@linkplain #getName name} of the type
     * (encoding signatures are scoped to a single {@link FieldType} name).
     * </p>
     *
     * @return binary encoding signature
     */
    public long getEncodingSignature() {
        return this.signature;
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
     * Get the default value for this field type encoded as a {@code byte[]} array.
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
     * Encode a non-null value as a {@link String} for later decoding by {@link #fromString fromString()}.
     *
     * <p>
     * Each of the characters in the returned {@link String} must be one of the valid XML characters
     * (tab, newline, carriage return, <code>&#92;u0020 - &#92;ud7ff</code>, and <code>&#92;ue000 - &#92;fffdf</code>).
     * </p>
     *
     * <p>
     * The implementation in {@link FieldType} checks that {@code value} is not null, then delegates to {@link #toParseableString}.
     * Subclasses that override this method should also override {@link #fromString fromString()}.
     * </p>
     *
     * @param value actual value, never null
     * @return string encoding of {@code value}
     * @throws IllegalArgumentException if {@code value} is null
     * @see <a href="http://www.w3.org/TR/REC-xml/#charsets">The XML 1.0 Specification</a>
     */
    public String toString(T value) {
        Preconditions.checkArgument(value != null, "null value");
        return this.toParseableString(value);
    }

    /**
     * Parse a non-null value previously encoded by {@link #toString(Object) toString(T)}.
     *
     * <p>
     * The implementation in {@link FieldType} creates a new {@link ParseContext} based on {@code string},
     * delegates to {@link #toParseableString} to parse it, and verifies that all of {@code string} was consumed
     * during the parse. Subclasses that override this method should also override {@link #toString(Object) toString(T)}.
     * </p>
     *
     * @param string non-null value previously encoded as a {@link String} by {@link #toString(Object) toString(T)}
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
     * or {@link NullPointerException}, if {@code obj} does not have the correct type, or is an illegal null value.
     * </p>
     *
     * <p>
     * This method is allowed to perform widening conversions of the object that lose no information, e.g.,
     * from {@link Integer} to {@link Long}.
     * </p>
     *
     * <p>
     * The implementation in {@link FieldType} simply casts the value using this instance's raw Java type.
     * Subclasses should override this method to implement any other restrictions, e.g., disallowing null values.
     * </p>
     *
     * @param obj object to validate
     * @return {@code obj} cast to this field's type
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
     * Order two field values.
     *
     * <p>
     * This method must provide a total ordering of all supported Java values that is consistent with the database ordering,
     * i.e., the unsigned lexicographical ordering of the corresponding {@code byte[]} encoded field values.
     *
     * <p>
     * If null is a supported Java value, then the returned {@link Comparator} must accept null parameters without
     * throwing an exception (note, this is a stronger requirement than the {@link Comparator} interface normally requires).
     *
     * <p>
     * Note: by convention, null values usually sort last.
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
     * Note: changing the return value of this method usually means changing the binary encoding, resulting in
     * an incompatible type.
     * </p>
     *
     * <p>
     * The implementation in {@link FieldType} returns {@code true}.
     * </p>
     *
     * @return true if an encoded value starting with {@code 0x00} exists
     */
    public boolean hasPrefix0x00() {
        return true;
    }

    /**
     * Determine whether any of this field type's encoded values start with a {@code 0xff} byte.
     * Certain optimizations are possible when this is not the case. It is safe for this method to always return true.
     *
     * <p>
     * Note: changing the return value of this method usually means changing the binary encoding, resulting in
     * an incompatible type.
     * </p>
     *
     * <p>
     * The implementation in {@link FieldType} returns {@code true}.
     * </p>
     *
     * @return true if an encoded value starting with {@code 0xff} exists
     */
    public boolean hasPrefix0xff() {
        return true;
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

// Object

    @Override
    public String toString() {
        return "field type `" + this.name + "'";
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

