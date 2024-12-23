
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.util.ByteData;
import io.permazen.util.NaturalSortAware;
import io.permazen.util.XMLUtil;

import java.io.Serializable;
import java.util.Comparator;
import java.util.OptionalInt;

/**
 * A range of values of some Java type, along with string and binary encodings and a total ordering of those values.
 *
 * <p>
 * {@link Encoding}'s are used to map between instances of some Java type and the {@code byte[]} encodings of those instances
 * stored in a Permazen database. The {@code byte[]} encoding defines the database sort order (via unsigned lexicographical
 * ordering), and this same ordering is reflected in Java via {@link #compare compare()}.
 *
 * <p>
 * An {@link Encoding} also defines a mapping between Java instances and {@link String} values.
 *
 * <p>
 * Instances may have an associated {@link EncodingId}, which is a globally unique URN-style identifier that allows the encoding
 * to be referred to by name (e.g., in an {@link EncodingRegistry}). Encodings with no {@link EncodingId} are called <i>anonymous</i>.
 *
 * <p>
 * {@link Encoding}s must satsify these requirements:
 * <ul>
 *  <li>Instances have an associated Java type which can represent any of the encoding's supported values (see
 *      {@link #getTypeToken}). However, an encoding is not required to support <i>every</i> value of the Java type.
 *      For example, there could be an encoding of type {@link Integer} that only supports non-negative values.
 *  <li>Instances totally order their supported Java values (see {@link #compare compare()}). If the associated Java type itself
 *      implements {@link Comparable}, then the two orderings do not necessarily have to agree, but they should if possible.
 *      In that case, {@link #sortsNaturally sortsNaturally()} should return true.
 *  <li>{@code null} may or may not be a supported value (see {@link #supportsNull}). If so, it must be fully supported value
 *      just like any other; for example, it must be handled by {@link #compare compare()} (typically null values sort last).
 *      Note that this is an additional requirement beyond what {@link Comparator} strictly requires.
 *  <li>There is a {@linkplain #getDefaultValue default value}. For types that support null, the default value must be null,
 *      and for types that don't support null, obviously the default value must not be null; however, an exception can be made
 *      for encodings that don't support null but also don't need default values, e.g., anonymous encodings that are always
 *      wrapped within a {@link NullSafeEncoding}; for such encodings, {@link #getDefaultValue} should throw an
 *      {@link UnsupportedOperationException}.
 *  <li>All <i>non-null</i> values can be encoded/decoded into a {@link String} without losing information (see
 *      {@link #toString(Object) toString()} and {@link #fromString fromString()}). These strings must contain characters
 *      that are valid in an XML document only.
 *  <li>All values, including null if supported, can be encoded/decoded into a self-delimiting binary string without
 *      losing information (see {@link #read read()} and {@link #write write()}). Moreover, these binary strings,
 *      when sorted lexicographically as unsigned values, sort consistently with {@link #compare compare()}.
 *  <li>An {@link Encoding}'s string and binary encodings and sort ordering is guaranteed to <i>never change</i>, unless the
 *      {@link EncodingId} is also changed, which effectively defines a new encoding (in such scenarios, automatic schema
 *      migration is possible by adding the appropriate logic to {@link #convert convert()}).
 * </ul>
 *
 * <p>
 * Two {@link Encoding} instances should be equal according to {@link #equals equals()} only when they behave identically
 * with respect to all of the above.
 *
 * <p>
 * Instances must be stateless (and therefore also thread safe).
 *
 * @param <T> The associated Java type
 * @see EncodingRegistry
 */
public interface Encoding<T> extends Comparator<T>, NaturalSortAware, Serializable {

    /**
     * The maximum number of supported array dimensions ({@value #MAX_ARRAY_DIMENSIONS}).
     */
    int MAX_ARRAY_DIMENSIONS = 255;

    /**
     * Get the globally unique encoding ID that identifies this encoding, if any.
     *
     * <p>
     * Once associated with a specific encoding, an encoding ID must never be changed or reused. If an {@link Encoding}'s
     * behavior changes in any way, then its encoding ID <b>must</b> also change. This applies only to the encoding itself,
     * and not the {@linkplain #getTypeToken associated Java type}. For example, an {@link Encoding}'s associated Java type
     * can change over time, e.g., if its class or package name changes.
     *
     * @return this encoding's unique ID, or null if this encoding is anonymous
     */
    EncodingId getEncodingId();

    /**
     * Get the Java type corresponding to this encoding's values.
     *
     * @return the Java type used to represent this encoding's values
     */
    TypeToken<T> getTypeToken();

    /**
     * Read a value from the given input.
     *
     * @param reader byte input
     * @return decoded value (possibly null)
     * @throws IllegalArgumentException if invalid input is encountered
     * @throws IndexOutOfBoundsException if input is truncated
     * @throws IllegalArgumentException if {@code reader} is null
     */
    T read(ByteData.Reader reader);

    /**
     * Write a value to the given output.
     *
     * @param writer byte output
     * @param value value to write (possibly null)
     * @throws IllegalArgumentException if {@code value} is null and this encoding does not support null
     * @throws IllegalArgumentException if {@code writer} is null
     */
    void write(ByteData.Writer writer, T value);

    /**
     * Get the default value for this encoding encoded as a byte string.
     *
     * <p>
     * The implementation in {@link Encoding} returns the binary encoding of the value returned by
     * {@link #getDefaultValue}.
     *
     * @return encoded default value
     * @throws UnsupportedOperationException if this encoding does not have a default value
     */
    default ByteData getDefaultValueBytes() {
        final ByteData.Writer writer = ByteData.newWriter();
        try {
            this.write(writer, this.getDefaultValue());
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException(String.format("%s does not have a default value", this));
        }
        return writer.toByteData();
    }

    /**
     * Get the default value for this encoding.
     *
     * <p>
     * If this encoding {@linkplain #supportsNull supports null values}, then this must return null.
     *
     * @return default value
     * @throws UnsupportedOperationException if this encoding does not have a default value
     */
    T getDefaultValue();

    /**
     * Read and discard an encoded value from the given input.
     *
     * <p>
     * If the value skipped over is invalid, this method may, but is not required to, throw {@link IllegalArgumentException}.
     *
     * <p>
     * If the value skipped over is truncated, this method <i>must</i> throw {@link IndexOutOfBoundsException}.
     *
     * @param reader byte input
     * @throws IllegalArgumentException if invalid input is encountered
     * @throws IndexOutOfBoundsException if input is truncated
     * @throws IllegalArgumentException if {@code reader} is null
     */
    void skip(ByteData.Reader reader);

    /**
     * Encode a non-null value as a {@link String} for later decoding by {@link #fromString fromString()}.
     *
     * <p>
     * Each of the characters in the returned {@link String}, when decoded as 32-bit Unicode codepoints,
     * must contain only valid XML characters (see {@link XMLUtil#isValidChar}).
     *
     * @param value actual value, never null
     * @return string encoding of {@code value} acceptable to {@link #fromString fromString()}
     * @throws IllegalArgumentException if {@code value} is null
     * @see <a href="http://www.w3.org/TR/REC-xml/#charsets">The XML 1.0 Specification</a>
     */
    String toString(T value);

    /**
     * Parse a non-null value previously encoded by {@link #toString(Object) toString(T)}.
     *
     * @param string non-null value previously encoded as a {@link String} by {@link #toString(Object) toString(T)}
     * @return actual value
     * @throws IllegalArgumentException if the input is invalid
     * @throws IllegalArgumentException if {@code string} is null
     */
    T fromString(String string);

    /**
     * Attempt to convert a value from the given {@link Encoding} into a value of this {@link Encoding}.
     *
     * <p>
     * For a non-null {@code value}, the implementation in {@link Encoding} first checks whether the {@code value} is already
     * a valid value for this encoding; if so, the value is returned. Otherwise, it invokes
     * {@code encoding.}{@link #toString(Object) toString(value)} to convert {@code value} into a {@link String}, and then
     * attempts to parse that string via {@code this.}{@link #fromString fromString()}; if the parse fails,
     * an {@link IllegalArgumentException} is thrown. Note this means that any value will convert successfully
     * to a {@link String}, as long as it doesn't contain an invalid escape sequence (see {@link StringEncoding#toString}).
     *
     * <p>
     * If {@code value} is null, the implementation in {@link Encoding} returns null, unless this encoding does not support
     * null values, in which case an {@link IllegalArgumentException} is thrown.
     *
     * <p>
     * Permazen's built-in encodings include the following conversions:
     * <ul>
     *  <li>Non-boolean Primitive types:
     *      <ul>
     *      <li>Convert from other non-boolean primitive types as if by the corresponding Java cast
     *      <li>Convert from boolean by converting to zero (if false) or one (if true)
     *      </ul>
     *  <li>Boolean: converts from other primitive types as if by {@code value != 0}
     *  <li>A {@code char[]} array and a {@link String} are convertible to each other
     *  <li>A {@code char} and a {@link String} of length one are convertible to each other (other {@link String}s are not)
     *  <li>Arrays: converted by converting each array element individually (if possible)
     * </ul>
     *
     * @param encoding the {@link Encoding} of {@code value}
     * @param value the value to convert
     * @param <S> source encoding
     * @return {@code value} converted to this instance's type
     * @throws IllegalArgumentException if the conversion fails
     */
    default <S> T convert(Encoding<S> encoding, S value) {
        Preconditions.checkArgument(encoding != null, "null encoding");
        try {
            return this.validate(value);
        } catch (IllegalArgumentException e) {
            if (value == null)
                throw e;
        }
        return this.fromString(encoding.toString(value));
    }

    /**
     * Verify the given object is a valid instance of this {@link Encoding}'s Java type and cast it to that type.
     *
     * <p>
     * Note that this method must throw {@link IllegalArgumentException}, not {@link ClassCastException}
     * or {@link NullPointerException}, if {@code obj} does not have the correct type, or is an unsupported value
     * - including null if null is not supported.
     *
     * <p>
     * This method is allowed to perform widening conversions of the object that lose no information, e.g.,
     * from {@link Integer} to {@link Long}.
     *
     * <p>
     * The implementation in {@link Encoding} first verifies the value is not null if this instance
     * {@linkplain #supportsNull does not allow null values}, and then attempts to cast the value using
     * this instance's raw Java type. Subclasses should override this method to implement any other restrictions.
     *
     * @param obj object to validate
     * @return {@code obj} cast to this encoding's type
     * @throws IllegalArgumentException if {@code obj} in not of type T
     * @throws IllegalArgumentException if {@code obj} is null and this encoding does not support null values
     * @throws IllegalArgumentException if {@code obj} is in any other way not supported by this {@link Encoding}
     */
    @SuppressWarnings("unchecked")
    default T validate(Object obj) {
        Preconditions.checkArgument(obj != null || this.supportsNull(), "invalid null value");
        try {
            return (T)this.getTypeToken().getRawType().cast(obj);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(String.format(
              "%s does not support values of type %s", this, obj.getClass().getName()));
        }
    }

    /**
     * Order two values.
     *
     * <p>
     * This method must provide a total ordering of all supported Java values that is consistent with the database ordering,
     * i.e., the unsigned lexicographical ordering of the corresponding {@code byte[]} encoded values.
     *
     * <p>
     * If null is a supported Java value, then the this method must accept null parameters without throwing an exception
     * (note, this is a stronger requirement than the {@link Comparator} interface normally requires).
     *
     * <p>
     * Note: by convention, null values usually sort last.
     *
     * @throws IllegalArgumentException if {@code value1} or {@code value2} is null and this encoding does not support null
     */
    @Override
    int compare(T value1, T value2);

    /**
     * Determine whether this encoding supports null values.
     *
     * @return true if null is a valid value, otherwise false
     */
    boolean supportsNull();

    /**
     * Determine whether any of this encoding's encoded values start with a {@code 0x00} byte.
     * Certain optimizations are possible when this is not the case. It is safe for this method to always return true.
     *
     * <p>
     * Note: changing the result of this method may result in an incompatible encoding if this encoding
     * is wrapped in another class.
     *
     * @return true if an encoded value starting with {@code 0x00} exists
     */
    boolean hasPrefix0x00();

    /**
     * Determine whether any of this encoding's encoded values start with a {@code 0xff} byte.
     * Certain optimizations are possible when this is not the case. It is safe for this method to always return true.
     *
     * <p>
     * Note: changing the result of this method may result in an incompatible encoding if this encoding
     * is wrapped in another class.
     *
     * @return true if an encoded value starting with {@code 0xff} exists
     */
    boolean hasPrefix0xff();

    /**
     * Get the fixed width of this encoding, if any.
     *
     * <p>
     * Some encodings encode every value into the same number of bytes. For such encodings, this method returns
     * that number. For variable width encodings, this method must return empty.
     *
     * @return the number of bytes of every encoded value, or empty if the encoding length varies
     */
    OptionalInt getFixedWidth();

    /**
     * Convenience method that both validates and encodes a value.
     *
     * <p>
     * Equivalent to:
     * <blockquote><pre>
     * this.write(writer, this.validate(obj))
     * </pre></blockquote>
     *
     * @param writer byte output
     * @param obj object to validate
     * @throws IllegalArgumentException if {@code obj} in not of type T
     * @throws IllegalArgumentException if {@code obj} is null and this encoding does not support null values
     * @throws IllegalArgumentException if {@code obj} is in any other way not supported by this {@link Encoding}
     * @throws IllegalArgumentException if {@code writer} is null
     */
    default void validateAndWrite(ByteData.Writer writer, Object obj) {
        this.write(writer, this.validate(obj));
    }

    /**
     * Encode the given value into a {@code byte[]} array.
     *
     * <p>
     * The implementation in {@link Encoding} creates a temporary {@link ByteData.Writer}
     * and then delegates to {@link #write write()}.
     *
     * @param value value to encode, possibly null
     * @return encoded value
     * @throws IllegalArgumentException if {@code obj} is invalid
     */
    default ByteData encode(T value) {
        final ByteData.Writer writer = ByteData.newWriter();
        this.write(writer, value);
        return writer.toByteData();
    }

    /**
     * Decode a valid from the given byte string.
     *
     * <p>
     * The implementation in {@link Encoding} creates a temporary {@link ByteData.Reader}
     * and then delegates to {@link #read read()}.
     *
     * @param bytes encoded value
     * @return decoded value, possibly null
     * @throws IllegalArgumentException if {@code bytes} is null, invalid, or contains trailing garbage
     */
    default T decode(ByteData bytes) {
        Preconditions.checkArgument(bytes != null, "null bytes");
        final ByteData.Reader reader = bytes.newReader();
        final T value = this.read(reader);
        if (reader.remain() > 0)
            throw new IllegalArgumentException("trailing garbage");
        return value;
    }
}
