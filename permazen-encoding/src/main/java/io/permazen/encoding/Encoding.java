
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.kv.KeyRange;
import io.permazen.util.BoundType;
import io.permazen.util.Bounds;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;
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
 *  <li>Instances have an associated Java type which can represent any of the encoding's supported values. However, an encoding
 *      is not required to support <i>every</i> instance of the Java type. For example, there can be an encoding of {@link Integer}
 *      that only supports non-negative values.
 *  <li>Instances totally order their supported Java values via {@link #compare compare()}. If the associated Java type itself
 *      implements {@link Comparable}, then the two orderings do not necessarily have to agree, but they should if possible.
 *      In that case, {@link #sortsNaturally} should return true.
 *  <li>{@code null} may or may not be a supported value; see {@link #supportsNull}. If so, it must be fully supported value just
 *      like any other; for example, it must be handled by {@link #compare compare()} (typically null values sort last).
 *      Note that this is an additional requirement beyond what {@link Comparator} strictly requires.
 *  <li>There is a {@linkplain #getDefaultValue default value}. For types that support null, the default value must be null,
 *      and for types that don't support null, obviously the default value must not be null; however, an exception can be made
 *      for encodings that don't support null but don't need default values, e.g., anonymous encodings that are always wrapped
 *      within a {@link NullSafeEncoding}.
 *  <li>All <i>non-null</i> values can be encoded/decoded into a {@link String} without losing information; see
 *      {@link #toString(Object) toString()} and {@link #fromString fromString()}.
 *  <li>All values, including null if supported, can be encoded/decoded into a self-delimiting binary string (i.e., {@code byte[]}
 *      array) without losing information. Moreover, these binary strings, when sorted lexicographically using unsigned comparison,
 *      sort consistently with the encoding's {@linkplain #compare total ordering} of the corresponding Java values;
 *      see {@link #read read()} and {@link #write write()}.
 *  <li>An {@link Encoding}'s string and binary encodings and sort ordering is guaranteed to <i>never change</i>, unless the
 *      {@link EncodingId} is also changed, which effectvely defines a new encoding. However, in such scenarios automatic
 *      schema migrations are easily handled by adding appropriate logic to {@link #convert convert()}.
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
     * encoding changes in any way, then its encoding ID <b>must</b> also change. This applies only to the encoding itself,
     * and not the {@linkplain #getTypeToken associated Java type}. For example, an {@link Encoding}'s associated Java type
     * can change over time, e.g., if the Java class changes package or class name.
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
    T read(ByteReader reader);

    /**
     * Write a value to the given output.
     *
     * @param writer byte output
     * @param value value to write (possibly null)
     * @throws IllegalArgumentException if {@code value} is null and this encoding does not support null
     * @throws IllegalArgumentException if {@code writer} is null
     */
    void write(ByteWriter writer, T value);

    /**
     * Get the default value for this encoding encoded as a {@code byte[]} array.
     *
     * <p>
     * The implementation in {@link Encoding} returns the binary encoding of the value returned by
     * {@link #getDefaultValue}.
     *
     * @return encoded default value
     * @throws UnsupportedOperationException if this encoding does not have a default value
     */
    default byte[] getDefaultValueBytes() {
        final ByteWriter writer = new ByteWriter();
        try {
            this.write(writer, this.getDefaultValue());
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException(this + " does not have a default value");
        }
        return writer.getBytes();
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
     * Read and discard a {@code byte[]} encoded value from the given input.
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
    void skip(ByteReader reader);

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
            throw new IllegalArgumentException(this + " does not support values of type " + obj.getClass().getName());
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
    default void validateAndWrite(ByteWriter writer, Object obj) {
        this.write(writer, this.validate(obj));
    }

    /**
     * Calculate the {@link KeyRange} that includes exactly those encoded values that lie within the given bounds.
     *
     * @param bounds bounds to impose
     * @return {@link KeyRange} corresponding to {@code bounds}
     * @throws IllegalArgumentException if {@code bounds} is null
     */
    default KeyRange getKeyRange(Bounds<? extends T> bounds) {

        // Sanity check
        Preconditions.checkArgument(bounds != null);

        // Get inclusive byte[] lower bound
        byte[] lowerBound = ByteUtil.EMPTY;
        final BoundType lowerBoundType = bounds.getLowerBoundType();
        if (!BoundType.NONE.equals(lowerBoundType)) {
            final ByteWriter writer = new ByteWriter();
            try {
                this.write(writer, bounds.getLowerBound());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("invalid lower bound " + bounds.getLowerBound() + " for " + this, e);
            }
            lowerBound = writer.getBytes();
            if (!lowerBoundType.isInclusive())
                lowerBound = ByteUtil.getNextKey(lowerBound);
        }

        // Get exclusive byte[] upper bound
        byte[] upperBound = null;
        final BoundType upperBoundType = bounds.getUpperBoundType();
        if (!BoundType.NONE.equals(upperBoundType)) {
            final ByteWriter writer = new ByteWriter();
            try {
                this.write(writer, bounds.getUpperBound());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("invalid upper bound " + bounds.getUpperBound() + " for " + this, e);
            }
            upperBound = writer.getBytes();
            if (upperBoundType.isInclusive())
                upperBound = ByteUtil.getNextKey(upperBound);
        }

        // Done
        return new KeyRange(lowerBound, upperBound);
    }

    /**
     * Encode the given value into a {@code byte[]} array.
     *
     * <p>
     * The implementation in {@link Encoding} creates a temporary {@link ByteWriter}
     * and then delegates to {@link #write write()}.
     *
     * @param value value to encode, possibly null
     * @return encoded value
     * @throws IllegalArgumentException if {@code obj} is invalid
     */
    default byte[] encode(T value) {
        final ByteWriter writer = new ByteWriter();
        this.write(writer, value);
        return writer.getBytes();
    }

    /**
     * Decode a valid from the given {@code byte[]} array.
     *
     * <p>
     * The implementation in {@link Encoding} creates a temporary {@link ByteReader}
     * and then delegates to {@link #read read()}.
     *
     * @param bytes encoded value
     * @return decoded value, possibly null
     * @throws IllegalArgumentException if {@code bytes} is null, invalid, or contains trailing garbage
     */
    default T decode(byte[] bytes) {
        final ByteReader reader = new ByteReader(bytes);
        final T value = this.read(reader);
        if (reader.remain() > 0)
            throw new IllegalArgumentException("trailing garbage");
        return value;
    }
}
