
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.encoding;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.kv.KeyRange;
import io.permazen.util.BoundType;
import io.permazen.util.Bounds;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;
import io.permazen.util.ParseContext;

import java.util.Comparator;

/**
 * Defines the binary encoding, ordering, Java type, and range of possible values for a {@link SimpleField}.
 *
 * <p>
 * An {@link Encoding} maps between instances of its supported Java type and the self-delimited {@code byte[]} encoding of
 * those instances used in the database. The {@code byte[]} encoding defines the database sort order (via unsigned
 * lexicographical ordering), and this ordering is also reflected via {@link #compare Encoding.compare()}.
 *
 * <p>
 * An {@link Encoding} also defines two mappings between Java instances and {@link String} values.
 * There are two separate {@link String} forms, a regular form and a self-delimiting form.
 *
 * <p>
 * {@link Encoding}s have these requirements and properties:
 * <ul>
 *  <li>They have an associated Java type which can represent any of the field's values in Java (possibly including null).
 *  <li>They may have an {@link EncodingId}, which is a globally unique URN-style identifier that allows the encoding
 *      to be looked up by name in an {@link EncodingRegistry}. Encodings with no {@link EncodingId} are <i>anonymous</i>.
 *  <li>Instances {@linkplain #compare totally order} their Java values. If the associated Java type implements {@link Comparable},
 *      then the two orderings do not necessarily have to agree, but they should if possible.</li>
 *  <li>All possible values can be encoded/decoded into a self-delimiting binary string (i.e., {@code byte[]} array)
 *      without losing information, and these binary strings, when sorted lexicographically using unsigned comparison,
 *      sort consistently with the {@linkplain #compare total ordering} of the corresponding Java values.</li>
 *  <li>All possible values can be encoded/decoded to/from {@link String}s without losing information,
 *      with both a {@linkplain #toString(Object) regular string form} for non-null values only and a
 *      {@linkplain #toParseableString self-delimiting string form} for any value including null
 *      (these two forms may be the same).</li>
 *  <li>{@code null} may or may not be a supported value; if so, it must be handled by {@link #compare compare()} (typically
 *      null values sort last) and have binary and string encodings just like any other value.</li>
 *  <li>There is a {@linkplain #getDefaultValue default value}. For types that support null, the default value must be null.
 *      Database field values that are equal to their field's encoding's default value are not actually stored.</li>
 * </ul>
 *
 * <p>
 * Two {@link Encoding} instances should be equal according to {@link #equals equals()} if only if they behave identically
 * with respect to all of the above.
 *
 * <p>
 * An {@link EncodingRegistry} contains a registry of {@link Encoding}s indexed by {@linkplain #getEncodingId encoding ID}.
 *
 * @param <T> The associated Java type
 * @see EncodingRegistry
 */
public interface Encoding<T> extends Comparator<T> {

    /**
     * The maximum number of supported array dimensions ({@value #MAX_ARRAY_DIMENSIONS}).
     */
    int MAX_ARRAY_DIMENSIONS = 255;

    /**
     * Get the globally unique encoding ID that identifies this encoding, if any.
     *
     * <p>
     * Once associated with a specific encoding, an encoding ID must never be changed or reused. If an {@link Encoding}'s
     * encoding changes in any way, then its encoding ID MUST also change. This applies only to the encoding itself,
     * and not the {@linkplain #getTypeToken associated Java type}. For example, an {@link Encoding}'s associated Java type
     * can change over time, e.g., when {@code javax.mail.internet.InternetAddress} moved to
     * {@code jakarta.mail.internet.InternetAddress}) in Jakarta EE 9.
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
     * @return field value (possibly null)
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
     * @return encoded default value
     */
    default byte[] getDefaultValue() {
        final ByteWriter writer = new ByteWriter();
        try {
            this.write(writer, this.getDefaultValueObject());
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException(this + " does not have a default value");
        }
        return writer.getBytes();
    }

    /**
     * Get the default value for this encoding.
     *
     * @return default value
     */
    T getDefaultValueObject();

    /**
     * Read and discard a value from the given input.
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
     * Each of the characters in the returned {@link String} must be one of the valid XML characters
     * (tab, newline, carriage return, <code>&#92;u0020 - &#92;ud7ff</code>, and <code>&#92;ue000 - &#92;fffdf</code>).
     *
     * <p>
     * The implementation in {@link Encoding} checks that {@code value} is not null, then delegates to {@link #toParseableString}.
     * Subclasses that override this method should also override {@link #fromString fromString()}.
     *
     * @param value actual value, never null
     * @return string encoding of {@code value} acceptable to {@link #fromString fromString()}
     * @throws IllegalArgumentException if {@code value} is null
     * @see <a href="http://www.w3.org/TR/REC-xml/#charsets">The XML 1.0 Specification</a>
     */
    default String toString(T value) {
        Preconditions.checkArgument(value != null, "null value");
        return this.toParseableString(value);
    }

    /**
     * Parse a non-null value previously encoded by {@link #toString(Object) toString(T)}.
     *
     * <p>
     * The implementation in {@link Encoding} creates a new {@link ParseContext} based on {@code string},
     * delegates to {@link #toParseableString} to parse it, and verifies that all of {@code string} was consumed
     * during the parse. Subclasses that override this method should also override {@link #toString(Object) toString(T)}.
     *
     * @param string non-null value previously encoded as a {@link String} by {@link #toString(Object) toString(T)}
     * @return actual value
     * @throws IllegalArgumentException if the input is invalid
     */
    default T fromString(String string) {
        final ParseContext ctx = new ParseContext(string);
        final T value = this.fromParseableString(ctx);
        if (!ctx.isEOF()) {
            throw new IllegalArgumentException("found trailing garbage starting with \""
              + ParseContext.truncate(ctx.getInput(), 20) + "\"");
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
     *
     * @param value actual value (possibly null)
     * @return string encoding of {@code value} acceptable to {@link #fromParseableString fromParseableString()}
     * @throws IllegalArgumentException if {@code value} is null and this encoding does not support null
     * @see <a href="http://www.w3.org/TR/REC-xml/#charsets">The XML 1.0 Specification</a>
     */
    String toParseableString(T value);

    /**
     * Parse a value previously encoded by {@link #toParseableString toParseableString()} as a self-delimited {@link String}
     * and positioned at the start of the given parsing context.
     *
     * @param context parse context starting with a string previously encoded via {@link #toParseableString toParseableString()}
     * @return actual value (possibly null)
     * @throws IllegalArgumentException if the input is invalid
     */
    T fromParseableString(ParseContext context);

    /**
     * Attempt to convert a value from the given {@link Encoding} into a value of this {@link Encoding}.
     *
     * <p>
     * For a non-null {@code value}, the implementation in {@link Encoding} first checks whether the {@code value} is already
     * a valid value for this encoding; if so, the value is returned. Otherwise, it invokes
     * {@code encoding.}{@link #toString(Object) toString(value)} to convert {@code value} into a {@link String}, and then
     * attempts to parse that string via {@code this.}{@link #fromString fromString()}; if the parse fails,
     * an {@link IllegalArgumentException} is thrown.
     *
     * <p>
     * If {@code value} is null, the implementation in {@link Encoding} returns null, unless this encoding does not support
     * null values, in which case an {@link IllegalArgumentException} is thrown.
     *
     * <p>
     * Permazen's built-in encodings include the following conversions:
     * <ul>
     *  <li>Primitive types other than Boolean convert as if by the corresponding Java cast</li>
     *  <li>Non-Boolean primitive types convert to Boolean as if by {@code value != 0}</li>
     *  <li>Boolean converts to non-Boolean primitive types by first converting to zero (if false) or one (if true)</li>
     *  <li>A {@code char} and a {@link String} of length one are convertible (other {@link String}s are not)</li>
     *  <li>A {@code char[]} array and a {@link String} are convertible</li>
     *  <li>Arrays are converted by converting each array element individually (if possible)</li>
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
     * or {@link NullPointerException}, if {@code obj} does not have the correct type, or is an illegal null value.
     *
     * <p>
     * This method is allowed to perform widening conversions of the object that lose no information, e.g.,
     * from {@link Integer} to {@link Long}.
     *
     * <p>
     * The implementation in {@link Encoding} first verifies the value is not null if this instance
     * {@linkplain #allowsNull does not allow null values}, and then attempts to cast the value using
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
        Preconditions.checkArgument(obj != null || this.allowsNull(), "invalid null value");
        try {
            return (T)this.getTypeToken().getRawType().cast(obj);
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
     * @throws IllegalArgumentException if {@code value1} or {@code value2} is null and this encoding does not support null
     */
    @Override
    int compare(T value1, T value2);

    /**
     * Determine whether this encoding supports null values.
     *
     * <p>
     * The implementation in {@link Encoding} returns {@code false}.
     *
     * @return true if null is a valid value, otherwise false
     */
    default boolean allowsNull() {
        return false;
    }

    /**
     * Determine whether any of this encoding's encoded values start with a {@code 0x00} byte.
     * Certain optimizations are possible when this is not the case. It is safe for this method to always return true.
     *
     * <p>
     * Note: changing the result of this method may result in an incompatible encoding if this encoding
     * is wrapped in another class.
     *
     * <p>
     * The implementation in {@link Encoding} returns {@code true}.
     *
     * @return true if an encoded value starting with {@code 0x00} exists
     */
    default boolean hasPrefix0x00() {
        return true;
    }

    /**
     * Determine whether any of this encoding's encoded values start with a {@code 0xff} byte.
     * Certain optimizations are possible when this is not the case. It is safe for this method to always return true.
     *
     * <p>
     * Note: changing the result of this method may result in an incompatible encoding if this encoding
     * is wrapped in another class.
     *
     * <p>
     * The implementation in {@link Encoding} returns {@code true}.
     *
     * @return true if an encoded value starting with {@code 0xff} exists
     */
    default boolean hasPrefix0xff() {
        return true;
    }

    /**
     * Convenience method that both validates and encodes a value.
     *
     * <p>
     * Equivalent to:
     * <blockquote><code>
     * this.write(writer, this.validate(obj))
     * </code></blockquote>
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
     * Remove any information that may differ between instances associated with the same indexed field in the same schema.
     *
     * <p>
     * This operation should be applied before using this instance with index queries.
     *
     * <p>
     * The implementation in {@link Encoding} just returns itself.
     *
     * @return this instance with all non-index-relevant information elided
     */
    default Encoding<T> genericizeForIndex() {
        return this;
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
}
