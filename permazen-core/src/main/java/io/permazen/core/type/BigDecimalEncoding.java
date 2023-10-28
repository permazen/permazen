
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.LongEncoder;
import io.permazen.util.ParseContext;
import io.permazen.util.UnsignedIntEncoder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * {@link BigDecimal} type. Null values are not supported by this class.
 *
 * <p>
 * This class' encoding preserves precision information, and therefore treats as distinct instances that differ only in
 * trailing zeroes after the decimal point. For example, {@code 1.23 < 1.230}.
 *
 * <p>
 * As a result, this class' {@link #compare compare()} method is consistent with {@link BigDecimal#equals BigDecimal.equals()},
 * unlike {@link BigDecimal}'s own {@link BigDecimal#compareTo compareTo()} method, which is not.
 */
public class BigDecimalEncoding extends BuiltinEncoding<BigDecimal> {

    private static final long serialVersionUID = -6401896548616656153L;

    private static final int FLAG_NEGATIVE = 0x01;
    private static final int FLAG_ZERO = 0x02;
    private static final int FLAG_POSITIVE = 0x03;

    public BigDecimalEncoding() {
        super(BigDecimal.class);
    }

// Encoding

    @Override
    public boolean hasPrefix0x00() {
        return false;
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }

    @Override
    public BigDecimal read(ByteReader reader) {

        // Read sign byte
        Preconditions.checkArgument(reader != null);
        final boolean negative;
        switch (reader.readByte()) {
        case FLAG_ZERO:
            return new BigDecimal(BigInteger.ZERO, UnsignedIntEncoder.read(reader));
        case FLAG_NEGATIVE:
            negative = true;
            break;
        case FLAG_POSITIVE:
            negative = false;
            break;
        default:
            throw new IllegalArgumentException("invalid encoded BigDecimal");
        }

        // Read exponent
        long exponent = LongEncoder.read(reader);
        if (negative)
            exponent = -exponent;
        Preconditions.checkArgument(exponent >= -Integer.MAX_VALUE && exponent <= Integer.MAX_VALUE, "invalid encoded BigDecimal");

        // Read BigInteger digits
        final StringBuilder digits = new StringBuilder();
      digitsLoop:
        while (true) {
            int value = reader.readByte();
            assert (value & ~0xff) == 0;
            if (negative)
                value = value ^ 0xff;
            for (int nibble : new int[] { value >> 4, value & 0x0f }) {
                if (nibble-- == 0)
                    break digitsLoop;
                if (nibble > 9)
                    throw new IllegalArgumentException("invalid encoded BigDecimal");
                digits.append(Character.forDigit(nibble, 10));
            }
        }
        final BigInteger unscaledValue = new BigInteger(digits.toString());
        final long scale = digits.length() - 1 - exponent;
        Preconditions.checkArgument(scale >= -Integer.MAX_VALUE && scale <= Integer.MAX_VALUE, "invalid encoded BigDecimal");
        BigDecimal value = new BigDecimal(unscaledValue, (int)scale);
        return negative ? value.negate() : value;
    }

    @Override
    public void write(ByteWriter writer, BigDecimal value) {

        // Sanity check
        Preconditions.checkArgument(writer != null);
        Preconditions.checkArgument(value != null);

        // Handle zero
        if (value.signum() == 0) {
            writer.writeByte(FLAG_ZERO);
            UnsignedIntEncoder.write(writer, value.scale());
            return;
        }

        // Get parts
        final boolean negative = value.signum() < 0;
        if (negative)
            value = value.abs();
        final String digits;
        final long exponent;
        if (value.unscaledValue().equals(BigInteger.ZERO)) {
            final char[] array = new char[value.scale() + 1];
            Arrays.fill(array, '0');
            digits = new String(array);
        } else
            digits = value.unscaledValue().toString();
        exponent = digits.length() - 1 - value.scale();

        // Write sign byte
        writer.writeByte(negative ? FLAG_NEGATIVE : FLAG_POSITIVE);

        // Write exponent
        LongEncoder.write(writer, negative ? -exponent : exponent);

        // Write digits
        int nextByte = 0;
        for (int i = 0; i < digits.length(); i++) {
            final int digit = Character.digit(digits.charAt(i), 10);
            assert digit >= 0 && digit < 10;
            nextByte = (nextByte << 4) | (digit + 1);
            if ((i & 1) == 1)
                writer.writeByte(negative ? ~nextByte : nextByte);
        }
        if ((digits.length() & 1) == 1)
            nextByte <<= 4;
        else
            nextByte = 0;
        writer.writeByte(negative ? ~nextByte : nextByte);
    }

    @Override
    public void skip(ByteReader reader) {
        final int flag = reader.readByte();
        reader.skip(LongEncoder.decodeLength(reader.peek()));
        if (flag == FLAG_ZERO)
            return;
        while (true) {
            final int value = reader.readByte();
            if ((value & 0x0f) == 0)
                break;
        }
    }

    @Override
    public String toString(BigDecimal value) {
        Preconditions.checkArgument(value != null);
        return value.toString();
    }

    @Override
    public BigDecimal fromString(String string) {
        Preconditions.checkArgument(string != null);
        return new BigDecimal(string);
    }

    @Override
    public String toParseableString(BigDecimal value) {
        return this.toString(value);
    }

    @Override
    public BigDecimal fromParseableString(ParseContext ctx) {
        Preconditions.checkArgument(ctx != null);
        return this.fromString(ctx.matchPrefix(
          "[-+]?(\\p{Digit}+(\\.\\p{Digit}+)?|\\.\\p{Digit}+)([Ee][-+]?\\p{Digit}+)?").group());
    }

    @Override
    public int compare(BigDecimal value1, BigDecimal value2) {
        int diff = value1.compareTo(value2);
        if (diff != 0)
            return diff;
        return value1.signum() < 0 ?
          Integer.compare(value2.scale(), value1.scale()) : Integer.compare(value1.scale(), value2.scale());
    }
}
