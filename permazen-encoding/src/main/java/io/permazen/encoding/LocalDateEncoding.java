
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.LongEncoder;

import java.time.LocalDate;
import java.util.OptionalInt;

/**
 * Non-null {@link LocalDate} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Binary encoding is via a single {@link LongEncoder}-encoded value representing the {@linkplain LocalDate#toEpochDay epoch day}.
 */
public class LocalDateEncoding extends AbstractEncoding<LocalDate> {

    private static final long serialVersionUID = -1245720029314097665L;

    public LocalDateEncoding() {
        super(LocalDate.class);
    }

// Encoding

    @Override
    public LocalDate read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        return LocalDate.ofEpochDay(LongEncoder.read(reader));
    }

    @Override
    public void write(ByteData.Writer writer, LocalDate localDate) {
        Preconditions.checkArgument(localDate != null, "null localDate");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, localDate.toEpochDay());
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(LongEncoder.decodeLength(reader.peek()));
    }

    @Override
    public LocalDate fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return LocalDate.parse(string);
    }

    @Override
    public String toString(LocalDate localDate) {
        Preconditions.checkArgument(localDate != null, "null localDate");
        return localDate.toString();
    }

    @Override
    public int compare(LocalDate localDate1, LocalDate localDate2) {
        return localDate1.compareTo(localDate2);
    }

    @Override
    public boolean supportsNull() {
        return false;
    }

    @Override
    public boolean sortsNaturally() {
        return true;
    }

    @Override
    public boolean hasPrefix0x00() {
        return false;
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }

    @Override
    public OptionalInt getFixedWidth() {
        return OptionalInt.empty();
    }
}
