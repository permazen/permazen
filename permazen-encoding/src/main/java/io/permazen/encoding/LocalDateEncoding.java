
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.LongEncoder;

import java.time.LocalDate;

/**
 * Non-null {@link LocalDate} type. Null values are not supported by this class.
 *
 * <p>
 * Binary encoding is via a single {@link LongEncoder}-encoded value representing the {@linkplain LocalDate#toEpochDay epoch day}.
 */
public class LocalDateEncoding extends AbstractEncoding<LocalDate> {

    private static final long serialVersionUID = -1245720029314097665L;

    public LocalDateEncoding(EncodingId encodingId) {
        super(encodingId, LocalDate.class, LocalDate.ofEpochDay(0));
    }

// Encoding

    @Override
    public LocalDateEncoding withEncodingId(EncodingId encodingId) {
        return new LocalDateEncoding(encodingId);
    }

    @Override
    public LocalDate read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return LocalDate.ofEpochDay(LongEncoder.read(reader));
    }

    @Override
    public void write(ByteWriter writer, LocalDate localDate) {
        Preconditions.checkArgument(localDate != null, "null localDate");
        Preconditions.checkArgument(writer != null);
        LongEncoder.write(writer, localDate.toEpochDay());
    }

    @Override
    public void skip(ByteReader reader) {
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
}
