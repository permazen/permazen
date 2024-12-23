
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import java.util.OptionalInt;
import java.util.UUID;

/**
 * Non-null {@link UUID} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Binary encoding is 16 bytes, consisting of the {@linkplain UUID#getMostSignificantBits eight high-order bytes} followed by the
 * {@linkplain UUID#getLeastSignificantBits eight low-order bytes}, with each of the 64 bit values having its highest order
 * bit flipped so that the encoding {@link #sortsNaturally}.
 */
public class UUIDEncoding extends AbstractEncoding<UUID> {

    private static final long serialVersionUID = -7426558458120883995L;

    private static final long MASK = 0x8000000000000000L;

    public UUIDEncoding() {
        super(UUID.class);
    }

// Encoding

    @Override
    public UUID read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        return new UUID(ByteUtil.readLong(reader) ^ MASK, ByteUtil.readLong(reader) ^ MASK);
    }

    @Override
    public void write(ByteData.Writer writer, UUID uuid) {
        Preconditions.checkArgument(uuid != null, "null uuid");
        Preconditions.checkArgument(writer != null);
        ByteUtil.writeLong(writer, uuid.getMostSignificantBits() ^ MASK);
        ByteUtil.writeLong(writer, uuid.getLeastSignificantBits() ^ MASK);
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(16);
    }

    @Override
    public UUID fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        return UUID.fromString(string);
    }

    @Override
    public String toString(UUID uuid) {
        Preconditions.checkArgument(uuid != null, "null uuid");
        return uuid.toString();
    }

    @Override
    public int compare(UUID uuid1, UUID uuid2) {
        return uuid1.compareTo(uuid2);
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
        return true;
    }

    @Override
    public boolean hasPrefix0xff() {
        return true;
    }

    @Override
    public OptionalInt getFixedWidth() {
        return OptionalInt.empty();
    }
}
