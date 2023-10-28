
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;
import io.permazen.util.ParseContext;

import java.util.UUID;

/**
 * Non-null {@link UUID} type. Null values are not supported by this class.
 *
 * <p>
 * Binary encoding is 16 bytes, consisting of the {@linkplain UUID#getMostSignificantBits eight high-order bytes} followed by the
 * {@linkplain UUID#getLeastSignificantBits eight low-order bytes}.
 */
public class UUIDEncoding extends BuiltinEncoding<UUID> {

    private static final long serialVersionUID = -7426558458120883995L;

    private static final long MASK = 0x8000000000000000L;
    private static final String PATTERN = "\\p{XDigit}{8}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{12}";

    public UUIDEncoding() {
        super(UUID.class);
    }

// Encoding

    @Override
    public UUID read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return new UUID(ByteUtil.readLong(reader) ^ MASK, ByteUtil.readLong(reader) ^ MASK);
    }

    @Override
    public void write(ByteWriter writer, UUID uuid) {
        Preconditions.checkArgument(uuid != null, "null uuid");
        Preconditions.checkArgument(writer != null);
        ByteUtil.writeLong(writer, uuid.getMostSignificantBits() ^ MASK);
        ByteUtil.writeLong(writer, uuid.getLeastSignificantBits() ^ MASK);
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(16);
    }

    @Override
    public UUID fromParseableString(ParseContext ctx) {
        return UUID.fromString(ctx.matchPrefix(PATTERN).group());
    }

    @Override
    public String toParseableString(UUID uuid) {
        return uuid.toString();
    }

    @Override
    public int compare(UUID uuid1, UUID uuid2) {
        return uuid1.compareTo(uuid2);
    }
}
