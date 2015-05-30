
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.UUID;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;

/**
 * Non-null {@link UUID} type. Null values are not supported by this class.
 */
class UUIDType extends NonNullFieldType<UUID> {

    private static final long MASK = 0x8000000000000000L;
    private static final String PATTERN = "\\p{XDigit}{8}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{4}-\\p{XDigit}{12}";

    UUIDType() {
        super(UUID.class, 0);
    }

// FieldType

    @Override
    public UUID read(ByteReader reader) {
        return new UUID(ByteUtil.readLong(reader) ^ MASK, ByteUtil.readLong(reader) ^ MASK);
    }

    @Override
    public void write(ByteWriter writer, UUID uuid) {
        if (uuid == null)
            throw new IllegalArgumentException("null uuid");
        ByteUtil.writeLong(writer, uuid.getMostSignificantBits() ^ MASK);
        ByteUtil.writeLong(writer, uuid.getLeastSignificantBits() ^ MASK);
    }

    @Override
    public void skip(ByteReader reader) {
        reader.skip(16);
    }

    @Override
    public UUID fromParseableString(ParseContext ctx) {
        return java.util.UUID.fromString(ctx.matchPrefix(PATTERN).group());
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

