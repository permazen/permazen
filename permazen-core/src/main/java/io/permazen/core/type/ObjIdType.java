
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;

import io.permazen.core.ObjId;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.ParseContext;

/**
 * Non-null encoding for encoding {@link ObjId}s. Null values are not supported by this class.
 *
 * <p>
 * Binary encoding uses the value from {@link ObjId#getBytes}.
 */
public class ObjIdType extends NonNullEncoding<ObjId> {

    private static final long serialVersionUID = 6921359865864012847L;

    public ObjIdType() {
        super(null, ObjId.class);
    }

// Encoding

    @Override
    public ObjId read(ByteReader reader) {
        return new ObjId(reader);
    }

    @Override
    public void write(ByteWriter writer, ObjId id) {
        Preconditions.checkArgument(writer != null);
        writer.write(id.getBytes());
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(ObjId.NUM_BYTES);
    }

    @Override
    public ObjId fromParseableString(ParseContext ctx) {
        return new ObjId(ctx.matchPrefix(ObjId.PATTERN).group());
    }

    @Override
    public String toParseableString(ObjId value) {
        return value.toString();
    }

    @Override
    public int compare(ObjId id1, ObjId id2) {
        return id1.compareTo(id2);
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }

    @Override
    public boolean hasPrefix0x00() {
        return false;                       // ObjId's may not have a storage ID of zero
    }
}
