
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.ParseContext;

/**
 * Non-null field type for encoding {@link ObjId}s. Null values are not supported by this class.
 */
public class ObjIdType extends NonNullFieldType<ObjId> {

    private static final long serialVersionUID = 6921359865864012847L;

    public ObjIdType() {
        super(ObjId.class, 0);
    }

// FieldType

    @Override
    public ObjId read(ByteReader reader) {
        return new ObjId(reader);
    }

    @Override
    public void write(ByteWriter writer, ObjId id) {
        writer.write(id.getBytes());
    }

    @Override
    public void skip(ByteReader reader) {
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

