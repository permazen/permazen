
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.encoding.AbstractEncoding;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;

/**
 * Non-null encoding for {@link ObjId}s.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Binary encoding uses the value from {@link ObjId#getBytes}.
 */
public class ObjIdEncoding extends AbstractEncoding<ObjId> {

    private static final long serialVersionUID = 6921359865864012847L;

// Constructors

    /**
     * Constructor.
     */
    public ObjIdEncoding() {
        super(ObjId.class);
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
    public ObjId fromString(String string) {
        return new ObjId(string);
    }

    @Override
    public String toString(ObjId value) {
        Preconditions.checkArgument(value != null, "null value");
        return value.toString();
    }

    @Override
    public int compare(ObjId id1, ObjId id2) {
        return id1.compareTo(id2);
    }

    @Override
    public boolean sortsNaturally() {
        return true;
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
