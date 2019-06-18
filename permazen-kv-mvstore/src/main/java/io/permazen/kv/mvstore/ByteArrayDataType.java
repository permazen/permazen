
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvstore;

import io.permazen.kv.util.KeyListEncoder;
import io.permazen.util.ByteUtil;
import io.permazen.util.UnsignedIntEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.dellroad.stuff.io.ByteBufferInputStream;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;

/**
 * MVStore {@link DataType} for {@code byte[]} arrays sorted lexicographically.
 *
 * <p>
 * When keys are encoded in bulk, prefix compression is applied via {@link KeyListEncoder}.
 */
public final class ByteArrayDataType implements DataType {

    public static final ByteArrayDataType INSTANCE = new ByteArrayDataType();

    private ByteArrayDataType() {
    }

// DataType

    @Override
    public int compare(Object a, Object b) {
        return ByteUtil.compare((byte[])a, (byte[])b);
    }

    @Override
    public int getMemory(Object obj) {
        return 16 + ((byte[])obj).length;                       // XXX accurate?
    }

// Writing

    @Override
    public void write(WriteBuffer buf, Object obj) {
        final byte[] bytes = (byte[])obj;
        buf.put(UnsignedIntEncoder.encode(bytes.length)).put(bytes);
    }

    @Override
    public void write(WriteBuffer buf, Object[] obj, int len, boolean key) {
        if (key)
            this.writeKeys(buf, obj, len);
        else
            this.writeValues(buf, obj, len);
    }

    protected void writeKeys(WriteBuffer buf, Object[] obj, int len) {
        final ByteArrayOutputStream data = new ByteArrayOutputStream();
        byte[] prev = null;
        for (int i = 0; i < len; i++) {
            final byte[] next = (byte[])obj[i];
            try {
                KeyListEncoder.write(data, next, prev);
            } catch (IOException e) {
                throw new RuntimeException("unexpected error", e);
            }
            prev = next;
        }
        buf.put(data.toByteArray());
    }

    protected void writeValues(WriteBuffer buf, Object[] obj, int len) {
        for (int i = 0; i < len; i++)
            this.write(buf, (byte[])obj[i]);
    }

// Reading

    @Override
    public byte[] read(ByteBuffer buf) {
        final byte[] bytes = new byte[UnsignedIntEncoder.read(buf)];
        buf.get(bytes);
        return bytes;
    }

    @Override
    public void read(ByteBuffer buf, Object[] obj, int len, boolean key) {
        if (key)
            this.readKeys(buf, obj, len);
        else
            this.readValues(buf, obj, len);
    }

    protected void readKeys(ByteBuffer buf, Object[] obj, int len) {
        final ByteBufferInputStream input = new ByteBufferInputStream(buf);
        byte[] prev = null;
        for (int i = 0; i < len; i++) {
            final byte[] next;
            try {
                next = KeyListEncoder.read(input, prev);
            } catch (IOException e) {
                throw new RuntimeException("unexpected error", e);
            }
            obj[i] = next;
            prev = next;
        }
    }

    protected void readValues(ByteBuffer buf, Object[] obj, int len) {
        for (int i = 0; i < len; i++)
            obj[i] = this.read(buf);
    }
}
