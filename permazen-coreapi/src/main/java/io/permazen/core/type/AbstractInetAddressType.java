
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;
import com.google.common.net.InetAddresses;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;
import io.permazen.util.ParseContext;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Support superclass for {@link InetAddress} types. Null values are not supported by this class.
 */
abstract class AbstractInetAddressType<T extends InetAddress> extends NonNullFieldType<T> {

    private static final long serialVersionUID = -3778250973615531382L;

    private final Class<T> addrType;
    private final String pattern;

    protected AbstractInetAddressType(Class<T> type, String pattern) {
        super(type, 0);
        this.addrType = type;
        this.pattern = pattern;
    }

// FieldType

    @Override
    public T read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        final InetAddress addr;
        try {
            addr = InetAddress.getByAddress(reader.readBytes(this.getLength(reader)));
        } catch (UnknownHostException e) {
            throw new RuntimeException("unexpected exception", e);
        }
        return this.addrType.cast(addr);
    }

    protected abstract int getLength(ByteReader reader);

    @Override
    public void write(ByteWriter writer, T addr) {
        Preconditions.checkArgument(writer != null);
        Preconditions.checkArgument(addr != null);
        final byte[] bytes = addr.getAddress();
        assert bytes.length == this.getLength(new ByteReader(bytes));
        writer.write(addr.getAddress());
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        reader.skip(this.getLength(reader));
    }

    @Override
    public String toString(T addr) {
        Preconditions.checkArgument(addr != null);
        return InetAddresses.toAddrString(addr);
    }

    @Override
    public T fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        try {
            return this.addrType.cast(InetAddresses.forString(string));
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("invalid " + this.addrType.getSimpleName() + " `" + string + "'");
        }
    }

    @Override
    public String toParseableString(T addr) {
        return this.toString(addr);
    }

    @Override
    public T fromParseableString(ParseContext ctx) {
        Preconditions.checkArgument(ctx != null);
        return this.fromString(ctx.matchPrefix(this.pattern).group());
    }

    @Override
    public int compare(T addr1, T addr2) {
        final byte[] bytes1 = addr1.getAddress();
        final byte[] bytes2 = addr2.getAddress();
        int diff = Integer.compare(bytes1.length, bytes2.length);
        if (diff != 0)
            return diff;
        return ByteUtil.compare(bytes1, bytes2);
    }
}

