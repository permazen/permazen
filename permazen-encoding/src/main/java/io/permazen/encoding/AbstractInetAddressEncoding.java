
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.net.InetAddresses;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Support superclass for non-null {@link InetAddress} types.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 */
abstract class AbstractInetAddressEncoding<T extends InetAddress> extends AbstractEncoding<T> {

    private static final long serialVersionUID = -3778250973615531382L;

    private final Class<T> addrType;
    private final String pattern;

    protected AbstractInetAddressEncoding(Class<T> addrType, String pattern) {
        super(addrType);
        Preconditions.checkArgument(pattern != null);
        this.addrType = addrType;
        this.pattern = pattern;
    }

// Encoding

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
        Preconditions.checkArgument(addr != null, "null addr");
        return InetAddresses.toAddrString(addr);
    }

    @Override
    public T fromString(String string) {
        Preconditions.checkArgument(string != null, "null string");
        try {
            return this.addrType.cast(InetAddresses.forString(string));
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(String.format("invalid %s \"%s\"", this.addrType.getSimpleName(), string));
        }
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
