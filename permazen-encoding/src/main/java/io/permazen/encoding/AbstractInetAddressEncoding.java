
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.net.InetAddresses;

import io.permazen.util.ByteData;

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
    public T read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        final InetAddress addr;
        try {
            addr = InetAddress.getByAddress(reader.readBytes(this.getLength(reader)).toByteArray());
        } catch (UnknownHostException e) {
            throw new RuntimeException("unexpected exception", e);
        }
        return this.addrType.cast(addr);
    }

    protected abstract int getLength(ByteData.Reader reader);

    @Override
    public void write(ByteData.Writer writer, T addr) {
        Preconditions.checkArgument(writer != null);
        Preconditions.checkArgument(addr != null);
        final ByteData bytes = ByteData.of(addr.getAddress());
        assert bytes.size() == this.getLength(bytes.newReader());
        writer.write(bytes);
    }

    @Override
    public void skip(ByteData.Reader reader) {
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
        final ByteData bytes1 = ByteData.of(addr1.getAddress());
        final ByteData bytes2 = ByteData.of(addr2.getAddress());
        int diff = Integer.compare(bytes1.size(), bytes2.size());
        if (diff != 0)
            return diff;
        return bytes1.compareTo(bytes2);
    }

    @Override
    public boolean supportsNull() {
        return false;
    }

    @Override
    public boolean sortsNaturally() {
        return false;
    }
}
