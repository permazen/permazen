
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;

import java.net.InetAddress;
import java.util.OptionalInt;

/**
 * Non-null {@link InetAddress} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Binary encoding uses the binary value from {@link java.net.InetAddress#getAddress}, preceded by
 * {@code 0x04} for IPv4 or {@code 0x06} for IPv6.
 */
public class InetAddressEncoding extends AbstractInetAddressEncoding<InetAddress> {

    private static final long serialVersionUID = 3568938922398348273L;

    private static final byte PREFIX_IPV4 = 4;
    private static final byte PREFIX_IPV6 = 6;

    public InetAddressEncoding() {
        super(InetAddress.class, "(" + Inet4AddressEncoding.PATTERN + "|" + Inet6AddressEncoding.PATTERN + ")");
    }

// Encoding

    @Override
    public boolean hasPrefix0x00() {
        return false;
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }

    @Override
    public OptionalInt getFixedWidth() {
        return OptionalInt.empty();
    }

    @Override
    protected int getLength(ByteData.Reader reader) {
        switch (reader.readByte()) {
        case PREFIX_IPV4:
            return Inet4AddressEncoding.LENGTH;
        case PREFIX_IPV6:
            return Inet6AddressEncoding.LENGTH;
        default:
            throw new IllegalArgumentException("invalid encoded InetAddress");
        }
    }

    @Override
    public void write(ByteData.Writer writer, InetAddress addr) {
        Preconditions.checkArgument(writer != null);
        Preconditions.checkArgument(addr != null);
        final byte[] bytes = addr.getAddress();
        switch (bytes.length) {
        case Inet4AddressEncoding.LENGTH:
            writer.write(PREFIX_IPV4);
            break;
        case Inet6AddressEncoding.LENGTH:
            writer.write(PREFIX_IPV6);
            break;
        default:
            throw new RuntimeException("internal error");
        }
        writer.write(bytes);
    }
}
