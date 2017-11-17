
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;

import java.net.InetAddress;

/**
 * Non-null {@link InetAddress} type. Null values are not supported by this class.
 *
 * <p>
 * Binary encoding uses the binary value from {@link java.net.InetAddress#getAddress}, preceded by
 * {@code 0x04} for IPv4 or {@code 0x06} for IPv6.
 */
public class InetAddressType extends AbstractInetAddressType<InetAddress> {

    private static final long serialVersionUID = 3568938922398348273L;

    private static final byte PREFIX_IPV4 = 4;
    private static final byte PREFIX_IPV6 = 6;

    public InetAddressType() {
        super(InetAddress.class, "(" + Inet4AddressType.PATTERN + "|" + Inet6AddressType.PATTERN + ")");
    }

// FieldType

    @Override
    public boolean hasPrefix0x00() {
        return false;
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }

    @Override
    protected int getLength(ByteReader reader) {
        switch (reader.readByte()) {
        case PREFIX_IPV4:
            return Inet4AddressType.LENGTH;
        case PREFIX_IPV6:
            return Inet6AddressType.LENGTH;
        default:
            throw new IllegalArgumentException("invalid encoded InetAddress");
        }
    }

    @Override
    public void write(ByteWriter writer, InetAddress addr) {
        Preconditions.checkArgument(writer != null);
        Preconditions.checkArgument(addr != null);
        final byte[] bytes = addr.getAddress();
        switch (bytes.length) {
        case Inet4AddressType.LENGTH:
            writer.writeByte(PREFIX_IPV4);
            break;
        case Inet6AddressType.LENGTH:
            writer.writeByte(PREFIX_IPV6);
            break;
        default:
            throw new RuntimeException("internal error");
        }
        writer.write(bytes);
    }
}

