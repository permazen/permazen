
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import io.permazen.util.ByteData;

import java.net.Inet6Address;
import java.util.OptionalInt;

/**
 * Non-null {@link Inet6Address} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Binary encoding uses the binary value from {@link java.net.InetAddress#getAddress}.
 */
public class Inet6AddressEncoding extends AbstractInetAddressEncoding<Inet6Address> {

    public static final int LENGTH = 16;

    static final String PATTERN = "[:\\p{XDigit}]+";

    private static final long serialVersionUID = -5443623479173176261L;

    public Inet6AddressEncoding() {
        super(Inet6Address.class, PATTERN);
    }

    @Override
    protected int getLength(ByteData.Reader reader) {
        return LENGTH;
    }

    @Override
    public boolean hasPrefix0x00() {
        return true;
    }

    @Override
    public boolean hasPrefix0xff() {
        return true;
    }

    @Override
    public OptionalInt getFixedWidth() {
        return OptionalInt.of(LENGTH);
    }
}
