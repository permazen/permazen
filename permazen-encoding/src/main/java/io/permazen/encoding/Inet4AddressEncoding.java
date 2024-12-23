
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import io.permazen.util.ByteData;

import java.net.Inet4Address;
import java.util.OptionalInt;

/**
 * Non-null {@link Inet4Address} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Binary encoding uses the binary value from {@link java.net.InetAddress#getAddress}.
 */
public class Inet4AddressEncoding extends AbstractInetAddressEncoding<Inet4Address> {

    public static final int LENGTH = 4;

    static final String PATTERN = "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+";

    private static final long serialVersionUID = -1737266234876361236L;

    public Inet4AddressEncoding() {
        super(Inet4Address.class, PATTERN);
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
