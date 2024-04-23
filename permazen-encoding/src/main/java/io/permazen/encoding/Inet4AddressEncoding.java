
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import io.permazen.util.ByteReader;

import java.net.Inet4Address;

/**
 * Non-null {@link Inet4Address} type. Null values are not supported by this class.
 *
 * <p>
 * Binary encoding uses the binary value from {@link java.net.InetAddress#getAddress}.
 */
public class Inet4AddressEncoding extends AbstractInetAddressEncoding<Inet4Address> {

    public static final int LENGTH = 4;

    static final String PATTERN = "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+";

    private static final long serialVersionUID = -1737266234876361236L;

    public Inet4AddressEncoding(EncodingId encodingId) {
        super(encodingId, Inet4Address.class, PATTERN);
    }

    @Override
    public Inet4AddressEncoding withEncodingId(EncodingId encodingId) {
        return new Inet4AddressEncoding(encodingId);
    }

    @Override
    protected int getLength(ByteReader reader) {
        return LENGTH;
    }
}
