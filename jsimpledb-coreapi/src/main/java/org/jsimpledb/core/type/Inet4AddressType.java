
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import java.net.Inet4Address;

import org.jsimpledb.util.ByteReader;

/**
 * Non-null {@link Inet4Address} type. Null values are not supported by this class.
 */
public class Inet4AddressType extends AbstractInetAddressType<Inet4Address> {

    public static final int LENGTH = 4;

    static final String PATTERN = "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+";

    private static final long serialVersionUID = -1737266234876361236L;

    public Inet4AddressType() {
        super(Inet4Address.class, PATTERN);
    }

    @Override
    protected int getLength(ByteReader reader) {
        return LENGTH;
    }
}

