
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.transaction.xa.Xid;

import org.dellroad.stuff.string.ByteArrayEncoder;

final class SimpleXid implements Xid {

    public static final String PATTERN = "([0-9a-f]{8})\\.(([0-9a-f][0-9a-f])+)\\.(([0-9a-f][0-9a-f])+)";

    private final int format;
    private final byte[] global;
    private final byte[] branch;

    public SimpleXid(int format, byte[] global, byte[] branch) {
        if (global == null)
            throw new IllegalArgumentException("null global");
        if (branch == null)
            throw new IllegalArgumentException("null branch");
        this.format = format;
        this.global = global;
        this.branch = branch;
    }

    @Override
    public int getFormatId() {
        return this.format;
    }

    @Override
    public byte[] getGlobalTransactionId() {
        return this.global;
    }

    @Override
    public byte[] getBranchQualifier() {
        return this.branch;
    }

    @Override
    public int hashCode() {
        return this.format ^ Arrays.hashCode(this.global) ^ Arrays.hashCode(this.branch);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!(obj instanceof Xid))
            return false;
        final Xid that = (Xid)obj;
        return this.getFormatId() == that.getFormatId()
          && Arrays.equals(this.global, that.getGlobalTransactionId())
          && Arrays.equals(this.branch, that.getBranchQualifier());
    }

    public static String toString(Xid xid) {
        return String.format("%08x.%s.%s", xid.getFormatId(),
          ByteArrayEncoder.encode(xid.getGlobalTransactionId()), ByteArrayEncoder.encode(xid.getBranchQualifier()));
    }

    public static Xid fromString(String string) {
        final Matcher matcher = Pattern.compile(PATTERN).matcher(string);
        if (!matcher.matches())
            return null;
        final int format = (int)Long.parseLong(matcher.group(1), 16);
        final byte[] global = ByteArrayEncoder.decode(matcher.group(2));
        final byte[] branch = ByteArrayEncoder.decode(matcher.group(3));
        return new SimpleXid(format, global, branch);
    }
}

