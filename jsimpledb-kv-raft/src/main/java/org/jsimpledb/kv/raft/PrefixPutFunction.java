
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import com.google.common.primitives.Bytes;

import java.util.AbstractMap;
import java.util.Map;

class PrefixPutFunction extends AbstractPrefixFunction<Map.Entry<byte[], byte[]>, Map.Entry<byte[], byte[]>> {

    PrefixPutFunction(byte[] prefix) {
        super(prefix);
    }

    @Override
    public Map.Entry<byte[], byte[]> apply(Map.Entry<byte[], byte[]> entry) {
        return new AbstractMap.SimpleEntry<byte[], byte[]>(Bytes.concat(this.prefix, entry.getKey()), entry.getValue());
    }
}

