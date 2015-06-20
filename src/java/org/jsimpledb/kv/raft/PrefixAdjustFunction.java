
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import com.google.common.primitives.Bytes;

import java.util.AbstractMap;
import java.util.Map;

class PrefixAdjustFunction extends AbstractPrefixFunction<Map.Entry<byte[], Long>, Map.Entry<byte[], Long>> {

    public PrefixAdjustFunction(byte[] prefix) {
        super(prefix);
    }

    @Override
    public Map.Entry<byte[], Long> apply(Map.Entry<byte[], Long> entry) {
        return new AbstractMap.SimpleEntry<byte[], Long>(Bytes.concat(this.prefix, entry.getKey()), entry.getValue());
    }
}

