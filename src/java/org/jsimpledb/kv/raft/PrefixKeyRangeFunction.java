
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import org.jsimpledb.kv.KeyRange;

class PrefixKeyRangeFunction extends AbstractPrefixFunction<KeyRange, KeyRange> {

    public PrefixKeyRangeFunction(byte[] prefix) {
        super(prefix);
    }

    @Override
    public KeyRange apply(KeyRange range) {
        return range.prefixedBy(this.prefix);
    }
}

