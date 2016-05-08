
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import org.jsimpledb.kv.KeyRange;

class PrefixKeyRangeFunction extends AbstractPrefixFunction<KeyRange, KeyRange> {

    PrefixKeyRangeFunction(byte[] prefix) {
        super(prefix);
    }

    @Override
    public KeyRange apply(KeyRange range) {
        return range.prefixedBy(this.prefix);
    }
}

