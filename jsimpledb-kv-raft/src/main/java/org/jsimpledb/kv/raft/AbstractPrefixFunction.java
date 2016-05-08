
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import com.google.common.base.Function;

abstract class AbstractPrefixFunction<F, T> implements Function<F, T> {

    protected final byte[] prefix;

    AbstractPrefixFunction(byte[] prefix) {
        assert prefix != null;
        this.prefix = prefix;
    }
}

