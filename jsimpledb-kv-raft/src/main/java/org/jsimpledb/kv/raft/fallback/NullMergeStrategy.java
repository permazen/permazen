
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.fallback;

import java.util.Date;

import org.jsimpledb.kv.KVTransaction;

/**
 * {@link MergeStrategy} that does nothing, i.e., it leaves the destination database unmodified.
 */
public class NullMergeStrategy implements MergeStrategy {

    @Override
    public void merge(KVTransaction src, KVTransaction dst, Date lastActiveTime) {
        // do nothing
    }

    @Override
    public String toString() {
        return "Null";
    }
}

