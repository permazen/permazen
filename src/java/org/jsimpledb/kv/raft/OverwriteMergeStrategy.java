
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import java.util.Iterator;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransaction;

/**
 * {@link MergeStrategy} that completely overwrites the destination database with the content of the source database.
 */
public class OverwriteMergeStrategy implements MergeStrategy {

    @Override
    public void merge(KVTransaction src, KVTransaction dst) {
        dst.removeRange(null, null);
        final Iterator<KVPair> i = src.getRange(null, null, false);
        while (i.hasNext()) {
            final KVPair pair = i.next();
            dst.put(pair.getKey(), pair.getValue());
        }
        Util.closeIfPossible(i);
    }

    @Override
    public String toString() {
        return "overwrite merge strategy";
    }
}

