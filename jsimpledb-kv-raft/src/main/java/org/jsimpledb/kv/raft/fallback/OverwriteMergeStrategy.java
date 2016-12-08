
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.fallback;

import java.util.Date;
import java.util.Iterator;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransaction;

/**
 * {@link MergeStrategy} that completely overwrites the destination database with the content of the source database.
 */
public class OverwriteMergeStrategy implements MergeStrategy {

    @Override
    public void mergeAndCommit(KVTransaction src, KVTransaction dst, Date lastActiveTime) {
        this.overwrite(src, dst);
        src.commit();
        dst.commit();
    }

    /**
     * Overwrite one key/value database with another.
     *
     * <p>
     * This method deletes every key/value pair in {@code dst}, and then copy every key/value pair
     * in {@code src} into {@code dst}.
     *
     * <p>
     * Does not commit {@code src} or {@code dst}.
     *
     * @param src database to copy from
     * @param dst database to copy {@code src} onto
     */
    protected void overwrite(KVTransaction src, KVTransaction dst) {
        dst.removeRange(null, null);
        final Iterator<KVPair> i = src.getRange(null, null, false);
        while (i.hasNext()) {
            final KVPair pair = i.next();
            dst.put(pair.getKey(), pair.getValue());
        }
        try {
            ((AutoCloseable)i).close();
        } catch (Exception e) {
            // ignore
        }
    }

    @Override
    public String toString() {
        return "Overwrite";
    }
}

