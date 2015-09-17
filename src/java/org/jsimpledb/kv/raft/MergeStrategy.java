
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import org.jsimpledb.kv.KVTransaction;

/**
 * Merge strategy used by a {@link FallbackKVDatabase} when migrating from one underlying database to another.
 */
public interface MergeStrategy {

    /**
     * Merge data between databases.
     *
     * @param src read-only view into the database being migrated away from
     * @param dst read-write transaction open on the database being migrated to
     */
    void merge(KVTransaction src, KVTransaction dst);
}

