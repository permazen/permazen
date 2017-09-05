
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.fallback;

import io.permazen.kv.KVTransaction;

import java.util.Date;

/**
 * Merge strategy used by a {@link FallbackKVDatabase} when migrating from one underlying database to another.
 */
@FunctionalInterface
public interface MergeStrategy {

    /**
     * Merge data from the source database being migrated away from into the destination database being migrated to.
     *
     * <p>
     * This method should {@link KVTransaction#commit commit()} both transactions before returning; {@code src}
     * should be committed before {@code dst}.
     *
     * <p>
     * If an exception is thrown, {@link KVTransaction#rollback rollback()} will be invoked on both transactions
     * (a {@link KVTransaction#rollback rollback()} on an already-{@link KVTransaction#commit commit()}'ed
     * transaction does nothing).
     *
     * @param src read-only view into the database being migrated away from
     * @param dst read-write transaction open on the database being migrated to
     * @param lastActiveTime time that {@code dst} was last active, or null if never
     * @throws RuntimeException if the merge or either {@link KVTransaction#commit commit()} fails
     */
    void mergeAndCommit(KVTransaction src, KVTransaction dst, Date lastActiveTime);
}

