
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.RetryTransactionException;
import io.permazen.kv.util.MemoryKVStore;

/**
 * A simple in-memory implementation of the {@link KVDatabase} interface.
 *
 * <p>
 * This is just a {@link SimpleKVDatabase} with a {@link MemoryKVStore} as its underlying key/value store.
 * See {@link SimpleKVDatabase} for further details.
 *
 * @see SimpleKVDatabase
 * @see MemoryKVStore
 */
public class MemoryKVDatabase extends SimpleKVDatabase {

    private static final long serialVersionUID = -1963855429310582609L;

    /**
     * Constructor.
     *
     * <p>
     * Uses the default wait and hold timeouts.
     */
    public MemoryKVDatabase() {
        super(new MemoryKVStore());
    }

    /**
     * Primary constructor.
     *
     * @param waitTimeout how long a thread will wait for a lock before throwing {@link RetryTransactionException}
     *  in milliseconds, or zero for unlimited
     * @param holdTimeout how long a thread may hold a contestested lock before throwing {@link RetryTransactionException}
     *  in milliseconds, or zero for unlimited
     * @throws IllegalArgumentException if {@code waitTimeout} or {@code holdTimeout} is negative
     */
    public MemoryKVDatabase(long waitTimeout, long holdTimeout) {
        super(new MemoryKVStore(), waitTimeout, holdTimeout);
    }
}
