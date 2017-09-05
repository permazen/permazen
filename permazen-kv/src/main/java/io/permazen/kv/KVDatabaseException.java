
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

/**
 * Superclass of all unchecked exceptions thrown by a {@link KVDatabase}.
 */
@SuppressWarnings("serial")
public class KVDatabaseException extends KVException {

    private final KVDatabase store;

    public KVDatabaseException(KVDatabase store) {
        this.store = store;
    }

    public KVDatabaseException(KVDatabase store, String message) {
        super(message);
        this.store = store;
    }

    public KVDatabaseException(KVDatabase store, Throwable cause) {
        super(cause);
        this.store = store;
    }

    public KVDatabaseException(KVDatabase store, String message, Throwable cause) {
        super(message, cause);
        this.store = store;
    }

    /**
     * Get the {@link KVDatabase} that generated this exception.
     *
     * @return associated database
     */
    public KVDatabase getStore() {
        return this.store;
    }
}

