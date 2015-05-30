
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv;

/**
 * A transactional database with a simple key/value API.
 *
 * @see KVTransaction
 */
public interface KVDatabase {

    /**
     * Create a new transaction.
     *
     * @return newly created transaction
     * @throws KVDatabaseException if an unexpected error occurs
     */
    KVTransaction createTransaction();
}

