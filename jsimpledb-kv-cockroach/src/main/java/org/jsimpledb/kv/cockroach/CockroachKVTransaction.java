
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.cockroach;

import java.sql.Connection;
import java.sql.SQLException;

import org.jsimpledb.kv.sql.SQLKVDatabase;
import org.jsimpledb.kv.sql.SQLKVTransaction;

/**
 * {@link KVTransaction} implementation based on CockroachDB.
 */
class CockroachKVTransaction extends SQLKVTransaction {

    CockroachKVTransaction(SQLKVDatabase database, Connection connection) throws SQLException {
        super(database, connection);
    }
}

