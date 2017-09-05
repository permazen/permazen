
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.cockroach;

import io.permazen.kv.sql.SQLKVDatabase;
import io.permazen.kv.sql.SQLKVTransaction;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * {@link KVTransaction} implementation based on CockroachDB.
 */
class CockroachKVTransaction extends SQLKVTransaction {

    CockroachKVTransaction(SQLKVDatabase database, Connection connection) throws SQLException {
        super(database, connection);
    }
}

