
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * MySQL variant of {@link SQLKVDatabase}.
 *
 * <p>
 * An idempotent script for initializing a MySQL database is available at
 * {@code classpath:org/jsimpledb/kv/sql/createTable-mysql.sql}
 * (see also {@link org.dellroad.stuff.schema.UpdatingDataSource}).
 * </p>
 *
 */
public class MySQLKVDatabase extends SQLKVDatabase {

    @Override
    protected Connection createConnection() throws SQLException {
        final Connection connection = super.createConnection();
        final Statement statement = connection.createStatement();
        statement.execute("SET SESSION sql_mode = 'TRADITIONAL'");      // force error if key or value is too long
        return connection;
    }

    @Override
    public String createGetAtMostStatement() {
        return super.createGetAtMostStatement() + " LIMIT 1";
    }

    @Override
    public String createGetFirstStatement() {
        return super.createGetFirstStatement() + " LIMIT 1";
    }

    @Override
    public String createGetAtLeastStatement() {
        return super.createGetAtLeastStatement() + " LIMIT 1";
    }

    @Override
    public String createGetLastStatement() {
        return super.createGetLastStatement() + " LIMIT 1";
    }

    /**
     * Encloses the given {@code name} in backticks.
     */
    @Override
    public String quote(String name) {
        return "`" + name + "`";
    }
}

