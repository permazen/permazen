
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.mssql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.jsimpledb.kv.sql.SQLKVDatabase;
import org.jsimpledb.kv.sql.SQLKVTransaction;

/**
 * Microsoft SQL Server variant of {@link SQLKVTransaction}.
 */
class MSSQLKVTransaction extends SQLKVTransaction {

    MSSQLKVTransaction(SQLKVDatabase database, Connection connection) throws SQLException {
        super(database, connection);
    }

    @Override
    public void setTimeout(long timeout) {
        super.setTimeout(timeout);
        try (final Statement statement = this.connection.createStatement()) {
            statement.execute("SET LOCK_TIMEOUT " + timeout);
        } catch (SQLException e) {
            throw this.handleException(e);
        }
    }

    // See SQLKVDatabase.createPutStatement()
    @Override
    protected void update(StmtType stmtType, byte[]... params) {
        if (StmtType.PUT.equals(stmtType)) {
            final byte[][] swizzledParams = new byte[4][];
            swizzledParams[0] = params[1];
            swizzledParams[1] = params[0];
            swizzledParams[2] = params[0];
            swizzledParams[3] = params[1];
            params = swizzledParams;
        }
        super.update(stmtType, params);
    }
}

