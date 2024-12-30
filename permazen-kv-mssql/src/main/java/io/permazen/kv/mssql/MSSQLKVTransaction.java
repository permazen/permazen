
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mssql;

import io.permazen.kv.KVDatabaseException;
import io.permazen.kv.sql.SQLKVDatabase;
import io.permazen.kv.sql.SQLKVTransaction;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

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
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("SET LOCK_TIMEOUT " + timeout);
        } catch (SQLException e) {
            throw this.handleException(e);
        }
    }

    // See SQLKVDatabase.createPutStatement()
    @Override
    protected void update(StmtType stmtType, ByteData... params) {
        if (StmtType.PUT.equals(stmtType)) {
            final ByteData[] swizzledParams = new ByteData[4];
            swizzledParams[0] = params[1];
            swizzledParams[1] = params[0];
            swizzledParams[2] = params[0];
            swizzledParams[3] = params[1];
            params = swizzledParams;
        }
        super.update(stmtType, params);
    }

    @Override
    protected ByteData encodeKey(ByteData key) {

        // Replace all 0x00 and 0x01 bytes with 0x0101 and 0x0102, respectively
        final ByteData.Writer dbkey = ByteData.newWriter(key.size() + 16);
        key.stream().forEach(value -> {
            if ((value & 0xfe) == 0) {
                dbkey.write(0x01);
                value++;
            }
            dbkey.write(value);
        });
        if (dbkey.size() == key.size())         // no 0x00 or 0x01 bytes found, so just return the key
            return key;

        // Return the encoded key
        return dbkey.toByteData();
    }

    @Override
    protected ByteData decodeKey(ByteData dbkey) {
        final ByteData.Writer key = ByteData.newWriter(dbkey.size());
        for (int i = 0; i < dbkey.size(); i++) {
            int value = dbkey.ubyteAt(i);
            switch (value) {
            case 0x00:
                throw new KVDatabaseException(this.database,
                  String.format("read invalid key [%s]: zero byte at offset %d", ByteUtil.toString(dbkey), i));
            case 0x01:
                if (++i == dbkey.size() || ((value = dbkey.ubyteAt(i)) != 0x01 && value != 0x02)) {
                    throw new KVDatabaseException(this.database,
                      String.format("read invalid key [%s]: invalid escape sequence 0x01 0x%02x at offset %d",
                      ByteUtil.toString(dbkey), value, i - 1));
                }
                value--;
                break;
            default:
                break;
            }
            key.write(value);
        }
        if (key.size() == dbkey.size())         // no encoded bytes found, so just return the dbkey
            return dbkey;

        // Done
        return key.toByteData();
    }
}
