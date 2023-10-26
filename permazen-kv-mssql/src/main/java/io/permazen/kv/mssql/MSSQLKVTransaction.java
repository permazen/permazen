
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mssql;

import io.permazen.kv.sql.SQLKVDatabase;
import io.permazen.kv.sql.SQLKVTransaction;
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

    @Override
    protected byte[] encodeKey(byte[] key) {

        // Determine if key contains any 0x00 bytes
        final int keyLength = key.length;
        int dbkeyLength = key.length;
        for (int i = 0; i < keyLength; i++) {
            if ((key[i] & 0xfe) == 0)                           // byte is either 0x00 or 0x01
                dbkeyLength++;
        }

        // If not, nothing need be done
        if (dbkeyLength == keyLength)
            return key;

        // Replace all 0x00 and 0x01 bytes with 0x0101 and 0x0102, respectively
        final byte[] dbkey = new byte[dbkeyLength];
        int j = 0;
        for (int i = 0; i < keyLength; i++) {
            byte b = key[i];
            if ((b & 0xfe) == 0) {
                dbkey[j++] = (byte)0x01;
                b++;
            }
            dbkey[j++] = b;
        }
        assert j == dbkeyLength;

        // Done
        return dbkey;
    }

    @Override
    protected byte[] decodeKey(byte[] dbkey) {

        // Determine if dbkey contains any encoded 0x00 or 0x01 bytes
        int keyLength = dbkey.length;
        for (int i = 0; i < dbkey.length; i++) {
            switch (dbkey[i]) {
            case 0x00:
                throw new RuntimeException(
                  String.format("read invalid key [%s]: zero byte at offset %d", ByteUtil.toString(dbkey), i));
            case 0x01:
                if (++i == dbkey.length || (dbkey[i] != 0x01 && dbkey[i] != 0x02)) {
                    throw new RuntimeException(String.format(
                      "read invalid key [%s]: invalid escape sequence at offset %d", ByteUtil.toString(dbkey), i - 1));
                }
                keyLength--;
                break;
            default:
                break;
            }
        }

        // If not, nothing need be done
        if (keyLength == dbkey.length)
            return dbkey;

        // Replace all 0x0101 and 0x0102 byte pairs with 0x00 and 0x01, respectively
        final byte[] key = new byte[keyLength];
        int j = 0;
        for (int i = 0; i < dbkey.length; i++, j++) {
            if (dbkey[i] != 0x01)
                key[j] = dbkey[i];
            else if (dbkey[++i] == 0x02)
                key[j] = (byte)0x01;
        }
        assert j == key.length;

        // Done
        return key;
    }
}
