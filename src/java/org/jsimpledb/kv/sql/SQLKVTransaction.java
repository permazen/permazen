
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.StaleTransactionException;

/**
 * {@link SQLKVDatabase} transaction.
 */
public class SQLKVTransaction implements KVTransaction {

    protected final SQLKVDatabase database;
    protected final Connection connection;

    private PreparedStatement getStatement;
    private PreparedStatement getAtLeastStatement;
    private PreparedStatement getFirstStatement;
    private PreparedStatement getAtMostStatement;
    private PreparedStatement getLastStatement;
    private PreparedStatement putStatement;
    private PreparedStatement removeStatement;
    private PreparedStatement removeRangeStatement;
    private PreparedStatement removeAtLeastStatement;
    private PreparedStatement removeAtMostStatement;
    private PreparedStatement removeAllStatement;

    private boolean stale;

    /**
     * Constructor.
     */
    public SQLKVTransaction(SQLKVDatabase database, Connection connection) throws SQLException {
        if (database == null)
            throw new IllegalArgumentException("null database");
        if (connection == null)
            throw new IllegalArgumentException("null connection");
        this.database = database;
        this.connection = connection;
    }

    @Override
    public SQLKVDatabase getKVDatabase() {
        return this.database;
    }

    /**
     * Set transaction timeout. This method is not supported by {@link SQLKVTransaction}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void setTimeout(long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized byte[] get(byte[] key) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (key == null)
            throw new IllegalArgumentException("null key");
        try {
            if (this.getStatement == null)
                this.getStatement = this.connection.prepareStatement(this.database.createGetStatement());
            this.getStatement.setBytes(1, key);
            final ResultSet rs = this.getStatement.executeQuery();
            final byte[] value = rs.next() ? rs.getBytes(1) : null;
            rs.close();
            return value;
        } catch (SQLException e) {
            throw this.database.wrapException(this, e);
        }
    }

    @Override
    public synchronized KVPair getAtLeast(byte[] minKey) {
        if (this.stale)
            throw new StaleTransactionException(this);
        try {
            final ResultSet rs;
            if (minKey != null) {
                if (this.getAtLeastStatement == null)
                    this.getAtLeastStatement = this.connection.prepareStatement(this.database.createGetAtLeastStatement());
                this.getAtLeastStatement.setBytes(1, minKey);
                rs = this.getAtLeastStatement.executeQuery();
            } else {
                if (this.getFirstStatement == null)
                    this.getFirstStatement = this.connection.prepareStatement(this.database.createGetFirstStatement());
                rs = this.getFirstStatement.executeQuery();
            }
            final KVPair pair = rs.next() ? new KVPair(rs.getBytes(1), rs.getBytes(2)) : null;
            rs.close();
            return pair;
        } catch (SQLException e) {
            throw this.database.wrapException(this, e);
        }
    }

    @Override
    public synchronized KVPair getAtMost(byte[] maxKey) {
        if (this.stale)
            throw new StaleTransactionException(this);
        try {
            final ResultSet rs;
            if (maxKey != null) {
                if (this.getAtMostStatement == null)
                    this.getAtMostStatement = this.connection.prepareStatement(this.database.createGetAtMostStatement());
                this.getAtMostStatement.setBytes(1, maxKey);
                rs = this.getAtMostStatement.executeQuery();
            } else {
                if (this.getLastStatement == null)
                    this.getLastStatement = this.connection.prepareStatement(this.database.createGetLastStatement());
                rs = this.getLastStatement.executeQuery();
            }
            final KVPair pair = rs.next() ? new KVPair(rs.getBytes(1), rs.getBytes(2)) : null;
            rs.close();
            return pair;
        } catch (SQLException e) {
            throw this.database.wrapException(this, e);
        }
    }

    @Override
    public synchronized void put(byte[] key, byte[] value) {
        if (key == null)
            throw new IllegalArgumentException("null key");
        if (value == null)
            throw new IllegalArgumentException("null value");
        if (this.stale)
            throw new StaleTransactionException(this);
        try {
            if (this.putStatement == null)
                this.putStatement = this.connection.prepareStatement(this.database.createPutStatement());
            this.putStatement.setBytes(1, key);
            this.putStatement.setBytes(2, value);
            this.putStatement.executeUpdate();
        } catch (SQLException e) {
            throw this.database.wrapException(this, e);
        }
    }

    @Override
    public synchronized void remove(byte[] key) {
        if (key == null)
            throw new IllegalArgumentException("null key");
        if (this.stale)
            throw new StaleTransactionException(this);
        try {
            if (this.removeStatement == null)
                this.removeStatement = this.connection.prepareStatement(this.database.createRemoveStatement());
            this.removeStatement.setBytes(1, key);
            this.removeStatement.executeUpdate();
        } catch (SQLException e) {
            throw this.database.wrapException(this, e);
        }
    }

    @Override
    public synchronized void removeRange(byte[] minKey, byte[] maxKey) {
        if (this.stale)
            throw new StaleTransactionException(this);
        try {
            if (minKey != null && maxKey != null) {
                if (this.removeRangeStatement == null)
                    this.removeRangeStatement = this.connection.prepareStatement(this.database.createRemoveRangeStatement());
                this.removeRangeStatement.setBytes(1, minKey);
                this.removeRangeStatement.setBytes(2, maxKey);
                this.removeRangeStatement.executeUpdate();
            } else if (minKey != null) {
                if (this.removeAtLeastStatement == null)
                    this.removeAtLeastStatement = this.connection.prepareStatement(this.database.createRemoveAtLeastStatement());
                this.removeAtLeastStatement.setBytes(1, minKey);
                this.removeAtLeastStatement.executeUpdate();
            } else if (minKey == null) {
                if (this.removeAtMostStatement == null)
                    this.removeAtMostStatement = this.connection.prepareStatement(this.database.createRemoveAtMostStatement());
                this.removeAtMostStatement.setBytes(1, maxKey);
                this.removeAtMostStatement.executeUpdate();
            } else {
                if (this.removeAllStatement == null)
                    this.removeAllStatement = this.connection.prepareStatement(this.database.createRemoveAllStatement());
                this.removeAllStatement.executeUpdate();
            }
        } catch (SQLException e) {
            throw this.database.wrapException(this, e);
        }
    }

    @Override
    public synchronized void commit() {
        if (this.stale)
            throw new StaleTransactionException(this);
        this.stale = true;
        try {
            this.connection.commit();
        } catch (SQLException e) {
            throw this.database.wrapException(this, e);
        }
    }

    @Override
    public synchronized void rollback() {
        if (this.stale)
            throw new StaleTransactionException(this);
        this.stale = true;
        try {
            this.connection.rollback();
        } catch (SQLException e) {
            throw this.database.wrapException(this, e);
        }
    }
}

