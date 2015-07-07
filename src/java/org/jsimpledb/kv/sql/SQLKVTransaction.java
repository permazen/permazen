
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.sql;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;

import org.jsimpledb.kv.AbstractKVStore;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.StaleTransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SQLKVDatabase} transaction.
 */
public class SQLKVTransaction extends AbstractKVStore implements KVTransaction {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    protected final SQLKVDatabase database;
    protected final Connection connection;

    private long timeout;
    private boolean closed;
    private boolean stale;

    /**
     * Constructor.
     *
     * @param database the associated database
     * @param connection the {@link Connection} for the transaction
     * @throws SQLException if an SQL error occurs
     */
    public SQLKVTransaction(SQLKVDatabase database, Connection connection) throws SQLException {
        Preconditions.checkArgument(database != null, "null database");
        Preconditions.checkArgument(connection != null, "null connection");
        this.database = database;
        this.connection = connection;
    }

    @Override
    public SQLKVDatabase getKVDatabase() {
        return this.database;
    }

    @Override
    public void setTimeout(long timeout) {
        Preconditions.checkArgument(timeout >= 0, "timeout < 0");
        this.timeout = timeout;
    }

    /**
     * Watch a key to monitor for changes in its value.
     *
     * <p>
     * The implementation in {@link SQLKVTransaction} always throws {@link UnsupportedOperationException}.
     * Subclasses may add support using a database-specific notification mechanism.
     *
     * @param key {@inheritDoc}
     * @return {@inheritDoc}
     * @throws StaleTransactionException {@inheritDoc}
     * @throws org.jsimpledb.kv.RetryTransactionException {@inheritDoc}
     * @throws org.jsimpledb.kv.KVDatabaseException {@inheritDoc}
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    @Override
    public Future<Void> watchKey(byte[] key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized byte[] get(byte[] key) {
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(key != null, "null key");
        return this.queryBytes(StmtType.GET, key);
    }

    @Override
    public synchronized KVPair getAtLeast(byte[] minKey) {
        if (this.stale)
            throw new StaleTransactionException(this);
        return minKey != null ?
          this.queryKVPair(StmtType.GET_AT_LEAST_SINGLE, minKey) : this.queryKVPair(StmtType.GET_FIRST);
    }

    @Override
    public synchronized KVPair getAtMost(byte[] maxKey) {
        if (this.stale)
            throw new StaleTransactionException(this);
        return maxKey != null ?
          this.queryKVPair(StmtType.GET_AT_MOST_SINGLE, maxKey) : this.queryKVPair(StmtType.GET_LAST);
    }

    @Override
    public synchronized Iterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (minKey == null && maxKey == null)
            return this.queryIterator(reverse ? StmtType.GET_ALL_REVERSE : StmtType.GET_ALL_FORWARD);
        if (minKey == null)
            return this.queryIterator(reverse ? StmtType.GET_AT_MOST_REVERSE : StmtType.GET_AT_MOST_FORWARD, maxKey);
        if (maxKey == null)
            return this.queryIterator(reverse ? StmtType.GET_AT_LEAST_REVERSE : StmtType.GET_AT_LEAST_FORWARD, minKey);
        else
            return this.queryIterator(reverse ? StmtType.GET_RANGE_REVERSE : StmtType.GET_RANGE_FORWARD, minKey, maxKey);
    }

    @Override
    public synchronized void put(byte[] key, byte[] value) {
        Preconditions.checkArgument(key != null, "null key");
        Preconditions.checkArgument(value != null, "null value");
        if (this.stale)
            throw new StaleTransactionException(this);
        this.update(StmtType.PUT, key, value, value);
    }

    @Override
    public synchronized void remove(byte[] key) {
        Preconditions.checkArgument(key != null, "null key");
        if (this.stale)
            throw new StaleTransactionException(this);
        this.update(StmtType.REMOVE, key);
    }

    @Override
    public synchronized void removeRange(byte[] minKey, byte[] maxKey) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (minKey == null && maxKey == null)
            this.update(StmtType.REMOVE_ALL);
        else if (minKey == null)
            this.update(StmtType.REMOVE_AT_MOST, maxKey);
        else if (maxKey == null)
            this.update(StmtType.REMOVE_AT_LEAST, minKey);
        else
            this.update(StmtType.REMOVE_RANGE, minKey, maxKey);
    }

    @Override
    public synchronized void commit() {
        if (this.stale)
            throw new StaleTransactionException(this);
        this.stale = true;
        try {
            this.connection.commit();
        } catch (SQLException e) {
            throw this.handleException(e);
        } finally {
            this.closeConnection();
        }
    }

    @Override
    public synchronized void rollback() {
        if (this.stale)
            return;
        this.stale = true;
        try {
            this.connection.rollback();
        } catch (SQLException e) {
            throw this.handleException(e);
        } finally {
            this.closeConnection();
        }
    }

    /**
     * Handle an unexpected SQL exception.
     *
     * <p>
     * The implementation in {@link SQLKVTransaction} rolls back the SQL transaction, closes the associated {@link Connection},
     * and wraps the exception via {@link SQLKVDatabase#wrapException SQLKVDatabase.wrapException()}.
     * </p>
     *
     * @param e original exception
     * @return key/value transaction exception
     */
    protected KVTransactionException handleException(SQLException e) {
        this.stale = true;
        try {
            this.connection.rollback();
        } catch (SQLException e2) {
            // ignore
        } finally {
            this.closeConnection();
        }
        return this.database.wrapException(this, e);
    }

    /**
     * Close the {@link Connection} associated with this instance, if it's not already closed.
     * This method is idempotent.
     */
    protected void closeConnection() {
        if (this.closed)
            return;
        this.closed = true;
        try {
            this.connection.close();
        } catch (SQLException e) {
            // ignore
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            if (!this.stale)
               this.log.warn(this + " leaked without commit() or rollback()");
            this.closeConnection();
        } finally {
            super.finalize();
        }
    }

// Helper methods

    private byte[] queryBytes(StmtType stmtType, byte[]... params) {
        return this.query(stmtType, new ResultSetFunction<byte[]>() {
            @Override
            public byte[] apply(PreparedStatement preparedStatement, ResultSet resultSet) throws SQLException {
                return resultSet.next() ? resultSet.getBytes(1) : null;
            }
        }, true, params);
    }

    private KVPair queryKVPair(StmtType stmtType, byte[]... params) {
        return this.query(stmtType, new ResultSetFunction<KVPair>() {
            @Override
            public KVPair apply(PreparedStatement preparedStatement, ResultSet resultSet) throws SQLException {
                return resultSet.next() ? new KVPair(resultSet.getBytes(1), resultSet.getBytes(2)) : null;
            }
        }, true, params);
    }

    private Iterator<KVPair> queryIterator(StmtType stmtType, byte[]... params) {
        return this.query(stmtType, new ResultSetFunction<Iterator<KVPair>>() {
            @Override
            public Iterator<KVPair> apply(PreparedStatement preparedStatement, ResultSet resultSet) throws SQLException {
                return new ResultSetIterator(preparedStatement, resultSet);
            }
        }, false, params);
    }

    private <T> T query(StmtType stmtType, ResultSetFunction<T> resultSetFunction, boolean close, byte[]... params) {
        try {
            final PreparedStatement preparedStatement = stmtType.create(this.database, this.connection);
            for (int i = 0; i < params.length; i++)
                preparedStatement.setBytes(i + 1, params[i]);
            preparedStatement.setQueryTimeout((int)((this.timeout + 999) / 1000));
            if (this.log.isTraceEnabled())
                this.log.trace("SQL query: " + preparedStatement);
            final ResultSet resultSet = preparedStatement.executeQuery();
            final T result = resultSetFunction.apply(preparedStatement, resultSet);
            if (close) {
                resultSet.close();
                preparedStatement.close();
            }
            return result;
        } catch (SQLException e) {
            throw this.handleException(e);
        }
    }

    private void update(StmtType stmtType, byte[]... params) {
        try {
            final PreparedStatement preparedStatement = stmtType.create(this.database, this.connection);
            for (int i = 0; i < params.length; i++)
                preparedStatement.setBytes(i + 1, params[i]);
            preparedStatement.setQueryTimeout((int)((this.timeout + 999) / 1000));
            if (this.log.isTraceEnabled())
                this.log.trace("SQL update: " + preparedStatement);
            preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (SQLException e) {
            throw this.handleException(e);
        }
    }

// ResultSetFunction

    private interface ResultSetFunction<T> {

        T apply(PreparedStatement preparedStatement, ResultSet resultSet) throws SQLException;
    }

// ResultSetIterator

    private class ResultSetIterator implements Iterator<KVPair>, Closeable {

        private final PreparedStatement preparedStatement;
        private final ResultSet resultSet;

        private boolean ready;
        private boolean closed;
        private byte[] removeKey;

        ResultSetIterator(PreparedStatement preparedStatement, ResultSet resultSet) {
            assert preparedStatement != null;
            assert resultSet != null;
            this.resultSet = resultSet;
            this.preparedStatement = preparedStatement;
        }

    // Iterator

        @Override
        public synchronized boolean hasNext() {
            if (this.closed)
                return false;
            if (this.ready)
                return true;
            try {
                this.ready = this.resultSet.next();
            } catch (SQLException e) {
                throw SQLKVTransaction.this.handleException(e);
            }
            if (!this.ready)
                this.close();
            return this.ready;
        }

        @Override
        public synchronized KVPair next() {
            if (!this.hasNext())
                throw new NoSuchElementException();
            final byte[] key;
            final byte[] value;
            try {
                key = this.resultSet.getBytes(1);
                value = this.resultSet.getBytes(2);
            } catch (SQLException e) {
                throw SQLKVTransaction.this.handleException(e);
            }
            this.removeKey = key.clone();
            this.ready = false;
            return new KVPair(key, value);
        }

        @Override
        public synchronized void remove() {
            if (this.closed || this.removeKey == null)
                throw new IllegalStateException();
            SQLKVTransaction.this.remove(this.removeKey);
            this.removeKey = null;
        }

    // Closeable

        @Override
        public synchronized void close() {
            if (this.closed)
                return;
            this.closed = true;
            try {
                this.resultSet.close();
            } catch (Exception e) {
                // ignore
            }
            try {
                this.preparedStatement.close();
            } catch (Exception e) {
                // ignore
            }
        }

    // Object

        @Override
        protected void finalize() throws Throwable {
            try {
                this.close();
            } finally {
                super.finalize();
            }
        }
    }

// StmtType

    abstract static class StmtType {

        static final StmtType GET = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.createGetStatement());
            };
        };
        static final StmtType GET_AT_LEAST_SINGLE = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.limitSingleRow(db.createGetAtLeastStatement(false)));
            };
        };
        static final StmtType GET_AT_MOST_SINGLE = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.limitSingleRow(db.createGetAtMostStatement(false)));
            };
        };
        static final StmtType GET_FIRST = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.limitSingleRow(db.createGetAllStatement(false)));
            };
        };
        static final StmtType GET_LAST = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.limitSingleRow(db.createGetAllStatement(true)));
            };
        };
        static final StmtType GET_AT_LEAST_FORWARD = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.createGetAtLeastStatement(false));
            };
        };
        static final StmtType GET_AT_LEAST_REVERSE = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.createGetAtLeastStatement(true));
            };
        };
        static final StmtType GET_AT_MOST_FORWARD = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.createGetAtMostStatement(false));
            };
        };
        static final StmtType GET_AT_MOST_REVERSE = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.createGetAtMostStatement(true));
            };
        };
        static final StmtType GET_RANGE_FORWARD = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.createGetRangeStatement(false));
            };
        };
        static final StmtType GET_RANGE_REVERSE = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.createGetRangeStatement(true));
            };
        };
        static final StmtType GET_ALL_FORWARD = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.createGetAllStatement(false));
            };
        };
        static final StmtType GET_ALL_REVERSE = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.createGetAllStatement(true));
            };
        };
        static final StmtType PUT = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.createPutStatement());
            };
        };
        static final StmtType REMOVE = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.createRemoveStatement());
            };
        };
        static final StmtType REMOVE_RANGE = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.createRemoveRangeStatement());
            };
        };
        static final StmtType REMOVE_AT_LEAST = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.createRemoveAtLeastStatement());
            };
        };
        static final StmtType REMOVE_AT_MOST = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.createRemoveAtMostStatement());
            };
        };
        static final StmtType REMOVE_ALL = new StmtType() {
            @Override
            PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException {
                return c.prepareStatement(db.createRemoveAllStatement());
            };
        };

        abstract PreparedStatement create(SQLKVDatabase db, Connection c) throws SQLException;
    }
}

