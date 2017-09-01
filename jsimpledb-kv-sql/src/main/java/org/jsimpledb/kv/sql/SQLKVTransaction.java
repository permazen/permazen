
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.sql;

import com.google.common.base.Preconditions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;

import org.jsimpledb.kv.AbstractKVStore;
import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KVTransactionException;
import org.jsimpledb.kv.StaleTransactionException;
import org.jsimpledb.kv.mvcc.MutableView;
import org.jsimpledb.kv.util.ForwardingKVStore;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SQLKVDatabase} transaction.
 */
public class SQLKVTransaction extends ForwardingKVStore implements KVTransaction {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    protected final SQLKVDatabase database;
    protected final Connection connection;

    private long timeout;
    private boolean readOnly;
    private KVStore view;
    private volatile boolean mutated;
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

    private synchronized byte[] getSQL(byte[] key) {
        if (this.stale)
            throw new StaleTransactionException(this);
        Preconditions.checkArgument(key != null, "null key");
        return this.queryBytes(StmtType.GET, this.encodeKey(key));
    }

    private synchronized KVPair getAtLeastSQL(byte[] minKey, byte[] maxKey) {
        if (this.stale)
            throw new StaleTransactionException(this);
        return minKey != null && minKey.length > 0 ?
          (maxKey != null ?
           this.queryKVPair(StmtType.GET_RANGE_FORWARD_SINGLE, this.encodeKey(minKey), this.encodeKey(maxKey)) :
           this.queryKVPair(StmtType.GET_AT_LEAST_FORWARD_SINGLE, this.encodeKey(minKey))) :
          (maxKey != null ?
           this.queryKVPair(StmtType.GET_AT_MOST_FORWARD_SINGLE, this.encodeKey(maxKey)) :
           this.queryKVPair(StmtType.GET_FIRST));
    }

    private synchronized KVPair getAtMostSQL(byte[] maxKey, byte[] minKey) {
        if (this.stale)
            throw new StaleTransactionException(this);
        return maxKey != null ?
          (minKey != null && minKey.length > 0 ?
           this.queryKVPair(StmtType.GET_RANGE_REVERSE_SINGLE, this.encodeKey(minKey), this.encodeKey(maxKey)) :
           this.queryKVPair(StmtType.GET_AT_MOST_REVERSE_SINGLE, this.encodeKey(maxKey))) :
          (minKey != null && minKey.length > 0 ?
           this.queryKVPair(StmtType.GET_AT_LEAST_REVERSE_SINGLE, this.encodeKey(minKey)) :
           this.queryKVPair(StmtType.GET_LAST));
    }

    private synchronized CloseableIterator<KVPair> getRangeSQL(byte[] minKey, byte[] maxKey, boolean reverse) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (minKey != null && minKey.length == 0)
            minKey = null;
        if (minKey == null && maxKey == null)
            return this.queryIterator(reverse ? StmtType.GET_ALL_REVERSE : StmtType.GET_ALL_FORWARD);
        if (minKey == null) {
            return this.queryIterator(reverse ?
              StmtType.GET_AT_MOST_REVERSE : StmtType.GET_AT_MOST_FORWARD, this.encodeKey(maxKey));
        }
        if (maxKey == null) {
            return this.queryIterator(reverse ?
              StmtType.GET_AT_LEAST_REVERSE : StmtType.GET_AT_LEAST_FORWARD, this.encodeKey(minKey));
        } else {
            return this.queryIterator(reverse ?
              StmtType.GET_RANGE_REVERSE : StmtType.GET_RANGE_FORWARD, this.encodeKey(minKey), this.encodeKey(maxKey));
        }
    }

    private synchronized void putSQL(byte[] key, byte[] value) {
        Preconditions.checkArgument(key != null, "null key");
        Preconditions.checkArgument(value != null, "null value");
        if (this.stale)
            throw new StaleTransactionException(this);
        this.update(StmtType.PUT, this.encodeKey(key), value, value);
    }

    private synchronized void removeSQL(byte[] key) {
        Preconditions.checkArgument(key != null, "null key");
        if (this.stale)
            throw new StaleTransactionException(this);
        this.update(StmtType.REMOVE, this.encodeKey(key));
    }

    private synchronized void removeRangeSQL(byte[] minKey, byte[] maxKey) {
        if (this.stale)
            throw new StaleTransactionException(this);
        if (minKey != null && minKey.length == 0)
            minKey = null;
        if (minKey == null && maxKey == null)
            this.update(StmtType.REMOVE_ALL);
        else if (minKey == null)
            this.update(StmtType.REMOVE_AT_MOST, this.encodeKey(maxKey));
        else if (maxKey == null)
            this.update(StmtType.REMOVE_AT_LEAST, this.encodeKey(minKey));
        else
            this.update(StmtType.REMOVE_RANGE, this.encodeKey(minKey), this.encodeKey(maxKey));
    }

    @Override
    public synchronized boolean isReadOnly() {
        return this.readOnly;
    }

    @Override
    public synchronized void commit() {
        if (this.stale)
            throw new StaleTransactionException(this);
        this.stale = true;
        try {
            if (this.readOnly && !(this.view instanceof MutableView))
                this.connection.rollback();
            else
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

    @Override
    public CloseableKVStore mutableSnapshot() {
        throw new UnsupportedOperationException();
    }

    /**
     * Handle an unexpected SQL exception.
     *
     * <p>
     * The implementation in {@link SQLKVTransaction} rolls back the SQL transaction, closes the associated {@link Connection},
     * and wraps the exception via {@link SQLKVDatabase#wrapException SQLKVDatabase.wrapException()}.
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

// KVStore

    @Override
    public void put(byte[] key, byte[] value) {
        this.mutated = true;
        this.delegate().put(key, value);
    }

    @Override
    public void remove(byte[] key) {
        this.mutated = true;
        this.delegate().remove(key);
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        this.mutated = true;
        this.delegate().removeRange(minKey, maxKey);
    }

    @Override
    protected synchronized KVStore delegate() {
        if (this.view == null)
            this.view = new SQLView();
        return this.view;
    }

    @Override
    public synchronized void setReadOnly(boolean readOnly) {
        if (readOnly == this.readOnly)
            return;
        Preconditions.checkArgument(readOnly, "read-only transaction cannot be made writable again");
        Preconditions.checkState(!this.mutated || this.database.rollbackForReadOnly, "data is already mutated");
        if (!this.database.rollbackForReadOnly)
            this.view = new MutableView(this.view);
        this.readOnly = readOnly;
    }

// Helper methods

    protected byte[] queryBytes(StmtType stmtType, byte[]... params) {
        return this.query(stmtType, (stmt, rs) -> rs.next() ? rs.getBytes(1) : null, true, params);
    }

    protected KVPair queryKVPair(StmtType stmtType, byte[]... params) {
        return this.query(stmtType, (stmt, rs) -> rs.next() ? new KVPair(this.decodeKey(rs.getBytes(1)), rs.getBytes(2)) : null,
          true, params);
    }

    protected CloseableIterator<KVPair> queryIterator(StmtType stmtType, byte[]... params) {
        return this.query(stmtType, ResultSetIterator::new, false, params);
    }

    protected <T> T query(StmtType stmtType, ResultSetFunction<T> resultSetFunction, boolean close, byte[]... params) {
        try {
            final PreparedStatement preparedStatement = stmtType.create(this.database, this.connection, this.log);
            final int numParams = preparedStatement.getParameterMetaData().getParameterCount();
            for (int i = 0; i < params.length && i < numParams; i++) {
                if (this.log.isTraceEnabled())
                    this.log.trace("setting ?" + (i + 1) + " = " + ByteUtil.toString(params[i]));
                preparedStatement.setBytes(i + 1, params[i]);
            }
            preparedStatement.setQueryTimeout((int)((this.timeout + 999) / 1000));
            if (this.log.isTraceEnabled())
                this.log.trace("executing SQL query");
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

    protected void update(StmtType stmtType, byte[]... params) {
        try (final PreparedStatement preparedStatement = stmtType.create(this.database, this.connection, this.log)) {
            final int numParams = preparedStatement.getParameterMetaData().getParameterCount();
            for (int i = 0; i < params.length && i < numParams; i++) {
                if (this.log.isTraceEnabled())
                    this.log.trace("setting ?" + (i + 1) + " = " + ByteUtil.toString(params[i]));
                preparedStatement.setBytes(i + 1, params[i]);
            }
            preparedStatement.setQueryTimeout((int)((this.timeout + 999) / 1000));
            if (this.log.isTraceEnabled())
                this.log.trace("executing SQL update");
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw this.handleException(e);
        }
    }

    /**
     * Encode the given key for the underlying database key column.
     *
     * <p>
     * The implementation in {@link SQLKVTransaction} just returns {@code key}.
     *
     * @param key key
     * @return database value for key column
     * @see #decodeKey decodeKey()
     */
    protected byte[] encodeKey(byte[] key) {
        return key;
    }

    /**
     * Decode the given database key value encoded by {@link #encodeKey encodeKey()}.
     *
     * <p>
     * The implementation in {@link SQLKVTransaction} just returns {@code dbkey}.
     *
     * @param dbkey database value for key column
     * @return key
     * @see #encodeKey encodeKey()
     */

    protected byte[] decodeKey(byte[] dbkey) {
        return dbkey;
    }

// SQLView

    private class SQLView extends AbstractKVStore {

        @Override
        public byte[] get(byte[] key) {
            return SQLKVTransaction.this.getSQL(key);
        }

        @Override
        public KVPair getAtLeast(byte[] minKey, byte[] maxKey) {
            return SQLKVTransaction.this.getAtLeastSQL(minKey, maxKey);
        }

        @Override
        public KVPair getAtMost(byte[] maxKey, byte[] minKey) {
            return SQLKVTransaction.this.getAtMostSQL(maxKey, minKey);
        }

        @Override
        public CloseableIterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
            return SQLKVTransaction.this.getRangeSQL(minKey, maxKey, reverse);
        }

        @Override
        public void put(byte[] key, byte[] value) {
            SQLKVTransaction.this.putSQL(key, value);
        }

        @Override
        public void remove(byte[] key) {
            SQLKVTransaction.this.removeSQL(key);
        }

        @Override
        public void removeRange(byte[] minKey, byte[] maxKey) {
            SQLKVTransaction.this.removeRangeSQL(minKey, maxKey);
        }
    }

// ResultSetFunction

    private interface ResultSetFunction<T> {

        T apply(PreparedStatement preparedStatement, ResultSet resultSet) throws SQLException;
    }

// ResultSetIterator

    private class ResultSetIterator implements CloseableIterator<KVPair> {

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
                key = SQLKVTransaction.this.decodeKey(this.resultSet.getBytes(1));
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

    /**
     * Used internally to build SQL statements.
     */
    protected enum StmtType {

        GET {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetStatement(), log);
            };
        },
        GET_FIRST {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.limitSingleRow(db.createGetAllStatement(false)), log);
            };
        },
        GET_LAST {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.limitSingleRow(db.createGetAllStatement(true)), log);
            };
        },
        GET_AT_LEAST_FORWARD {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetAtLeastStatement(false), log);
            };
        },
        GET_AT_LEAST_FORWARD_SINGLE {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.limitSingleRow(db.createGetAtLeastStatement(false)), log);
            };
        },
        GET_AT_LEAST_REVERSE {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetAtLeastStatement(true), log);
            };
        },
        GET_AT_LEAST_REVERSE_SINGLE {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.limitSingleRow(db.createGetAtLeastStatement(true)), log);
            };
        },
        GET_AT_MOST_FORWARD {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetAtMostStatement(false), log);
            };
        },
        GET_AT_MOST_FORWARD_SINGLE {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.limitSingleRow(db.createGetAtMostStatement(false)), log);
            };
        },
        GET_AT_MOST_REVERSE {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetAtMostStatement(true), log);
            };
        },
        GET_AT_MOST_REVERSE_SINGLE {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.limitSingleRow(db.createGetAtMostStatement(true)), log);
            };
        },
        GET_RANGE_FORWARD {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetRangeStatement(false), log);
            };
        },
        GET_RANGE_FORWARD_SINGLE {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.limitSingleRow(db.createGetRangeStatement(false)), log);
            };
        },
        GET_RANGE_REVERSE {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetRangeStatement(true), log);
            };
        },
        GET_RANGE_REVERSE_SINGLE {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.limitSingleRow(db.createGetRangeStatement(true)), log);
            };
        },
        GET_ALL_FORWARD {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetAllStatement(false), log);
            };
        },
        GET_ALL_REVERSE {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetAllStatement(true), log);
            };
        },
        PUT {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createPutStatement(), log);
            };
        },
        REMOVE {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createRemoveStatement(), log);
            };
        },
        REMOVE_RANGE {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createRemoveRangeStatement(), log);
            };
        },
        REMOVE_AT_LEAST {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createRemoveAtLeastStatement(), log);
            };
        },
        REMOVE_AT_MOST {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createRemoveAtMostStatement(), log);
            };
        },
        REMOVE_ALL {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createRemoveAllStatement(), log);
            };
        };

        protected abstract PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException;

        protected PreparedStatement prepare(Connection c, String sql, Logger log) throws SQLException {
            if (log.isTraceEnabled())
                log.trace("preparing SQL statement: " + sql);
            return c.prepareStatement(sql);
        }
    }
}

