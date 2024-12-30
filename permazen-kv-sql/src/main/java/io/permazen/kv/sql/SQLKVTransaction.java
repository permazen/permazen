
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.sql;

import com.google.common.base.Preconditions;

import io.permazen.kv.AbstractKVStore;
import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KVTransaction;
import io.permazen.kv.KVTransactionException;
import io.permazen.kv.KeyRange;
import io.permazen.kv.StaleKVTransactionException;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.mvcc.Mutations;
import io.permazen.kv.util.ForwardingKVStore;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SQLKVDatabase} transaction.
 */
public class SQLKVTransaction extends ForwardingKVStore implements KVTransaction {

    private static final int MAX_DATA_PER_BATCH = 10 * 1024 * 1024;     // 10 MB
    private static final int MAX_STATEMENTS_PER_BATCH = 1000;
    private static final int BATCH_STATEMENT_OVERHEAD = 8;              // just a guess

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
     * @throws StaleKVTransactionException {@inheritDoc}
     * @throws io.permazen.kv.RetryKVTransactionException {@inheritDoc}
     * @throws io.permazen.kv.KVDatabaseException {@inheritDoc}
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    @Override
    public Future<Void> watchKey(ByteData key) {
        throw new UnsupportedOperationException();
    }

    private synchronized ByteData getSQL(ByteData key) {
        if (this.stale)
            throw new StaleKVTransactionException(this);
        Preconditions.checkArgument(key != null, "null key");
        return this.queryBytes(StmtType.GET, this.encodeKey(key));
    }

    private synchronized KVPair getAtLeastSQL(ByteData minKey, ByteData maxKey) {
        if (this.stale)
            throw new StaleKVTransactionException(this);
        return minKey != null && !minKey.isEmpty() ?
          (maxKey != null ?
           this.queryKVPair(StmtType.GET_RANGE_FORWARD_SINGLE, this.encodeKey(minKey), this.encodeKey(maxKey)) :
           this.queryKVPair(StmtType.GET_AT_LEAST_FORWARD_SINGLE, this.encodeKey(minKey))) :
          (maxKey != null ?
           this.queryKVPair(StmtType.GET_AT_MOST_FORWARD_SINGLE, this.encodeKey(maxKey)) :
           this.queryKVPair(StmtType.GET_FIRST));
    }

    private synchronized KVPair getAtMostSQL(ByteData maxKey, ByteData minKey) {
        if (this.stale)
            throw new StaleKVTransactionException(this);
        return maxKey != null ?
          (minKey != null && !minKey.isEmpty() ?
           this.queryKVPair(StmtType.GET_RANGE_REVERSE_SINGLE, this.encodeKey(minKey), this.encodeKey(maxKey)) :
           this.queryKVPair(StmtType.GET_AT_MOST_REVERSE_SINGLE, this.encodeKey(maxKey))) :
          (minKey != null && !minKey.isEmpty() ?
           this.queryKVPair(StmtType.GET_AT_LEAST_REVERSE_SINGLE, this.encodeKey(minKey)) :
           this.queryKVPair(StmtType.GET_LAST));
    }

    private synchronized CloseableIterator<KVPair> getRangeSQL(ByteData minKey, ByteData maxKey, boolean reverse) {
        if (this.stale)
            throw new StaleKVTransactionException(this);
        if (minKey != null && minKey.isEmpty())
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

    private synchronized void putSQL(ByteData key, ByteData value) {
        Preconditions.checkArgument(key != null, "null key");
        Preconditions.checkArgument(value != null, "null value");
        if (this.stale)
            throw new StaleKVTransactionException(this);
        this.update(StmtType.PUT, this.encodeKey(key), value, value);
    }

    private synchronized void removeSQL(ByteData key) {
        Preconditions.checkArgument(key != null, "null key");
        if (this.stale)
            throw new StaleKVTransactionException(this);
        this.update(StmtType.REMOVE, this.encodeKey(key));
    }

    private synchronized void removeRangeSQL(ByteData minKey, ByteData maxKey) {
        if (this.stale)
            throw new StaleKVTransactionException(this);
        if (minKey != null && minKey.isEmpty())
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

    private synchronized void applyBatch(Mutations mutations) {
        Preconditions.checkArgument(mutations != null, "null mutations");
        if (this.stale)
            throw new StaleKVTransactionException(this);

        // Do removes
        final EnumMap<StmtType, ArrayList<ByteData>> removeBatchMap = new EnumMap<>(StmtType.class);
        final Function<StmtType, ArrayList<ByteData>> listInit = st -> new ArrayList<>();
        boolean removeAll = false;
        try (Stream<KeyRange> ranges = mutations.getRemoveRanges()) {
            for (Iterator<KeyRange> i = ranges.iterator(); i.hasNext(); ) {
                final KeyRange remove = i.next();
                final ByteData min = remove.getMin();
                final ByteData max = remove.getMax();
                assert min != null;
                if (min.isEmpty() && max == null) {
                    removeAll = true;
                    break;
                }
                if (min.isEmpty())
                    removeBatchMap.computeIfAbsent(StmtType.REMOVE_AT_MOST, listInit).add(this.encodeKey(max));
                else if (max == null)
                    removeBatchMap.computeIfAbsent(StmtType.REMOVE_AT_LEAST, listInit).add(this.encodeKey(min));
                else if (ByteUtil.isConsecutive(min, max))
                    removeBatchMap.computeIfAbsent(StmtType.REMOVE, listInit).add(this.encodeKey(min));
                else {
                    final ArrayList<ByteData> batch = removeBatchMap.computeIfAbsent(StmtType.REMOVE_RANGE, listInit);
                    batch.add(this.encodeKey(min));
                    batch.add(this.encodeKey(max));
                }
            }
        }
        if (removeAll)
            this.update(StmtType.REMOVE_ALL);
        else {
            for (Map.Entry<StmtType, ArrayList<ByteData>> entry : removeBatchMap.entrySet())
                this.updateBatch(entry.getKey(), entry.getValue());
        }

        // Do puts
        final ArrayList<ByteData> putBatch = new ArrayList<>();
        try (Stream<Map.Entry<ByteData, ByteData>> puts = mutations.getPutPairs()) {
            puts.iterator().forEachRemaining(entry -> {
                putBatch.add(this.encodeKey(entry.getKey()));
                putBatch.add(entry.getValue());
                putBatch.add(entry.getValue());
            });
        }
        this.updateBatch(StmtType.PUT, putBatch);

        // Do adjusts
        try (Stream<Map.Entry<ByteData, Long>> adjusts = mutations.getAdjustPairs()) {
            adjusts.iterator().forEachRemaining(entry -> this.adjustCounter(entry.getKey(), entry.getValue()));
        }
    }

    @Override
    public synchronized boolean isReadOnly() {
        return this.readOnly;
    }

    @Override
    public synchronized void commit() {
        if (this.stale)
            throw new StaleKVTransactionException(this);
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
    public CloseableKVStore readOnlySnapshot() {
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
    @SuppressWarnings("deprecation")
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
    public void put(ByteData key, ByteData value) {
        this.mutated = true;
        this.delegate().put(key, value);
    }

    @Override
    public void remove(ByteData key) {
        this.mutated = true;
        this.delegate().remove(key);
    }

    @Override
    public void removeRange(ByteData minKey, ByteData maxKey) {
        this.mutated = true;
        this.delegate().removeRange(minKey, maxKey);
    }

    @Override
    public void apply(Mutations mutations) {
        this.mutated = true;
        this.delegate().apply(mutations);
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

    protected ByteData getBytes(ResultSet resultSet, int index) throws SQLException {
        Preconditions.checkArgument(resultSet != null, "null resultSet");
        return Optional.ofNullable(resultSet.getBytes(index))
          .map(ByteData::of)
          .orElse(null);
    }

    protected ByteData queryBytes(StmtType stmtType, ByteData... params) {
        assert params.length == stmtType.getNumParams();
        final ByteData result = this.query(stmtType, (stmt, rs) -> rs.next() ? this.getBytes(rs, 1) : null, true, params);
        if (this.log.isTraceEnabled())
            this.log.trace("SQL query returned {}", result != null ? result.size() + " bytes" : "not found");
        return result;
    }

    protected KVPair queryKVPair(StmtType stmtType, ByteData... params) {
        assert params.length == stmtType.getNumParams();
        final KVPair pair = this.query(stmtType,
          (stmt, rs) -> rs.next() ? new KVPair(this.decodeKey(this.getBytes(rs, 1)), this.getBytes(rs, 2)) : null,
          true, params);
        if (this.log.isTraceEnabled()) {
            this.log.trace("SQL query returned "
              + (pair != null ? "(" + pair.getKey().size() + ", " + pair.getValue().size() + ") bytes" : "not found"));
        }
        return pair;
    }

    protected CloseableIterator<KVPair> queryIterator(StmtType stmtType, ByteData... params) {
        assert params.length == stmtType.getNumParams();
        final CloseableIterator<KVPair> i = this.query(stmtType, ResultSetIterator::new, false, params);
        if (this.log.isTraceEnabled())
            this.log.trace("SQL query returned {}", (i.hasNext() ? "non-" : "") + "empty iterator");
        return i;
    }

    protected <T> T query(StmtType stmtType, ResultSetFunction<T> resultSetFunction, boolean close, ByteData... params) {
        assert params.length == stmtType.getNumParams();
        try {
            final PreparedStatement preparedStatement = stmtType.create(this.database, this.connection, this.log);
            final int numParams = preparedStatement.getParameterMetaData().getParameterCount();
            for (int i = 0; i < params.length && i < numParams; i++) {
                if (this.log.isTraceEnabled())
                    this.log.trace("setting ?{} = {}", i + 1, ByteUtil.toString(params[i]));
                preparedStatement.setBytes(i + 1, params[i].toByteArray());
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

    protected void update(StmtType stmtType, ByteData... params) {
        assert params.length == stmtType.getNumParams();
        try (PreparedStatement preparedStatement = stmtType.create(this.database, this.connection, this.log)) {
            final int numParams = preparedStatement.getParameterMetaData().getParameterCount();
            for (int i = 0; i < params.length && i < numParams; i++) {
                if (this.log.isTraceEnabled())
                    this.log.trace("setting ?{} = {}", i + 1, ByteUtil.toString(params[i]));
                preparedStatement.setBytes(i + 1, params[i].toByteArray());
            }
            preparedStatement.setQueryTimeout((int)((this.timeout + 999) / 1000));
            if (this.log.isTraceEnabled())
                this.log.trace("executing SQL update");
            preparedStatement.executeUpdate();
            if (this.log.isTraceEnabled())
                this.log.trace("SQL update completed");
        } catch (SQLException e) {
            throw this.handleException(e);
        }
    }

    protected void updateBatch(StmtType stmtType, List<ByteData> paramList) {

        // Each statement will consume this many parameters from paramList
        final int numStmtParams = stmtType.getNumParams();
        assert numStmtParams > 0;
        assert paramList.size() % numStmtParams == 0;

        // Create statement and do batches
        try (PreparedStatement preparedStatement = stmtType.create(this.database, this.connection, this.log)) {
            final int numSqlParams = preparedStatement.getParameterMetaData().getParameterCount();

            // Set query timeout
            preparedStatement.setQueryTimeout((int)((this.timeout + 999) / 1000));

            // While there is more data, do more batches
            int paramIndex = 0;
            while (paramIndex < paramList.size()) {

                // While data limit per batch not reached, add more data to the batch
                int numStatementsInBatch = 0;
                int batchTotalBytes = 0;
                while (numStatementsInBatch < MAX_STATEMENTS_PER_BATCH
                  && batchTotalBytes < MAX_DATA_PER_BATCH && paramIndex < paramList.size()) {
                    for (int i = 0; i < numSqlParams; i++) {
                        final ByteData param = paramList.get(paramIndex + i);
                        if (this.log.isTraceEnabled())
                            this.log.trace("setting ?{} = {}", i + 1, ByteUtil.toString(param));
                        preparedStatement.setBytes(i + 1, param.toByteArray());
                        batchTotalBytes += param.size();
                    }
                    preparedStatement.addBatch();
                    paramIndex += numStmtParams;
                    numStatementsInBatch++;
                    batchTotalBytes += BATCH_STATEMENT_OVERHEAD;
                }

                // Execute batch
                if (this.log.isTraceEnabled())
                    this.log.trace("executing SQL batch update");
                preparedStatement.executeBatch();
                if (this.log.isTraceEnabled())
                    this.log.trace("SQL batch update completed");
            }
            assert paramIndex == paramList.size();
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
    protected ByteData encodeKey(ByteData key) {
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

    protected ByteData decodeKey(ByteData dbkey) {
        return dbkey;
    }

// SQLView

    private class SQLView extends AbstractKVStore {

        @Override
        public ByteData get(ByteData key) {
            return SQLKVTransaction.this.getSQL(key);
        }

        @Override
        public KVPair getAtLeast(ByteData minKey, ByteData maxKey) {

            // Avoid range query when only a single key is in the range
            if (minKey != null && maxKey != null && ByteUtil.isConsecutive(minKey, maxKey)) {
                final ByteData value = this.get(minKey);
                return value != null ? new KVPair(minKey, value) : null;
            }
            return SQLKVTransaction.this.getAtLeastSQL(minKey, maxKey);
        }

        @Override
        public KVPair getAtMost(ByteData maxKey, ByteData minKey) {

            // Avoid range query when only a single key is in the range
            if (minKey != null && maxKey != null && ByteUtil.isConsecutive(minKey, maxKey)) {
                final ByteData value = this.get(minKey);
                return value != null ? new KVPair(minKey, value) : null;
            }
            return SQLKVTransaction.this.getAtMostSQL(maxKey, minKey);
        }

        @Override
        public CloseableIterator<KVPair> getRange(final ByteData minKey, final ByteData maxKey, final boolean reverse) {

            // Avoid range query when only a single key is in the range
            if (minKey != null && maxKey != null && ByteUtil.isConsecutive(minKey, maxKey)) {
                final ByteData value = this.get(minKey);
                if (value == null) {
                    return new CloseableIterator<KVPair>() {

                        @Override
                        public boolean hasNext() {
                            return false;
                        }

                        @Override
                        public KVPair next() {
                            throw new NoSuchElementException();
                        }

                        @Override
                        public void remove() {
                            throw new NoSuchElementException();
                        }

                        @Override
                        public void close() {
                        }
                    };
                } else {
                    return new CloseableIterator<KVPair>() {

                        private boolean gotten;

                        @Override
                        public boolean hasNext() {
                            return !this.gotten;
                        }

                        @Override
                        public KVPair next() {
                            if (this.gotten)
                                throw new NoSuchElementException();
                            this.gotten = true;
                            return new KVPair(minKey, value);
                        }

                        @Override
                        public void remove() {
                            if (!this.gotten)
                                throw new NoSuchElementException();
                            SQLView.this.remove(minKey);
                        }

                        @Override
                        public void close() {
                        }
                    };
                }
            }
            return SQLKVTransaction.this.getRangeSQL(minKey, maxKey, reverse);
        }

        @Override
        public void put(ByteData key, ByteData value) {
            SQLKVTransaction.this.putSQL(key, value);
        }

        @Override
        public void remove(ByteData key) {
            SQLKVTransaction.this.removeSQL(key);
        }

        @Override
        public void removeRange(ByteData minKey, ByteData maxKey) {
            SQLKVTransaction.this.removeRangeSQL(minKey, maxKey);
        }

        @Override
        public void apply(Mutations mutations) {
            SQLKVTransaction.this.applyBatch(mutations);
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
        private ByteData removeKey;

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
            final ByteData key;
            final ByteData value;
            try {
                key = SQLKVTransaction.this.decodeKey(SQLKVTransaction.this.getBytes(this.resultSet, 1));
                value = SQLKVTransaction.this.getBytes(this.resultSet, 2);
            } catch (SQLException e) {
                throw SQLKVTransaction.this.handleException(e);
            }
            this.removeKey = key;
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
        @SuppressWarnings("deprecation")
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

        GET(1) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetStatement(), log);
            };
        },
        GET_FIRST(0) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.limitSingleRow(db.createGetAllStatement(false)), log);
            };
        },
        GET_LAST(0) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.limitSingleRow(db.createGetAllStatement(true)), log);
            };
        },
        GET_AT_LEAST_FORWARD(1) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetAtLeastStatement(false), log);
            };
        },
        GET_AT_LEAST_FORWARD_SINGLE(1) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.limitSingleRow(db.createGetAtLeastStatement(false)), log);
            };
        },
        GET_AT_LEAST_REVERSE(1) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetAtLeastStatement(true), log);
            };
        },
        GET_AT_LEAST_REVERSE_SINGLE(1) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.limitSingleRow(db.createGetAtLeastStatement(true)), log);
            };
        },
        GET_AT_MOST_FORWARD(1) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetAtMostStatement(false), log);
            };
        },
        GET_AT_MOST_FORWARD_SINGLE(1) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.limitSingleRow(db.createGetAtMostStatement(false)), log);
            };
        },
        GET_AT_MOST_REVERSE(1) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetAtMostStatement(true), log);
            };
        },
        GET_AT_MOST_REVERSE_SINGLE(1) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.limitSingleRow(db.createGetAtMostStatement(true)), log);
            };
        },
        GET_RANGE_FORWARD(2) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetRangeStatement(false), log);
            };
        },
        GET_RANGE_FORWARD_SINGLE(2) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.limitSingleRow(db.createGetRangeStatement(false)), log);
            };
        },
        GET_RANGE_REVERSE(2) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetRangeStatement(true), log);
            };
        },
        GET_RANGE_REVERSE_SINGLE(2) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.limitSingleRow(db.createGetRangeStatement(true)), log);
            };
        },
        GET_ALL_FORWARD(0) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetAllStatement(false), log);
            };
        },
        GET_ALL_REVERSE(0) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createGetAllStatement(true), log);
            };
        },
        PUT(3) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createPutStatement(), log);
            };
        },
        REMOVE(1) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createRemoveStatement(), log);
            };
        },
        REMOVE_RANGE(2) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createRemoveRangeStatement(), log);
            };
        },
        REMOVE_AT_LEAST(1) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createRemoveAtLeastStatement(), log);
            };
        },
        REMOVE_AT_MOST(1) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createRemoveAtMostStatement(), log);
            };
        },
        REMOVE_ALL(0) {
            @Override
            protected PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException {
                return this.prepare(c, db.createRemoveAllStatement(), log);
            };
        };

        private final int numParams;

        StmtType(int numParams) {
            this.numParams = numParams;
        }

        public int getNumParams() {
            return this.numParams;
        }

        protected abstract PreparedStatement create(SQLKVDatabase db, Connection c, Logger log) throws SQLException;

        protected PreparedStatement prepare(Connection c, String sql, Logger log) throws SQLException {
            if (log.isTraceEnabled())
                log.trace("preparing SQL statement: {}", sql);
            return c.prepareStatement(sql,
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
        }
    }
}
