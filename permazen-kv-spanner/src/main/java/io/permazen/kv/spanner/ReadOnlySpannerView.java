
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.common.base.Preconditions;

import io.permazen.kv.AbstractKVStore;
import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a (read-only) {@link KVStore} view of a {@link ReadContext}.
 *
 * <p>
 * For best performance, consider wrapping an instance of this class in a {@link io.permazen.kv.caching.CachingKVStore}.
 *
 * @see ReadWriteSpannerView
 */
public class ReadOnlySpannerView extends AbstractKVStore implements CloseableKVStore {

    private static final byte[] TOP = new byte[] { (byte)0xff };
    private static final Key TOP_KEY = Key.of(ByteArray.copyFrom(TOP));
    private static final List<String> V_COL = Arrays.asList("val");
    private static final List<String> KV_COL = Arrays.asList("key", "val");

    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final String tableName;
    protected final ReadContext context;
    protected final Function<? super SpannerException, RuntimeException> exceptionMapper;

    /**
     * Primary constructor.
     *
     * @param tableName name of the Spanner database table
     * @param context read context
     * @param exceptionMapper mapper for any thrown {@link SpannerException}s, or null for none
     * @throws IllegalArgumentException if {@code tableName} or {@code context} is null
     */
    public ReadOnlySpannerView(String tableName, ReadContext context,
      Function<? super SpannerException, RuntimeException> exceptionMapper) {
        Preconditions.checkArgument(tableName != null);
        Preconditions.checkArgument(context != null);
        this.tableName = tableName;
        this.context = context;
        this.exceptionMapper = exceptionMapper;
    }

    /**
     * Convenience constructor.
     *
     * <p>
     * Equivalent to {@code ReadOnlySpannerView(tableName, context, null)}.
     *
     * @param tableName name of the Spanner database table
     * @param context read context
     * @throws IllegalArgumentException if {@code tableName} or {@code context} is null
     */
    public ReadOnlySpannerView(String tableName, ReadContext context) {
        this(tableName, context, null);
    }

    @Override
    public byte[] get(byte[] key) {
        try {
            final Struct row = this.context.readRow(this.tableName, Key.of(ByteArray.copyFrom(key)), V_COL);
            final byte[] val = row != null ? row.getBytes(0).toByteArray() : null;
            if (this.log.isTraceEnabled())
                this.log.trace("spanner: get(): {} -> {}", ByteUtil.toString(key), ByteUtil.toString(val));
            return val;
        } catch (SpannerException e) {
            throw this.exceptionMapper != null ? this.exceptionMapper.apply(e) : e;
        }
    }

    @Override
    public KVPair getAtLeast(byte[] minKey, byte[] maxKey) {
        if (this.log.isTraceEnabled()) {
            this.log.trace("spanner: getAtLeast():\n  minKey={}\n  maxKey={}",
              ByteUtil.toString(minKey), ByteUtil.toString(maxKey));
        }
        try (ResultSet resultSet = this.getPairs(minKey, maxKey, Options.limit(1))) {
            return resultSet.next() ? ReadOnlySpannerView.kv(resultSet.getCurrentRowAsStruct()) : null;
        } catch (SpannerException e) {
            throw this.exceptionMapper != null ? this.exceptionMapper.apply(e) : e;
        }
    }

    @Override
    public KVPair getAtMost(byte[] maxKey, byte[] minKey) {
        if (minKey != null && minKey.length == 0)
            minKey = null;
        try {
            // After this bug is fixed: https://github.com/GoogleCloudPlatform/google-cloud-java/issues/1632
            // try (ResultSet resultSet = this.getPairs(minKey, maxKey, Options.limit(1), Options.reverseOrder())) {
            //     return resultSet.next() ? ReadOnlySpannerView.kv(resultSet.getCurrentRowAsStruct()) : null;
            final Statement.Builder builder = Statement.newBuilder("SELECT key, val FROM " + this.tableName);
            this.addMaxKey(builder, maxKey, this.addMinKey(builder, minKey, false));
            builder.append(" ORDER BY key DESC LIMIT 1");
            if (this.log.isTraceEnabled()) {
                this.log.trace("spanner: getAtMost():\n  maxKey={}\n  minKey={}\n  query={}",
                  ByteUtil.toString(maxKey), ByteUtil.toString(minKey), builder.build());
            }
            try (ResultSet resultSet = this.context.executeQuery(builder.build())) {
                return resultSet.next() ? ReadOnlySpannerView.kv(resultSet.getCurrentRowAsStruct()) : null;
            }
        } catch (SpannerException e) {
            throw this.exceptionMapper != null ? this.exceptionMapper.apply(e) : e;
        }
    }

    @Override
    public CloseableIterator<KVPair> getRange(byte[] minKey, byte[] maxKey, boolean reverse) {
        if (minKey != null && minKey.length == 0)
            minKey = null;
        try {
            // After this bug is fixed: https://github.com/GoogleCloudPlatform/google-cloud-java/issues/1632
            // return new Iter(this.getPairs(minKey, maxKey, Options.reverseOrder()));
            if (!reverse)
                return new Iter(this.getPairs(minKey, maxKey));
            final Statement.Builder builder = Statement.newBuilder("SELECT key, val FROM " + this.tableName);
            this.addMaxKey(builder, maxKey, this.addMinKey(builder, minKey, false));
            builder.append(" ORDER BY key DESC");
            if (this.log.isTraceEnabled()) {
                this.log.trace("spanner: getRange():\n  minKey={}\n  maxKey={}\n  reverse={}\n  query={}",
                  ByteUtil.toString(minKey), ByteUtil.toString(maxKey), reverse, builder.build());
            }
            return new Iter(this.context.executeQuery(builder.build()));
        } catch (SpannerException e) {
            throw this.exceptionMapper != null ? this.exceptionMapper.apply(e) : e;
        }
    }

    private ResultSet getPairs(byte[] minKey, byte[] maxKey, Options.ReadOption... options) {
        final Key min = Key.of(ByteArray.copyFrom(minKey != null ? minKey : ByteUtil.EMPTY));
        final Key max = maxKey != null ? Key.of(ByteArray.copyFrom(maxKey)) : TOP_KEY;
        return this.context.read(this.tableName, KeySet.range(KeyRange.closedOpen(min, max)), KV_COL, options);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("read-only view");
    }

    @Override
    public void remove(byte[] key) {
        throw new UnsupportedOperationException("read-only view");
    }

    @Override
    public void removeRange(byte[] minKey, byte[] maxKey) {
        throw new UnsupportedOperationException("read-only view");
    }

// Closeable

    /**
     * {@linkplain ReadContext#close Closes} the associated {@link ReadContext}.
     */
    @Override
    public void close() {
        try {
            this.context.close();
        } catch (Exception e) {
            if (this.log.isDebugEnabled())
                this.log.debug("got exception closing {} (ignoring)", this.context, e);
        }
    }

// Internal methods

    private boolean addMinKey(Statement.Builder builder, byte[] minKey, boolean where) {
        return this.addKey(builder, minKey, "minKey", ">=", where);
    }

    private boolean addMaxKey(Statement.Builder builder, byte[] maxKey, boolean where) {
        return this.addKey(builder, maxKey, "maxKey", "<", where);
    }

    private boolean addKey(Statement.Builder builder, byte[] key, String name, String op, boolean where) {
        if (key == null)
            return where;
        builder.append(where ? " AND " : " WHERE ").append("key ").append(op).append(" @").append(name)
          .bind(name).to(ByteArray.copyFrom(key));
        return true;
    }

    protected static KVPair kv(Struct struct) {
        Preconditions.checkArgument(struct != null);
        return new KVPair(struct.getBytes(0).toByteArray(), struct.getBytes(1).toByteArray());
    }

// Iter

    private class Iter implements CloseableIterator<KVPair> {

        private final ResultSet resultSet;

        private Struct next;

        Iter(ResultSet resultSet) {
            this.resultSet = resultSet;
        }

        @Override
        public boolean hasNext() {
            try {
                if (this.next != null)
                    return true;
                if (!this.resultSet.next())
                    return false;
                this.next = this.resultSet.getCurrentRowAsStruct();
                return true;
            } catch (SpannerException e) {
                throw ReadOnlySpannerView.this.exceptionMapper != null ? ReadOnlySpannerView.this.exceptionMapper.apply(e) : e;
            }
        }

        @Override
        public KVPair next() {
            if (this.next == null && !this.hasNext())
                throw new NoSuchElementException();
            final KVPair kv = ReadOnlySpannerView.kv(this.next);
            this.next = null;
            return kv;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("read-only view");
        }

        @Override
        public void close() {
            this.resultSet.close();
        }

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
}
