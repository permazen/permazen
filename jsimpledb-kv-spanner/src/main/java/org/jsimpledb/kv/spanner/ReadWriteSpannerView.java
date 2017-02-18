
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.mvcc.MutableView;
import org.jsimpledb.kv.mvcc.Writes;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a mutable {@link KVStore} view of a {@link ReadContext}.
 *
 * <p>
 * Use {@link #bufferMutations bufferMutations()} to transfer outstanding mutations into a {@link TransactionContext}.
 *
 * @see ReadOnlySpannerView
 */
public class ReadWriteSpannerView extends MutableView implements CloseableKVStore {

    private static final byte[] TOP = new byte[] { (byte)0xff };
    private static final Key TOP_KEY = Key.of(ByteArray.copyFrom(TOP));

    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final String tableName;
    protected final Function<? super SpannerException, RuntimeException> exceptionMapper;

    /**
     * Primary constructor.
     *
     * @param tableName name of the Spanner database table
     * @param context read context
     * @param exceptionMapper mapper for any thrown {@link SpannerException}s, or null for none
     * @throws IllegalArgumentException if {@code tableName} or {@code context} is null
     */
    public ReadWriteSpannerView(String tableName, ReadContext context,
      Function<? super SpannerException, RuntimeException> exceptionMapper) {
        super(new ReadOnlySpannerView(tableName, context, exceptionMapper), null, new Writes());
        this.tableName = tableName;
        this.exceptionMapper = exceptionMapper;
    }

    /**
     * Convenience constructor.
     *
     * <p>
     * Equivalent to {@code ReadWriteSpannerView(tableName, context, null)}.
     *
     * @param tableName name of the Spanner database table
     * @param context read context
     * @throws IllegalArgumentException if {@code tableName} or {@code context} is null
     */
    public ReadWriteSpannerView(String tableName, ReadContext context) {
        this(tableName, context, null);
    }

    /**
     * Atomically transfer all of the outstanding mutations associated with this instance into the given transaction context,
     * and then clear them.
     *
     * @param context transaction context
     * @throws IllegalArgumentException if {@code context} is null
     */
    public synchronized void bufferMutations(TransactionContext context) {
        Preconditions.checkArgument(context != null);

        // Get writes
        final Writes writes = this.getWrites();
        if (this.log.isTraceEnabled())
            this.log.trace("applying " + writes + " to " + context);

        // Add removes
        for (org.jsimpledb.kv.KeyRange range : writes.getRemoves()) {
            final byte[] rangeMax = range.getMax();
            final Key minKey = Key.of(ByteArray.copyFrom(range.getMin()));
            final Key maxKey = rangeMax == null || ByteUtil.compare(rangeMax, TOP) >= 0 ?
              TOP_KEY : Key.of(ByteArray.copyFrom(rangeMax));
            this.buffer(context, Mutation.delete(this.tableName, KeySet.range(KeyRange.closedOpen(minKey, maxKey))));
        }

        // Add puts
        for (Map.Entry<byte[], byte[]> put : writes.getPuts().entrySet()) {

            // Ignore keys starting with 0xff
            if (ByteUtil.compare(put.getKey(), TOP) >= 0)
                continue;

            // Add put
            this.buffer(context, Mutation.newReplaceBuilder(this.tableName)
              .set("key").to(ByteArray.copyFrom(put.getKey()))
              .set("val").to(ByteArray.copyFrom(put.getValue()))
              .build());
        }

        // Add adjusts
        for (Map.Entry<byte[], Long> adjust : writes.getAdjusts().entrySet()) {

            // Ignore keys starting with 0xff
            if (ByteUtil.compare(adjust.getKey(), TOP) >= 0)
                continue;

            // Get counter key
            final ByteArray key = ByteArray.copyFrom(adjust.getKey());

            // Read old counter value
            final Struct row = context.readRow(this.tableName, Key.of(key), Collections.singleton("val"));
            if (row == null)
                continue;
            final byte[] data = row.getBytes(0).toByteArray();
            if (data.length != 8)
                continue;

            // Decode value
            final long adjustedValue = ByteUtil.readLong(new ByteReader(data)) + adjust.getValue();
            final ByteWriter writer = new ByteWriter(8);
            ByteUtil.writeLong(writer, adjustedValue);

            // Write back new counter value
            this.buffer(context, Mutation.newReplaceBuilder(this.tableName)
              .set("key").to(key)
              .set("val").to(ByteArray.copyFrom(writer.getBytes()))
              .build());
        }

        // Done
        writes.clear();
    }

    private void buffer(TransactionContext context, Mutation mutation) {
        if (this.log.isTraceEnabled())
            this.log.trace("adding mutation " + mutation);
        context.buffer(mutation);
    }

// Cloneable

    @Override
    public ReadWriteSpannerView clone() {
        return (ReadWriteSpannerView)super.clone();
    }

// Closeable

    /**
     * Close the associated {@link ReadContext}.
     */
    @Override
    public void close() {
        ((ReadOnlySpannerView)this.getKVStore()).close();
    }
}

