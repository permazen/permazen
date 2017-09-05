
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.spanner;

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

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVStore;
import io.permazen.kv.caching.CachingKVStore;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.mvcc.Writes;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a mutable {@link KVStore} view of a {@link ReadContext}.
 *
 * <p>
 * For best performance, supply an {@link ExecutorService} for asynchronous batch loading.
 *
 * <p>
 * Use {@link #bufferMutations bufferMutations()} to transfer outstanding mutations into a {@link TransactionContext}.
 *
 * @see ReadOnlySpannerView
 * @see io.permazen.kv.caching.CachingKVStore
 */
public class ReadWriteSpannerView extends MutableView implements CloseableKVStore {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final String tableName;
    protected final Function<? super SpannerException, RuntimeException> exceptionMapper;

    /**
     * Constructor for when no batching is desired.
     *
     * @param tableName name of the Spanner database table
     * @param context read context
     * @param exceptionMapper mapper for any thrown {@link SpannerException}s, or null for none
     * @throws IllegalArgumentException if {@code tableName} or {@code context} is null
     */
    public ReadWriteSpannerView(String tableName, ReadContext context,
      Function<? super SpannerException, RuntimeException> exceptionMapper) {
        this(tableName, new ReadOnlySpannerView(tableName, context, exceptionMapper), exceptionMapper);
    }

    /**
     * Constructor for when batching is desired.
     *
     * @param tableName name of the Spanner database table
     * @param context read context
     * @param exceptionMapper mapper for any thrown {@link SpannerException}s, or null for none
     * @param executor asynchronous load task executor
     * @param rttEstimate initial RTT estimate in nanoseconds
     * @throws IllegalArgumentException if {@code tableName}, {@code context} or {@code executor} is null
     * @throws IllegalArgumentException if {@code rttEstimate} is negative
     */
    public ReadWriteSpannerView(String tableName, ReadContext context,
      Function<? super SpannerException, RuntimeException> exceptionMapper, ExecutorService executor, long rttEstimate) {
        this(tableName,
          new CachingKVStore(new ReadOnlySpannerView(tableName, context, exceptionMapper), executor, rttEstimate), null);
    }

    private ReadWriteSpannerView(String tableName, KVStore view,
      Function<? super SpannerException, RuntimeException> exceptionMapper) {
        super(view, null, new Writes());
        this.tableName = tableName;
        this.exceptionMapper = exceptionMapper;
    }

    /**
     * Get the current RTT estimate.
     *
     * @return current RTT estimate in nanoseconds
     */
    public double getRttEstimate() {
        return ((CachingKVStore)this.getKVStore()).getRttEstimate();
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
        for (io.permazen.kv.KeyRange range : writes.getRemoves()) {
            final byte[] rangeMax = range.getMax();
            final Key minKey = Key.of(ByteArray.copyFrom(range.getMin()));
            this.buffer(context, Mutation.delete(this.tableName,
              rangeMax != null ?                            // https://github.com/GoogleCloudPlatform/google-cloud-java/issues/1629
               KeySet.range(KeyRange.closedOpen(minKey, Key.of(ByteArray.copyFrom(rangeMax)))) :
               KeySet.range(KeyRange.closedClosed(minKey, Key.of()))));
        }

        // Add puts
        for (Map.Entry<byte[], byte[]> put : writes.getPuts().entrySet()) {

            // Add put
            this.buffer(context, Mutation.newReplaceBuilder(this.tableName)
              .set("key").to(ByteArray.copyFrom(put.getKey()))
              .set("val").to(ByteArray.copyFrom(put.getValue()))
              .build());
        }

        // Add adjusts
        for (Map.Entry<byte[], Long> adjust : writes.getAdjusts().entrySet()) {

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
        ((CloseableKVStore)this.getKVStore()).close();
    }
}

