
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import com.google.common.base.Preconditions;

import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.kv.mvcc.Mutations;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;

import java.util.Map;
import java.util.stream.Stream;

/**
 * General API into a key/value store where the keys are sorted lexicographically as unsigned bytes.
 *
 * <p>
 * Implementations are not required to support accessing keys that start with {@code 0xff},
 * and if not may throw {@link IllegalArgumentException} if such keys are accessed.
 *
 * <p><b>Thread Safety</b></p>
 *
 * <p>
 * Instances must be thread safe, in the sense that multi-threaded operations never lead to a behavior
 * that is inconsisitent with some consistent total ordering of those operations. So for example if
 * thread A invokes {@link #removeRange removeRange()} while thread B does a {@link #put put} to some
 * key in the range, then afterwards either the range is empty or it contains only the key, but in
 * any case no other outcome is possible.
 *
 * <p>
 * With respect to thread safety, the set of possible "operations" includes accessing the {@link CloseableIterator}
 * returned by {@link #getRange getRange()}; see {@link #getRange(ByteData, ByteData, boolean) getRange()} for details.
 *
 * <p><b>Lock-free Counters</b></p>
 *
 * <p>
 * Implementations are encouraged to include support for encoding a 64 bit counter in a key/value pair such that the counter
 * can be efficiently {@linkplain #adjustCounter adjusted} by concurrent transactions without conflict.
 * In practice this means no locking is required to increment or decrement the counter by some amount, as long as
 * it's not necessary to actually directly read or write the counter value in the same transaction.
 * Whether counter adjustments are actually lock-free is implementation dependent, however, the counter methods
 * {@link #encodeCounter encodeCounter()}, {@link #decodeCounter decodeCounter()}, and {@link #adjustCounter adjustCounter()}
 * must function correctly as specified in all cases.
 *
 * <p>
 * How counters are encoded is specific to the implementation. Clients needing to read or write counter values directly
 * should use {@link #decodeCounter decodeCounter()} and {@link #encodeCounter encodeCounter()}, respectively.
 * Counters are removed using the normal methods (i.e., {@link #remove remove()} and {@link #removeRange removeRange()}).
 */
public interface KVStore {

    /**
     * Get the value associated with the given key, if any.
     *
     * @param key key
     * @return value associated with key, or null if not found
     * @throws IllegalArgumentException if {@code key} starts with {@code 0xff} and such keys are not supported
     * @throws StaleKVTransactionException if an underlying transaction is no longer usable
     * @throws RetryKVTransactionException if an underlying transaction must be retried and is no longer usable
     * @throws NullPointerException if {@code key} is null
     */
    ByteData get(ByteData key);

    /**
     * Get the key/value pair having the smallest key greater than or equal to the given minimum, if any.
     *
     * <p>
     * An optional (exclusive) maximum key may also be specified; if {@code maxKey} is null, there is no upper bound;
     * if {@code maxKey <= minKey}, null is always returned.
     *
     * <p>
     * If keys starting with {@code 0xff} are not supported by this instance, and {@code minKey} starts with {@code 0xff},
     * then this method returns null.
     *
     * @param minKey minimum key (inclusive), or null for no minimum (get the smallest key)
     * @param maxKey maximum key (exclusive), or null for no maximum (no upper bound)
     * @return smallest key/value pair with {@code key >= minKey} and {@code key < maxKey}, or null if none exists
     * @throws StaleKVTransactionException if an underlying transaction is no longer usable
     * @throws RetryKVTransactionException if an underlying transaction must be retried and is no longer usable
     */
    KVPair getAtLeast(ByteData minKey, ByteData maxKey);

    /**
     * Get the key/value pair having the largest key strictly less than the given maximum, if any.
     *
     * <p>
     * An optional (inclusive) minimum key may also be specified; if {@code minKey} is null, there is no lower bound
     * (equivalent to a lower bound of the empty byte array); if {@code minKey >= maxKey}, null is always returned.
     *
     * <p>
     * If keys starting with {@code 0xff} are not supported by this instance, and {@code maxKey} starts with {@code 0xff},
     * then this method behaves as if {@code maxKey} were null.
     *
     * @param maxKey maximum key (exclusive), or null for no maximum (get the largest key)
     * @param minKey minimum key (inclusive), or null for no minimum (no lower bound)
     * @return largest key/value pair with {@code key < maxKey} and {@code key >= minKey}, or null if none exists
     * @throws StaleKVTransactionException if an underlying transaction is no longer usable
     * @throws RetryKVTransactionException if an underlying transaction must be retried and is no longer usable
     */
    KVPair getAtMost(ByteData maxKey, ByteData minKey);

    /**
     * Iterate the key/value pairs in the specified range. The returned {@link CloseableIterator}'s
     * {@link CloseableIterator#remove remove()} method must be supported and should have the same effect as
     * invoking {@link #remove remove()} on the corresponding key.
     *
     * <p>
     * If keys starting with {@code 0xff} are not supported by this instance, and {@code minKey} starts with {@code 0xff},
     * then this method returns an empty iteration.
     *
     * <p>
     * If keys starting with {@code 0xff} are not supported by this instance, and {@code maxKey} starts with {@code 0xff},
     * then this method behaves as if {@code maxKey} were null.
     *
     * <p>
     * The returned {@link CloseableIterator} is <i>weakly consistent</i> (see {@link java.util.concurrent}).
     * In short, the returned {@link CloseableIterator} must not throw {@link java.util.ConcurrentModificationException};
     * however, whether or not a "live" {@link CloseableIterator} reflects any modifications made after its creation is
     * implementation dependent. Implementations that do make post-creation updates visible in the {@link CloseableIterator},
     * even if the update occurs after some delay, must preserve the order in which the modifications actually occurred.
     *
     * <p>
     * The returned {@link CloseableIterator} itself is not guaranteed to be thread safe; is should only be used
     * in the thread that created it.
     *
     * <p>
     * Invokers of this method are encouraged to {@link java.io.Closeable#close close()} the returned iterators,
     * though this is not required for correct behavior.
     *
     * @param minKey minimum key (inclusive), or null for no minimum (start at the smallest key)
     * @param maxKey maximum key (exclusive), or null for no maximum (end at the largest key)
     * @param reverse true to return key/value pairs in reverse order (i.e., keys descending)
     * @return iteration of key/value pairs in the range {@code minKey} (inclusive) to {@code maxKey} (exclusive)
     * @throws IllegalArgumentException if {@code minKey > maxKey}
     * @throws StaleKVTransactionException if an underlying transaction is no longer usable
     * @throws RetryKVTransactionException if an underlying transaction must be retried and is no longer usable
     */
    CloseableIterator<KVPair> getRange(ByteData minKey, ByteData maxKey, boolean reverse);

    /**
     * Iterate the key/value pairs in the specified range in the forward direction.
     *
     * <p>
     * This is a convenience method, equivalent to:
     * {@link #getRange(ByteData, ByteData, boolean) getRange}{@code (minKey, maxKey, false)}.
     *
     * @param minKey minimum key (inclusive), or null for no minimum (start at the smallest key)
     * @param maxKey maximum key (exclusive), or null for no maximum (end at the largest key)
     * @return iteration of key/value pairs in the range {@code minKey} (inclusive) to {@code maxKey} (exclusive)
     * @throws IllegalArgumentException if {@code minKey > maxKey}
     * @throws StaleKVTransactionException if an underlying transaction is no longer usable
     * @throws RetryKVTransactionException if an underlying transaction must be retried and is no longer usable
     */
    default CloseableIterator<KVPair> getRange(ByteData minKey, ByteData maxKey) {
        return this.getRange(minKey, maxKey, false);
    }

    /**
     * Iterate the key/value pairs in the specified range in the forward direction.
     *
     * <p>
     * This is a convenience method, equivalent to:
     * {@link #getRange(ByteData, ByteData, boolean) getRange}{@code (range.getMin(), range.getMax(), false)}.
     *
     * @param range range of keys to iterate
     * @return iteration of key/value pairs in {@code range}
     * @throws StaleKVTransactionException if an underlying transaction is no longer usable
     * @throws RetryKVTransactionException if an underlying transaction must be retried and is no longer usable
     * @throws IllegalArgumentException if {@code range} is null
     */
    default CloseableIterator<KVPair> getRange(KeyRange range) {
        Preconditions.checkArgument(range != null, "null range");
        return this.getRange(range.getMin(), range.getMax(), false);
    }

    /**
     * Set the value associated with the given key.
     *
     * @param key key
     * @param value value
     * @throws IllegalArgumentException if {@code key} starts with {@code 0xff} and such keys are not supported
     * @throws StaleKVTransactionException if an underlying transaction is no longer usable
     * @throws RetryKVTransactionException if an underlying transaction must be retried and is no longer usable
     * @throws NullPointerException if {@code key} or {@code value} is null
     */
    void put(ByteData key, ByteData value);

    /**
     * Remove the key/value pair with the given key, if it exists.
     *
     * @param key key
     * @throws IllegalArgumentException if {@code key} starts with {@code 0xff} and such keys are not supported
     * @throws StaleKVTransactionException if an underlying transaction is no longer usable
     * @throws RetryKVTransactionException if an underlying transaction must be retried and is no longer usable
     * @throws NullPointerException if {@code key} is null
     */
    void remove(ByteData key);

    /**
     * Remove all key/value pairs whose keys are in a given range.
     *
     * <p>
     * The {@code minKey} must be less than or equal to {@code maxKey}; if they equal (and not null)
     * then nothing happens; if they are both null then all entries are deleted.
     *
     * <p>
     * If keys starting with {@code 0xff} are not supported by this instance, then:
     * <ul>
     *  <li>If {@code minKey} starts with {@code 0xff}, then no change occurs</li>
     *  <li>If {@code maxKey} starts with {@code 0xff}, then this method behaves as if {@code maxKey} were null</li>
     * </ul>
     *
     * @param minKey minimum key (inclusive), or null for no minimum
     * @param maxKey maximum key (exclusive), or null for no maximum
     * @throws IllegalArgumentException if {@code minKey > maxKey}
     * @throws StaleKVTransactionException if an underlying transaction is no longer usable
     * @throws RetryKVTransactionException if an underlying transaction must be retried and is no longer usable
     */
    void removeRange(ByteData minKey, ByteData maxKey);

    /**
     * Remove all key/value pairs whose keys are in a given range.
     *
     * <p>
     * Equivalent to: {@link #removeRange removeRange}{@code (range.getMin(), range.getMax())}.
     *
     * @param range range to remove
     * @throws IllegalArgumentException if {@code range} is null
     * @throws StaleKVTransactionException if an underlying transaction is no longer usable
     * @throws RetryKVTransactionException if an underlying transaction must be retried and is no longer usable
     */
    default void removeRange(KeyRange range) {
        Preconditions.checkArgument(range != null, "null range");
        this.removeRange(range.getMin(), range.getMax());
    }

    /**
     * Encode a counter value into a {@code byte[]} value suitable for use with {@link #decodeCounter decodeCounter()}
     * and/or {@link #adjustCounter adjustCounter()}.
     *
     * @param value desired counter value
     * @return encoded counter value
     * @throws StaleKVTransactionException if an underlying transaction is no longer usable
     * @throws RetryKVTransactionException if an underlying transaction must be retried and is no longer usable
     */
    ByteData encodeCounter(long value);

    /**
     * Decode a counter value previously encoded by {@link #encodeCounter encodeCounter()}.
     *
     * @param value encoded counter value
     * @return decoded counter value
     * @throws IllegalArgumentException if {@code value} is not a valid counter value
     * @throws StaleKVTransactionException if an underlying transaction is no longer usable
     * @throws RetryKVTransactionException if an underlying transaction must be retried and is no longer usable
     * @throws NullPointerException if {@code value} is null
     */
    long decodeCounter(ByteData value);

    /**
     * Adjust the counter at the given key by the given amount.
     *
     * <p>
     * Ideally this operation should behave in a lock-free manner, so that concurrent transactions can invoke it without
     * conflict. However, when lock-free behavior occurs (if at all) depends on the implementation.
     *
     * <p>
     * If there is no value associated with {@code key}, or {@code key}'s value is not a valid counter encoding as
     * would be acceptable to {@link #decodeCounter decodeCounter()}, then how this operation affects {@code key}'s
     * value is undefined.
     *
     * @param key key
     * @param amount amount to adjust counter value by
     * @throws StaleKVTransactionException if an underlying transaction is no longer usable
     * @throws RetryKVTransactionException if an underlying transaction must be retried and is no longer usable
     * @throws NullPointerException if {@code key} is null
     */
    void adjustCounter(ByteData key, long amount);

    /**
     * Apply all the given {@link Mutations} to this instance.
     *
     * <p>
     * Mutations are always to be applied in this order: removes, puts, counter adjustments.
     *
     * <p>
     * The implementation in {@link KVStore} simply iterates over the individual changes and applies them
     * via {@link #remove remove()} (for removals of a single key), {@link #removeRange removeRange()},
     * {@link #put put()}, and/or {@link #adjustCounter adjustCounter()}. Implementations that can process
     * batch updates more efficiently are encouraged to override this method.
     *
     * <p>
     * Unlike {@link AtomicKVStore#apply(Mutations, boolean) AtomicKVStore.apply()}, this method is
     * <i>not</i> required to apply the mutations atomically.
     *
     * @param mutations mutations to apply
     * @throws IllegalArgumentException if {@code mutations} is null
     * @throws UnsupportedOperationException if this instance is immutable
     */
    default void apply(Mutations mutations) {
        Preconditions.checkArgument(mutations != null, "null mutations");
        try (Stream<KeyRange> removes = mutations.getRemoveRanges()) {
            removes.iterator().forEachRemaining(remove -> {
                final ByteData min = remove.getMin();
                final ByteData max = remove.getMax();
                assert min != null;
                if (max != null && ByteUtil.isConsecutive(min, max))
                    this.remove(min);
                else
                    this.removeRange(min, max);
            });
        }
        try (Stream<Map.Entry<ByteData, ByteData>> puts = mutations.getPutPairs()) {
            puts.iterator().forEachRemaining(entry -> this.put(entry.getKey(), entry.getValue()));
        }
        try (Stream<Map.Entry<ByteData, Long>> adjusts = mutations.getAdjustPairs()) {
            adjusts.iterator().forEachRemaining(entry -> this.adjustCounter(entry.getKey(), entry.getValue()));
        }
    }
}
