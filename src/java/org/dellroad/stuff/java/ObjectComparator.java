
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link Comparator} that creates a stable, total ordering of all Java objects.
 *
 * <p>
 * Instances are compared first using {@linkplain System#identityHashCode identity hash codes}, and then using
 * an internally generated unique identifier; identifiers are indexed using weak keys to avoid memory leaks.
 * Any two distict Java objects will always compare as non-equal.
 * </p>
 *
 * <p>
 * Note: this ordering is only <i>consistent with equals</i> (see {@link Comparable}) for classes whose
 * {@link #equals equals()} method is implemented in terms of object equality (e.g., not overridden at all).
 * </p>
 *
 * <p>
 * Note: while each instance creates a stable sort ordering, distinct instances of this class may sort objects differently.
 * </p>
 *
 * <p>
 * Null values are supported and always sort last. Instances of this class are thread safe.
 * </p>
 *
 * <p>
 * This class requires <a href="https://github.com/google/guava">Google Guava</a>.
 * </p>
 */
public class ObjectComparator implements Comparator<Object> {

    private final AtomicLong nextId = new AtomicLong();
    private final LoadingCache<Object, Long> idMap;

    /**
     * Default constructor.
     */
    public ObjectComparator() {
        this(CacheBuilder.newBuilder());
    }

    /**
     * Constructor.
     *
     * @param concurrencyLevel guides the allowed concurrency among update operations; used as a hint for internal sizing
     * @throws IllegalArgumentException if {@code concurrencyLevel} is zero or less
     * @see CacheBuilder#concurrencyLevel
     */
    public ObjectComparator(int concurrencyLevel) {
        this(CacheBuilder.newBuilder().concurrencyLevel(concurrencyLevel));
    }

    /**
     * Internal constructor.
     *
     * @param cacheBuilder object unique ID cache builder
     * @throws NullPointerException if {@code cacheBuilder} is null
     */
    protected ObjectComparator(CacheBuilder<Object, Object> cacheBuilder) {
        this.idMap = cacheBuilder.weakKeys().<Object, Long>build(new CacheLoader<Object, Long>() {
            @Override
            public Long load(Object obj) {
                return ObjectComparator.this.nextId.getAndIncrement();
            }
        });
    }

    @Override
    public int compare(Object obj1, Object obj2) {

        // Handle the equality case first to avoid creating unique ID's for no reason
        if (obj1 == obj2)
            return 0;

        // Handle one of the objects being null
        if (obj1 == null)
            return 1;
        if (obj2 == null)
            return -1;

        // Compare hash values
        final int hash1 = System.identityHashCode(obj1);
        final int hash2 = System.identityHashCode(obj2);
        if (hash1 < hash2)
            return -1;
        if (hash1 > hash2)
            return 1;

        // Compare unique serial numbers (it should be very rare that we get to this point)
        final long id1 = this.idMap.getUnchecked(obj1);
        final long id2 = this.idMap.getUnchecked(obj2);
        assert id1 != id2;
        return Long.compare(id1, id2);
    }
}

