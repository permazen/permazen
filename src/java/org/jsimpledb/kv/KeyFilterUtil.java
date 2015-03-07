
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv;

import java.util.ArrayList;
import java.util.Arrays;

import org.jsimpledb.util.ByteUtil;

/**
 * Utility methods for dealing with {@link KeyFilter}s.
 *
 * @see KeyFilter
 */
public final class KeyFilterUtil {

    private KeyFilterUtil() {
    }

    /**
     * Create a {@link KeyFilter} that represents the union of the given instances.
     *
     * @param keyFilters instances to unify
     * @return the union of the given {@code keyFilters}
     * @throws IllegalArgumentException if {@code keyFilters} is empty
     * @throws IllegalArgumentException if {@code keyFilters} or any element in {@code keyFilters} is null
     */
    public static KeyFilter union(KeyFilter... keyFilters) {
        if (keyFilters == null)
            throw new IllegalArgumentException("null keyFilters");
        switch (keyFilters.length) {
        case 0:
            throw new IllegalArgumentException("empty keyFilters");
        case 1:
            return keyFilters[0];
        default:
            break;
        }

        // Optimize when all are KeyRanges instances
        boolean allKeyRanges = true;
        for (KeyFilter keyFilter : keyFilters) {
            if (keyFilter == null)
                throw new IllegalArgumentException("null keyFilter");
            if (!(keyFilter instanceof KeyRanges))
                allKeyRanges = false;
        }
        if (allKeyRanges) {
            KeyRanges union = (KeyRanges)keyFilters[0];
            for (int i = 1; i < keyFilters.length; i++)
                union = union.union((KeyRanges)keyFilters[i]);
            return union;
        }

        // Can't optimize
        return new UnionKeyFilter(keyFilters);
    }

    /**
     * Create a {@link KeyFilter} that represents the intersection of the given instances.
     *
     * @param keyFilters instances to intersect
     * @return the intersection of the given {@code keyFilters}
     * @throws IllegalArgumentException if {@code keyFilters} is empty
     * @throws IllegalArgumentException if {@code keyFilters} or any element in {@code keyFilters} is null
     */
    public static KeyFilter intersection(KeyFilter... keyFilters) {
        if (keyFilters == null)
            throw new IllegalArgumentException("null keyFilters");
        switch (keyFilters.length) {
        case 0:
            throw new IllegalArgumentException("empty keyFilters");
        case 1:
            return keyFilters[0];
        default:
            break;
        }

        // Optimize when all are KeyRanges instances
        boolean allKeyRanges = true;
        for (KeyFilter keyFilter : keyFilters) {
            if (keyFilter == null)
                throw new IllegalArgumentException("null keyFilter");
            if (!(keyFilter instanceof KeyRanges))
                allKeyRanges = false;
        }
        if (allKeyRanges) {
            KeyRanges intersection = (KeyRanges)keyFilters[0];
            for (int i = 1; i < keyFilters.length; i++)
                intersection = intersection.intersection((KeyRanges)keyFilters[i]);
            return intersection;
        }

        // Can't optimize
        return new IntersectionKeyFilter(keyFilters);
    }

    /**
     * Determine if the given instance is empty, i.e., contains no keys.
     *
     * @param keyFilter instance to check
     * @return true if filter is empty
     * @throws IllegalArgumentException if {@code keyFilter} is null
     */
    public static boolean isEmpty(KeyFilter keyFilter) {
        if (keyFilter == null)
            throw new IllegalArgumentException("null keyFilter");
        return keyFilter.seekHigher(ByteUtil.EMPTY) == null;
    }

    private static byte[] seek(KeyFilter[] keyFilters, byte[] key, boolean seekHigher, boolean preferHigher) {
        if (key == null)
            throw new IllegalArgumentException("null key");
        assert keyFilters.length > 0;
        final boolean preferNull = seekHigher == preferHigher;
        byte[] best = null;
        for (int i = 0; i < keyFilters.length; i++) {
            final KeyFilter keyFilter = keyFilters[i];
            final byte[] next = seekHigher ? keyFilter.seekHigher(key) : keyFilter.seekLower(key);
            if (i == 0)
                best = next;
            if (next == null) {
                if (preferNull)
                    return null;
                continue;
            }
            assert next.length != 0 || key.length == 0;
            if (i > 0 && best != null) {
                final int diff = (!seekHigher && next.length == 0) ? 1 : ByteUtil.compare(next, best);
                if (preferHigher ? diff < 0 : diff > 0)
                    continue;
            } else
                assert i == 0 || !preferNull;
            best = next;
        }
        return best;
    }

// UnionKeyFilter

    private static class UnionKeyFilter implements KeyFilter {

        private final KeyFilter[] keyFilters;

        UnionKeyFilter(KeyFilter[] keyFilters) {
            assert keyFilters.length >= 2;
            final ArrayList<KeyFilter> list = new ArrayList<>();
            for (KeyFilter keyFilter : keyFilters) {
                if (keyFilter instanceof UnionKeyFilter)
                    list.addAll(Arrays.asList(((UnionKeyFilter)keyFilter).keyFilters));
                else
                    list.add(keyFilter);
            }
            this.keyFilters = list.toArray(new KeyFilter[list.size()]);
        }

        @Override
        public boolean contains(byte[] key) {
            for (KeyFilter keyFilter : this.keyFilters) {
                if (keyFilter.contains(key))
                    return true;
            }
            return false;
        }

        @Override
        public byte[] seekHigher(byte[] key) {
            return KeyFilterUtil.seek(this.keyFilters, key, true, false);
        }

        @Override
        public byte[] seekLower(byte[] key) {
            return KeyFilterUtil.seek(this.keyFilters, key, false, true);
        }
    }

// IntersectionKeyFilter

    private static class IntersectionKeyFilter implements KeyFilter {

        private final KeyFilter[] keyFilters;

        IntersectionKeyFilter(KeyFilter[] keyFilters) {
            assert keyFilters.length >= 2;
            final ArrayList<KeyFilter> list = new ArrayList<>();
            for (KeyFilter keyFilter : keyFilters) {
                if (keyFilter instanceof IntersectionKeyFilter)
                    list.addAll(Arrays.asList(((IntersectionKeyFilter)keyFilter).keyFilters));
                else
                    list.add(keyFilter);
            }
            this.keyFilters = list.toArray(new KeyFilter[list.size()]);
        }

        @Override
        public boolean contains(byte[] key) {
            for (KeyFilter keyFilter : this.keyFilters) {
                if (!keyFilter.contains(key))
                    return false;
            }
            return true;
        }

        @Override
        public byte[] seekHigher(byte[] key) {
            return KeyFilterUtil.seek(this.keyFilters, key, true, true);
        }

        @Override
        public byte[] seekLower(byte[] key) {
            return KeyFilterUtil.seek(this.keyFilters, key, false, false);
        }
    }
}

