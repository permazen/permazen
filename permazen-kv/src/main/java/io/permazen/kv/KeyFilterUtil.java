
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteData;

import java.util.ArrayList;
import java.util.Arrays;

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

        // Handle the easy cases
        Preconditions.checkArgument(keyFilters != null, "null keyFilters");
        switch (keyFilters.length) {
        case 0:
            throw new IllegalArgumentException("empty keyFilters");
        case 1:
            Preconditions.checkArgument(keyFilters[0] != null, "null keyFilter");
            return keyFilters[0];
        default:
            break;
        }

        // Are all KeyFilters are actually KeyRanges instances?
        boolean allKeyRanges = true;
        for (KeyFilter keyFilter : keyFilters) {
            if (!(keyFilter instanceof KeyRanges)) {
                allKeyRanges = false;
                break;
            }
        }

        // Optimize if so
        if (allKeyRanges) {
            final KeyRanges union = KeyRanges.empty();
            for (KeyFilter keyFilter : keyFilters)
                union.add((KeyRanges)keyFilter);
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

        // Handle the easy cases
        Preconditions.checkArgument(keyFilters != null, "null keyFilters");
        switch (keyFilters.length) {
        case 0:
            throw new IllegalArgumentException("empty keyFilters");
        case 1:
            Preconditions.checkArgument(keyFilters[0] != null, "null keyFilter");
            return keyFilters[0];
        default:
            break;
        }

        // Are all KeyFilters are actually KeyRanges instances?
        boolean allKeyRanges = true;
        for (KeyFilter keyFilter : keyFilters) {
            if (!(keyFilter instanceof KeyRanges)) {
                allKeyRanges = false;
                break;
            }
        }

        // Optimize if so
        if (allKeyRanges) {
            final KeyRanges intersection = KeyRanges.full();
            for (KeyFilter keyFilter : keyFilters)
                intersection.intersect((KeyRanges)keyFilter);
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
        Preconditions.checkArgument(keyFilter != null, "null keyFilter");
        return keyFilter.seekHigher(ByteData.empty()) == null;
    }

    private static ByteData seek(KeyFilter[] keyFilters, ByteData key, boolean seekHigher, boolean preferHigher) {
        Preconditions.checkArgument(key != null, "null key");
        assert keyFilters.length > 0;
        final boolean preferNull = seekHigher == preferHigher;
        ByteData best = null;
        for (int i = 0; i < keyFilters.length; i++) {
            final KeyFilter keyFilter = keyFilters[i];
            final ByteData next = seekHigher ? keyFilter.seekHigher(key) : keyFilter.seekLower(key);
            assert next == null || (seekHigher ? next.compareTo(key) >= 0 : key.isEmpty() || next.compareTo(key) <= 0);
            if (i == 0)
                best = next;
            if (next == null) {
                if (preferNull)
                    return null;
                continue;
            }
            assert !next.isEmpty() || key.isEmpty();
            if (i > 0 && best != null) {
                final int diff = (!seekHigher && next.isEmpty()) ? 1 : next.compareTo(best);
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
        public boolean contains(ByteData key) {
            for (KeyFilter keyFilter : this.keyFilters) {
                if (keyFilter.contains(key))
                    return true;
            }
            return false;
        }

        @Override
        public ByteData seekHigher(ByteData key) {
            return KeyFilterUtil.seek(this.keyFilters, key, true, false);
        }

        @Override
        public ByteData seekLower(ByteData key) {
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
        public boolean contains(ByteData key) {
            for (KeyFilter keyFilter : this.keyFilters) {
                if (!keyFilter.contains(key))
                    return false;
            }
            return true;
        }

        @Override
        public ByteData seekHigher(ByteData key) {
            return KeyFilterUtil.seek(this.keyFilters, key, true, true);
        }

        @Override
        public ByteData seekLower(ByteData key) {
            return KeyFilterUtil.seek(this.keyFilters, key, false, false);
        }
    }
}
