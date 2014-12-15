
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
 * Utility methods for dealing with {@link KeyRanges}.
 *
 * <p>
 * Instances are immutable.
 * </p>
 *
 * @see KeyRanges
 */
public final class KeyRangesUtil {

    private KeyRangesUtil() {
    }

    /**
     * Create an instance that represents the union of the given instances.
     *
     * @param ranges instances to unify
     * @return the union of the given {@code ranges}
     * @throws IllegalArgumentException if {@code ranges} is empty
     * @throws IllegalArgumentException if {@code ranges} or any element in {@code ranges} is null
     */
    public static KeyRanges union(KeyRanges... ranges) {
        if (ranges == null)
            throw new IllegalArgumentException("null ranges");
        switch (ranges.length) {
        case 0:
            throw new IllegalArgumentException("empty ranges");
        case 1:
            return ranges[0];
        default:
            break;
        }
        boolean allSimple = true;
        for (KeyRanges keyRanges : ranges) {
            if (keyRanges == null)
                throw new IllegalArgumentException("null range");
            if (!(keyRanges instanceof SimpleKeyRanges))
                allSimple = false;
        }

        // Optimize when all are SimpleKeyRanges instances
        if (allSimple) {
            SimpleKeyRanges union = (SimpleKeyRanges)ranges[0];
            for (int i = 1; i < ranges.length; i++)
                union = union.union((SimpleKeyRanges)ranges[i]);
            return union;
        }
        return new UnionKeyRanges(ranges);
    }

    /**
     * Create an instance that represents the intersection of the given instances.
     *
     * @param ranges instances to intersect
     * @return the intersection of the given {@code ranges}
     * @throws IllegalArgumentException if {@code ranges} is empty
     * @throws IllegalArgumentException if {@code ranges} or any element in {@code ranges} is null
     */
    public static KeyRanges intersection(KeyRanges... ranges) {
        if (ranges == null)
            throw new IllegalArgumentException("null ranges");
        switch (ranges.length) {
        case 0:
            throw new IllegalArgumentException("empty ranges");
        case 1:
            return ranges[0];
        default:
            break;
        }
        boolean allSimple = true;
        for (KeyRanges keyRanges : ranges) {
            if (keyRanges == null)
                throw new IllegalArgumentException("null range");
            if (!(keyRanges instanceof SimpleKeyRanges))
                allSimple = false;
        }

        // Optimize when all are SimpleKeyRanges instances
        if (allSimple) {
            SimpleKeyRanges intersection = (SimpleKeyRanges)ranges[0];
            for (int i = 1; i < ranges.length; i++)
                intersection = intersection.intersection((SimpleKeyRanges)ranges[i]);
            return intersection;
        }
        return new IntersectionKeyRanges(ranges);
    }

    /**
     * Determine if the given instance is empty, i.e., contains no keys.
     *
     * @param ranges instance to check
     * @throws IllegalArgumentException if {@code ranges} is null
     */
    public static boolean isEmpty(KeyRanges ranges) {
        if (ranges == null)
            throw new IllegalArgumentException("null ranges");
        return ranges.nextHigherRange(ByteUtil.EMPTY) == null;
    }

    /**
     * Determine if the given instance is full, i.e., contains all keys.
     *
     * @param ranges instance to check
     * @throws IllegalArgumentException if {@code ranges} is null
     */
    public static boolean isFull(KeyRanges ranges) {
        if (ranges == null)
            throw new IllegalArgumentException("null ranges");
        return KeyRangesUtil.isEmpty(ranges.inverse());
    }

    /**
     * Determine if the first instance contains the second, i.e., all keys contained by
     * the second instance are also contained by the first.
     *
     * @param bigger containing instance
     * @param smaller contained instance
     * @return true if {@code bigger} wholly contains {@code smaller}
     * @throws IllegalArgumentException if either parameter is null
     */
    public static boolean contains(KeyRanges bigger, KeyRanges smaller) {
        if (bigger == null)
            throw new IllegalArgumentException("null bigger");
        if (smaller == null)
            throw new IllegalArgumentException("null smaller");
        return KeyRangesUtil.isEmpty(KeyRangesUtil.intersection(bigger.inverse(), smaller));
    }

// UnionKeyRanges

    private static class UnionKeyRanges implements KeyRanges {

        private final KeyRanges[] ranges;

        UnionKeyRanges(KeyRanges[] ranges) {
            final ArrayList<KeyRanges> list = new ArrayList<>();
            for (KeyRanges keyRanges : ranges) {
                if (keyRanges instanceof UnionKeyRanges)
                    list.addAll(Arrays.asList(((UnionKeyRanges)keyRanges).ranges));
                else
                    list.add(keyRanges);
            }
            this.ranges = list.toArray(new KeyRanges[list.size()]);
        }

        @Override
        public boolean contains(byte[] key) {
            for (KeyRanges keyRanges : this.ranges) {
                if (keyRanges.contains(key))
                    return true;
            }
            return false;
        }

        @Override
        public KeyRanges inverse() {
            final ArrayList<KeyRanges> inverses = new ArrayList<>(this.ranges.length);
            for (KeyRanges keyRanges : this.ranges)
                inverses.add(keyRanges.inverse());
            return KeyRangesUtil.intersection(inverses.toArray(new KeyRanges[inverses.size()]));
        }

        @Override
        public KeyRange nextHigherRange(byte[] key) {
            return this.nextRange(key, true);
        }

        @Override
        public KeyRange nextLowerRange(byte[] key) {
            return this.nextRange(key, false);
        }

        private KeyRange nextRange(byte[] key, boolean higher) {
            if (key == null)
                throw new IllegalArgumentException("null key");
            SimpleKeyRanges union = null;
            for (KeyRanges keyRanges : this.ranges) {
                final KeyRange nextRange = higher ? keyRanges.nextHigherRange(key) : keyRanges.nextLowerRange(key);
                if (nextRange != null) {
                    final SimpleKeyRanges nextRanges = new SimpleKeyRanges(nextRange);
                    union = union != null ? union.union(nextRanges) : nextRanges;
                }
            }
            return union.findKey(key)[higher ? 1 : 0];
        }
    }

// IntersectionKeyRanges

    private static class IntersectionKeyRanges implements KeyRanges {

        private final KeyRanges[] ranges;

        IntersectionKeyRanges(KeyRanges[] ranges) {
            final ArrayList<KeyRanges> list = new ArrayList<>();
            for (KeyRanges keyRanges : ranges) {
                if (keyRanges instanceof IntersectionKeyRanges)
                    list.addAll(Arrays.asList(((IntersectionKeyRanges)keyRanges).ranges));
                else
                    list.add(keyRanges);
            }
            this.ranges = list.toArray(new KeyRanges[list.size()]);
        }

        @Override
        public boolean contains(byte[] key) {
            for (KeyRanges keyRanges : this.ranges) {
                if (!keyRanges.contains(key))
                    return false;
            }
            return true;
        }

        @Override
        public KeyRanges inverse() {
            final ArrayList<KeyRanges> inverses = new ArrayList<>(this.ranges.length);
            for (KeyRanges keyRanges : this.ranges)
                inverses.add(keyRanges.inverse());
            return KeyRangesUtil.union(inverses.toArray(new KeyRanges[inverses.size()]));
        }

        @Override
        public KeyRange nextHigherRange(byte[] key) {
            return this.nextRange(key, true);
        }

        @Override
        public KeyRange nextLowerRange(byte[] key) {
            return this.nextRange(key, false);
        }

        private KeyRange nextRange(byte[] key, boolean higher) {
            if (key == null)
                throw new IllegalArgumentException("null key");
            SimpleKeyRanges intersection = null;
            for (KeyRanges keyRanges : this.ranges) {
                final KeyRange nextRange = higher ? keyRanges.nextHigherRange(key) : keyRanges.nextLowerRange(key);
                if (nextRange == null)
                    return null;
                final SimpleKeyRanges nextRanges = new SimpleKeyRanges(nextRange);
                intersection = intersection != null ? intersection.union(nextRanges) : nextRanges;
            }
            return intersection.findKey(key)[higher ? 1 : 0];
        }
    }
}

