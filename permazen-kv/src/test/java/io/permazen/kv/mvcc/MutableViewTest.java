
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import com.google.common.collect.Lists;

import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyRange;
import io.permazen.kv.KeyRanges;
import io.permazen.kv.util.MemoryKVStore;
import io.permazen.kv.util.UnmodifiableKVStore;
import io.permazen.test.TestSupport;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;

import java.util.Iterator;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MutableViewTest extends TestSupport {

    private static final ByteData KEY_00 = ByteData.of(0x00);
    private static final ByteData KEY_10 = ByteData.of(0x10);
    private static final ByteData KEY_20 = ByteData.of(0x20);
    private static final ByteData KEY_30 = ByteData.of(0x30);
    private static final ByteData KEY_40 = ByteData.of(0x40);
    private static final ByteData KEY_50 = ByteData.of(0x50);
    private static final ByteData KEY_60 = ByteData.of(0x60);
    private static final ByteData KEY_70 = ByteData.of(0x70);
    private static final ByteData KEY_80 = ByteData.of(0x80);
    private static final ByteData KEY_8000 = ByteData.of(0x80, 0x00);
    private static final ByteData KEY_800000 = ByteData.of(0x80, 0x00, 0x00);
    private static final ByteData KEY_81 = ByteData.of(0x81);
    private static final ByteData KEY_82 = ByteData.of(0x82);
    private static final ByteData KEY_83 = ByteData.of(0x83);
    private static final ByteData KEY_84 = ByteData.of(0x84);
    private static final ByteData KEY_90 = ByteData.of(0x90);
    private static final ByteData KEY_9000 = ByteData.of(0x90, 0x00);
    private static final ByteData KEY_900000 = ByteData.of(0x90, 0x00, 0x00);
    private static final ByteData KEY_91 = ByteData.of(0x91);
    private static final ByteData KEY_A0 = ByteData.of(0xa0);
    private static final ByteData KEY_B0 = ByteData.of(0xb0);
    private static final ByteData KEY_C0 = ByteData.of(0xc0);
    private static final ByteData KEY_D0 = ByteData.of(0xd0);
    private static final ByteData KEY_E0 = ByteData.of(0xe0);
    private static final ByteData KEY_F0 = ByteData.of(0xf0);
    private static final ByteData KEY_F8 = ByteData.of(0xf0);       // counter

    private static final ByteData VAL_01 = ByteData.of(0x01);
    private static final ByteData VAL_02 = ByteData.of(0x02);
    private static final ByteData VAL_03 = ByteData.of(0x03);
    private static final ByteData VAL_04 = ByteData.of(0x04);
    private static final ByteData VAL_05 = ByteData.of(0x05);
    private static final ByteData VAL_06 = ByteData.of(0x06);
    private static final ByteData VAL_07 = ByteData.of(0x07);

    private static final ByteData COUNTER_0 = ByteData.zeros(8);

    private void setup(KVStore kv) {
        kv.removeRange(null, null);
        kv.put(KEY_20, VAL_01);
        kv.put(KEY_40, VAL_02);
        kv.put(KEY_60, VAL_03);
        kv.put(KEY_80, VAL_04);
        kv.put(KEY_A0, VAL_05);
        kv.put(KEY_C0, VAL_06);
        kv.put(KEY_E0, VAL_07);
        kv.put(KEY_F8, COUNTER_0);
    }

// VIEW

    @Test
    public void testReadTracking() throws Exception {
        final KVStore kvstore = new MemoryKVStore();
        final MutableView mv = new MutableView(kvstore);

        // Define three key ranges
        final KeyRange read1 = new KeyRange(ByteData.fromHex("0101"), ByteData.fromHex("0202"));
        final KeyRange read2 = new KeyRange(ByteData.fromHex("0303"), ByteData.fromHex("0404"));
        final KeyRange read3 = new KeyRange(ByteData.fromHex("0505"), ByteData.fromHex("0606"));

        // Do 1st read with tracking enabled and verify reads
        mv.getRange(read1.getMin(), read1.getMax(), false).hasNext();
        Assert.assertEquals(mv.getReads(), new Reads(new KeyRanges(read1)));

        // Do 2nd read with tracking disabled and verify reads
        mv.withoutReadTracking(false, () -> mv.getRange(read2.getMin(), read2.getMax(), false).hasNext());
        Assert.assertEquals(mv.getReads(), new Reads(new KeyRanges(read1)));

        // Do 3rd read with tracking enabled and verify reads
        mv.getRange(read3.getMin(), read3.getMax(), false).hasNext();
        Assert.assertEquals(mv.getReads(), new Reads(new KeyRanges(read1, read3)));

        // Verify writes are disallowed
        try {
            mv.withoutReadTracking(false, () -> mv.put(read1.getMin(), read1.getMax()));
            assert false : "expected exception but didn't get one";
        } catch (IllegalStateException e) {
            this.log.debug("got expected {}", e.toString());
        }
    }

    @Test
    public void testRandomWrites() throws Exception {
        KVStore kvstore = new MemoryKVStore();
        KVStore expected = new MemoryKVStore();
        MutableView mv = new MutableView(kvstore);
        for (int i = 0; i < 100000; i++) {

            // Get key(s) and value
            ByteData minKey;
            ByteData maxKey;
            do {
                final byte[] minKeyBytes = new byte[1 << this.random.nextInt(4)];
                this.random.nextBytes(minKeyBytes);
                final byte[] maxKeyBytes = this.random.nextInt(7) != 0 ? new byte[1 << this.random.nextInt(4)] : null;
                if (maxKeyBytes != null)
                    this.random.nextBytes(maxKeyBytes);
                minKey = ByteData.of(minKeyBytes);
                maxKey = maxKeyBytes != null ? ByteData.of(maxKeyBytes) : null;
            } while (maxKey != null && maxKey.compareTo(minKey) < 0);
            final byte[] valueBytes = new byte[1 << this.random.nextInt(2)];
            this.random.nextBytes(valueBytes);
            ByteData value = ByteData.of(valueBytes);

            // Mutate
            final int choice = this.random.nextInt(85);
            if (choice < 10) {
                value = mv.get(minKey);
                ByteData evalue = expected.get(minKey);
                Assert.assertEquals(value, evalue);
            } else if (choice < 20) {
                KVPair pair = mv.getAtLeast(minKey, maxKey);
                KVPair epair = expected.getAtLeast(minKey, maxKey);
                Assert.assertEquals(pair, epair);
            } else if (choice < 30) {
                KVPair pair = mv.getAtMost(maxKey, minKey);
                KVPair epair = expected.getAtMost(maxKey, minKey);
                Assert.assertEquals(pair, epair);
            } else if (choice < 40) {
                final boolean reverse = this.random.nextBoolean();
                if (this.random.nextInt(10) == 0)
                    minKey = null;
                if (this.random.nextInt(10) == 0)
                    maxKey = null;
                final List<KVPair> alist = Lists.newArrayList(mv.getRange(minKey, maxKey, reverse));
                final List<KVPair> elist = Lists.newArrayList(expected.getRange(minKey, maxKey, reverse));
                Assert.assertEquals(alist, elist, "iterations differ:\n  alist=" + alist + "\n  elist=" + elist + "\n");
            } else if (choice < 60) {
                mv.put(minKey, value);
                expected.put(minKey, value);
                if (this.log.isTraceEnabled())
                    this.log.trace("PUT: {} -> {}", ByteUtil.toString(minKey), ByteUtil.toString(value));
            } else if (choice < 70) {
                mv.remove(minKey);
                expected.remove(minKey);
                if (this.log.isTraceEnabled())
                    this.log.trace("REMOVE: {}", ByteUtil.toString(minKey));
            } else if (choice < 80) {
                mv.removeRange(minKey, maxKey);
                expected.removeRange(minKey, maxKey);
                if (this.log.isTraceEnabled())
                    this.log.trace("REMOVE_RANGE: {}, {}", ByteUtil.toString(minKey), ByteUtil.toString(maxKey));
            } else {
                mv.getWrites().applyTo(kvstore);
                mv = new MutableView(kvstore);
            }

            // Verify
            final boolean reverse = this.random.nextBoolean();
            final List<KVPair> alist = Lists.newArrayList(mv.getRange(null, null, reverse));
            final List<KVPair> elist = Lists.newArrayList(expected.getRange(null, null, reverse));
            Assert.assertEquals(alist, elist, "contents differ:\n  alist=" + alist + "\n  elist=" + elist + "\n");
        }
    }

    @Test
    public void testClone() throws Exception {

        final MutableView view = new MutableView(new MemoryKVStore());
        view.getWrites().getRemoves().add(KeyRanges.forPrefix(ByteData.fromHex("3311")));

        final MutableView view2 = new MutableView(view.getBaseKVStore(), view.getWrites().readOnlySnapshot());
        try {
            view2.getWrites().getRemoves().add(KeyRanges.forPrefix(ByteData.fromHex("4455")));
            assert false;
        } catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            view2.getWrites().getPuts().put(ByteData.fromHex("01"), ByteData.fromHex("23"));
            assert false;
        } catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            view2.getWrites().getAdjusts().put(ByteData.fromHex("45"), 6789L);
            assert false;
        } catch (UnsupportedOperationException e) {
            // expected
        }

        final MutableView view3 = view2.clone();
        view3.getWrites().getRemoves().add(KeyRanges.forPrefix(ByteData.fromHex("6677")));
        view3.getWrites().getPuts().put(ByteData.fromHex("01"), ByteData.fromHex("23"));
        view3.getWrites().getAdjusts().put(ByteData.fromHex("45"), 6789L);
    }

// CONFLICTS

    @Test(dataProvider = "conflicts")
    public void testConflict(List<Access> access1, List<Access> access2, boolean expected) {

        // Set up KVStore
        final MemoryKVStore kv = new MemoryKVStore();
        this.setup(kv);

        // Create views
        final MutableView v1 = new MutableView(new UnmodifiableKVStore(kv));
        final MutableView v2 = new MutableView(new UnmodifiableKVStore(kv));

        // Apply accesses
        for (Access access : access1)
            access.apply(v1);
        for (Access access : access2)
            access.apply(v2);

        // Check conflicts
        final boolean actual = v2.getReads().isConflict(v1.getWrites());
        Assert.assertEquals(actual, expected);
    }

    @DataProvider(name = "conflicts")
    private Object[][] conflictTests() throws Exception {
        return new Object[][] {

        // Read same key, write back different values -> conflict
        {
            buildList(get(KEY_20), put(KEY_20, VAL_01)),
            buildList(get(KEY_20), put(KEY_20, VAL_02)),
            true
        },

        // Read same key, write different values to different key -> no conflict
        {
            buildList(get(KEY_10), put(KEY_20, VAL_01)),
            buildList(get(KEY_10), put(KEY_20, VAL_02)),
            false
        },

        // Tx1 writes into a range, Tx2 scans the range -> conflict
        {
            buildList(put(KEY_81, VAL_01)),
            buildList(getRange(KEY_80, KEY_90)),
            true
        },

        // Tx1 and Tx2 write conflicting values without reading -> no conflict
        {
            buildList(put(KEY_81, VAL_01)),
            buildList(put(KEY_81, VAL_02)),
            false
        },

        // Tx1 reads an empty range, Tx2 removes that range -> no conflict
        {
            buildList(getRange(KEY_8000, KEY_90)),
            buildList(remove(KEY_70, KEY_A0)),
            false
        },

        // Tx1 and Tx2 both adjust a counter -> no conflict
        {
            buildList(adjust(KEY_F8, 123)),
            buildList(adjust(KEY_F8, 456)),
            false
        },

        // Tx2 performs no reads -> no conflict
        {
            buildList(getRange(null, null), put(KEY_20, VAL_03), remove(KEY_30)),
            buildList(remove(KEY_20), put(KEY_30, VAL_04)),
            false
        },

        // Tx1 adjusts, Tx2 removes -> no conflict
        {
            buildList(adjust(KEY_F8, 456)),
            buildList(remove(KEY_F8)),
            false
        },

    // Live read conflicts

        // Tx1 puts, Tx2 live reads -> conflict
        {
            buildList(put(KEY_80, VAL_03)),
            buildList(get(KEY_80)),
            true
        },

        // Tx1 removes, Tx2 live reads -> conflict
        {
            buildList(remove(KEY_80, KEY_8000)),
            buildList(get(KEY_80)),
            true
        },

        // Tx1 removes, Tx2 live reads -> conflict
        {
            buildList(remove(KEY_80, KEY_8000)),
            buildList(getRange(KEY_80, KEY_8000)),
            true
        },

        // Tx1 removes, Tx2 live reads -> conflict
        {
            buildList(remove(KEY_80, KEY_8000)),
            buildList(getRange(KEY_80, KEY_8000, true)),
            true
        },

        // Tx1 removes, Tx2 live reads -> conflict
        {
            buildList(remove(KEY_80, KEY_8000)),
            buildList(getRange(null, null)),
            true
        },

        // Tx1 removes, Tx2 live reads -> conflict
        {
            buildList(remove(KEY_80, KEY_8000)),
            buildList(getRange(null, null, true)),
            true
        },

        // Tx1 adjusts, Tx2 live reads -> conflict
        {
            buildList(adjust(KEY_F8, 123)),
            buildList(get(KEY_F8)),
            true
        },

    // Dead read conflicts

        // Tx1 puts, Tx2 dead reads -> conflict
        {
            buildList(put(KEY_90, VAL_03)),
            buildList(get(KEY_90)),
            true
        },

        // Tx1 removes, Tx2 dead reads -> conflict
        {
            buildList(remove(KEY_90, KEY_9000)),
            buildList(get(KEY_90)),
            true
        },

        // Tx1 removes, Tx2 dead reads -> conflict
        {
            buildList(remove(KEY_90, KEY_9000)),
            buildList(getRange(KEY_90, KEY_9000)),
            true
        },

        // Tx1 removes, Tx2 dead reads -> conflict
        {
            buildList(remove(KEY_90, KEY_9000)),
            buildList(getRange(KEY_90, KEY_9000, true)),
            true
        },

        // Tx1 removes, Tx2 dead reads -> conflict
        {
            buildList(remove(KEY_90, KEY_9000)),
            buildList(getRange(null, null)),
            true
        },

        // Tx1 removes, Tx2 dead reads -> conflict
        {
            buildList(remove(KEY_90, KEY_9000)),
            buildList(getRange(null, null, true)),
            true
        },

        // Tx1 adjusts, Tx2 dead reads -> conflict
        {
            buildList(adjust(KEY_F8, 123)),
            buildList(get(KEY_F8)),
            true
        },

        };
    }

// Accesses

    private abstract static class Access {

        private final String name;
        private final Object[] args;

        protected Access(String name, Object... args) {
            this.name = name;
            this.args = args;
        }

        public abstract void apply(KVStore kv);

        @Override
        public String toString() {
            final StringBuilder buf = new StringBuilder();
            buf.append(this.name);
            boolean first = true;
            for (Object arg : this.args) {
                if (arg instanceof ByteData)
                    arg = ((ByteData)arg).toHex();
                buf.append(first ? '[' : ',');
                buf.append(arg);
                first = false;
            }
            buf.append(']');
            return buf.toString();
        }
    }

    private static Access get(final ByteData key) {
        assert key != null;
        return new Access("GET", key) {
            @Override
            public void apply(KVStore kv) {
                kv.get(key);
            }
        };
    }

    private static Access getAtLeast(final ByteData min, final ByteData max) {
        return new Access("GETAL", min, max) {
            @Override
            public void apply(KVStore kv) {
                kv.getAtLeast(min, max);
            }
        };
    }

    private static Access getAtMost(final ByteData max, final ByteData min) {
        return new Access("GETAM", max, min) {
            @Override
            public void apply(KVStore kv) {
                kv.getAtMost(max, min);
            }
        };
    }

    private static Access getRange(final ByteData min, final ByteData max) {
        return MutableViewTest.getRange(min, max, false);
    }

    private static Access getRange(final ByteData min, final ByteData max, final boolean reverse) {
        return MutableViewTest.getRange(min, max, reverse, null);
    }

    private static Access getRange(final ByteData min, final ByteData max, final boolean reverse, final KeyRanges removes) {
        assert min == null || max == null || min.compareTo(max) <= 0;
        return new Access("GETRNG", min, max, reverse, removes) {
            @Override
            public void apply(KVStore kv) {
                for (Iterator<KVPair> i = kv.getRange(min, max, reverse); i.hasNext(); ) {
                    final KVPair pair = i.next();
                    if (removes != null && removes.contains(pair.getKey()))
                        i.remove();
                }
            }
        };
    }

    private static Access put(final ByteData key, final ByteData value) {
        assert key != null;
        assert value != null;
        return new Access("PUT", key, value) {
            @Override
            public void apply(KVStore kv) {
                kv.put(key, value);
            }
        };
    }

    private static Access remove(final ByteData key) {
        assert key != null;
        return new Access("REM", key) {
            @Override
            public void apply(KVStore kv) {
                kv.remove(key);
            }
        };
    }

    private static Access remove(final ByteData min, final ByteData max) {
        assert min == null || max == null || min.compareTo(max) <= 0;
        return new Access("REM", min, max) {
            @Override
            public void apply(KVStore kv) {
                kv.removeRange(min, max);
            }
        };
    }

    private static Access adjust(final ByteData key, final long value) {
        assert key != null;
        return new Access("ADJ", key, value) {
            @Override
            public void apply(KVStore kv) {
                kv.adjustCounter(key, value);
            }
        };
    }
}
