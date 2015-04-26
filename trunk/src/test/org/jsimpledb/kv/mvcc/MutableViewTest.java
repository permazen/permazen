
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.mvcc;

import java.util.Iterator;
import java.util.List;

import org.jsimpledb.TestSupport;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.util.NavigableMapKVStore;
import org.jsimpledb.kv.util.UnmodifiableKVStore;
import org.jsimpledb.util.ByteUtil;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MutableViewTest extends TestSupport {

    private static final byte[] KEY_00 = new byte[] { (byte)0x00 };
    private static final byte[] KEY_10 = new byte[] { (byte)0x10 };
    private static final byte[] KEY_20 = new byte[] { (byte)0x20 };
    private static final byte[] KEY_30 = new byte[] { (byte)0x30 };
    private static final byte[] KEY_40 = new byte[] { (byte)0x40 };
    private static final byte[] KEY_50 = new byte[] { (byte)0x50 };
    private static final byte[] KEY_60 = new byte[] { (byte)0x60 };
    private static final byte[] KEY_70 = new byte[] { (byte)0x70 };
    private static final byte[] KEY_80 = new byte[] { (byte)0x80 };
    private static final byte[] KEY_8000 = new byte[] { (byte)0x80, (byte)0x00 };
    private static final byte[] KEY_800000 = new byte[] { (byte)0x80, (byte)0x00, (byte)0x00 };
    private static final byte[] KEY_81 = new byte[] { (byte)0x81 };
    private static final byte[] KEY_82 = new byte[] { (byte)0x82 };
    private static final byte[] KEY_83 = new byte[] { (byte)0x83 };
    private static final byte[] KEY_84 = new byte[] { (byte)0x84 };
    private static final byte[] KEY_90 = new byte[] { (byte)0x90 };
    private static final byte[] KEY_9000 = new byte[] { (byte)0x90, (byte)0x00 };
    private static final byte[] KEY_900000 = new byte[] { (byte)0x90, (byte)0x00, (byte)0x00 };
    private static final byte[] KEY_91 = new byte[] { (byte)0x91 };
    private static final byte[] KEY_A0 = new byte[] { (byte)0xa0 };
    private static final byte[] KEY_B0 = new byte[] { (byte)0xb0 };
    private static final byte[] KEY_C0 = new byte[] { (byte)0xc0 };
    private static final byte[] KEY_D0 = new byte[] { (byte)0xd0 };
    private static final byte[] KEY_E0 = new byte[] { (byte)0xe0 };
    private static final byte[] KEY_F0 = new byte[] { (byte)0xf0 };
    private static final byte[] KEY_F8 = new byte[] { (byte)0xf0 };     // counter

    private static final byte[] VAL_01 = new byte[] { (byte)0x01 };
    private static final byte[] VAL_02 = new byte[] { (byte)0x02 };
    private static final byte[] VAL_03 = new byte[] { (byte)0x03 };
    private static final byte[] VAL_04 = new byte[] { (byte)0x04 };
    private static final byte[] VAL_05 = new byte[] { (byte)0x05 };
    private static final byte[] VAL_06 = new byte[] { (byte)0x06 };
    private static final byte[] VAL_07 = new byte[] { (byte)0x07 };

    private static final byte[] COUNTER_0 = new byte[8];

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

    @Test(dataProvider = "conflicts")
    public void testConflict(List<Access> access1, List<Access> access2, boolean expected) {

        // Set up KVStore
        final NavigableMapKVStore kv = new NavigableMapKVStore();
        this.setup(kv);

        // Create views
        final MutableView v1 = new MutableView(new UnmodifiableKVStore(kv), true);
        final MutableView v2 = new MutableView(new UnmodifiableKVStore(kv), true);

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

        // Tx1 removes, Tx2 dead reads -> no conflict
        {
            buildList(remove(KEY_90, KEY_9000)),
            buildList(get(KEY_90)),
            false
        },

        // Tx1 removes, Tx2 dead reads -> no conflict
        {
            buildList(remove(KEY_90, KEY_9000)),
            buildList(getRange(KEY_90, KEY_9000)),
            false
        },

        // Tx1 removes, Tx2 dead reads -> no conflict
        {
            buildList(remove(KEY_90, KEY_9000)),
            buildList(getRange(KEY_90, KEY_9000, true)),
            false
        },

        // Tx1 removes, Tx2 dead reads -> no conflict
        {
            buildList(remove(KEY_90, KEY_9000)),
            buildList(getRange(null, null)),
            false
        },

        // Tx1 removes, Tx2 dead reads -> no conflict
        {
            buildList(remove(KEY_90, KEY_9000)),
            buildList(getRange(null, null, true)),
            false
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
                if (arg instanceof byte[])
                    arg = s((byte[])arg);
                buf.append(first ? '[' : ',');
                buf.append(arg);
                first = false;
            }
            buf.append(']');
            return buf.toString();
        }
    }

    private static Access get(final byte[] key) {
        assert key != null;
        return new Access("GET", key) {
            @Override
            public void apply(KVStore kv) {
                kv.get(key);
            }
        };
    }

    private static Access getAtLeast(final byte[] min) {
        return new Access("GETAL", min) {
            @Override
            public void apply(KVStore kv) {
                kv.getAtLeast(min);
            }
        };
    }

    private static Access getAtMost(final byte[] max) {
        return new Access("GETAM", max) {
            @Override
            public void apply(KVStore kv) {
                kv.getAtMost(max);
            }
        };
    }

    private static Access getRange(final byte[] min, final byte[] max) {
        return MutableViewTest.getRange(min, max, false);
    }

    private static Access getRange(final byte[] min, final byte[] max, final boolean reverse) {
        return MutableViewTest.getRange(min, max, reverse, null);
    }

    private static Access getRange(final byte[] min, final byte[] max, final boolean reverse, final KeyRanges removes) {
        assert min == null || max == null || ByteUtil.compare(min, max) <= 0;
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

    private static Access put(final byte[] key, final byte[] value) {
        assert key != null;
        assert value != null;
        return new Access("PUT", key, value) {
            @Override
            public void apply(KVStore kv) {
                kv.put(key, value);
            }
        };
    }

    private static Access remove(final byte[] key) {
        assert key != null;
        return new Access("REM", key) {
            @Override
            public void apply(KVStore kv) {
                kv.remove(key);
            }
        };
    }

    private static Access remove(final byte[] min, final byte[] max) {
        assert min == null || max == null || ByteUtil.compare(min, max) <= 0;
        return new Access("REM", min, max) {
            @Override
            public void apply(KVStore kv) {
                kv.removeRange(min, max);
            }
        };
    }

    private static Access adjust(final byte[] key, final long value) {
        assert key != null;
        return new Access("ADJ", key, value) {
            @Override
            public void apply(KVStore kv) {
                kv.adjustCounter(key, value);
            }
        };
    }
}

