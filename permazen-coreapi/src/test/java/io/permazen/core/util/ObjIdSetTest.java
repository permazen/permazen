
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.util;

import com.google.common.collect.Iterators;

import java.util.HashSet;
import java.util.Iterator;

import io.permazen.core.CoreAPITestSupport;
import io.permazen.core.ObjId;
import io.permazen.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ObjIdSetTest extends CoreAPITestSupport {

    @Test
    public void testObjIdSet() throws Exception {

        final ObjIdSet actual = new ObjIdSet();
        final HashSet<ObjId> expected = new HashSet<>();

        for (int i = 0; i < 5000; i++) {
            int shift = 0;
            while (shift < 31 && this.random.nextBoolean())
                shift++;
            final ObjId id = new ObjId(0x0100000000000000L | this.random.nextInt(3 << shift));
            final int action = this.random.nextInt(100);
            boolean expectedResult = false;
            boolean actualResult = false;
            if (action < 3) {
                actual.clear();
                expected.clear();
            } else if (action < 6) {
                final int pos = this.random.nextInt(10);
                final Iterator<ObjId> iter = actual.iterator();
                final ObjId id2 = Iterators.get(iter, pos, null);
                if (id2 != null) {
                    iter.remove();
                    expected.remove(id2);
                }
            } else if (action < 45) {
                actualResult = actual.add(id);
                expectedResult = expected.add(id);
            } else if (action < 85) {
                actualResult = actual.remove(id);
                expectedResult = expected.remove(id);
            } else {
                actualResult = actual.contains(id);
                expectedResult = expected.contains(id);
            }
            TestSupport.checkSet(actual, expected);
            Assert.assertEquals(actualResult, expectedResult,
              "wrong result: actual=" + actual.debugDump() + " expected=" + expected);
        }
    }
}

