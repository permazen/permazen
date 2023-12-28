
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.util;

import io.permazen.core.CoreAPITestSupport;
import io.permazen.core.ObjId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.testng.annotations.Test;

public class ObjIdQueueTest extends CoreAPITestSupport {

    @Test
    public void testObjIdQueue() throws Exception {
        for (int i = 0; i < 100; i++) {

            final ObjIdSet ids = new ObjIdSet();
            final int numIds = this.random.nextInt(137) + (1 << this.random.nextInt(13));
            for (int j = 0; j < numIds; j++) {
                int shift = this.random.nextInt(56);
                final ObjId id = new ObjId(0x0100000000000000L | this.random.nextLong(1L << shift));
                ids.add(id);
            }

//            this.log.debug("ObjIdQueueTest: iteration #{}: ids={}", i, ids);

            final ObjIdQueue lifo = ObjIdQueues.lifo();
            final ObjIdQueue fifo = ObjIdQueues.fifo();
            final ObjIdQueue unor = ObjIdQueues.unordered();

            final ArrayList<ObjId> lifoResult = new ArrayList<>(ids.size());
            final ArrayList<ObjId> fifoResult = new ArrayList<>(ids.size());
            final ObjIdSet unorResult = new ObjIdSet();

            final ArrayList<ObjId> fifoExpect = new ArrayList<>(ids.size());
            final ArrayList<ObjId> lifoExpect = new ArrayList<>(ids.size());
            final ArrayDeque<ObjId> lifoStack = new ArrayDeque<>();
            final ObjIdSet unorExpect = new ObjIdSet();

            int addCount = 0;
            int nextCount = 0;

            while (!ids.isEmpty() || nextCount < addCount) {

                // Check empty status
                assert lifo.isEmpty() == (nextCount == addCount);
                assert fifo.isEmpty() == (nextCount == addCount);
                assert unor.isEmpty() == (nextCount == addCount);

                // Decide what to do
                final boolean add;
                if (ids.isEmpty())
                    add = false;
                else if (nextCount == addCount)
                    add = true;
                else
                    add = this.random.nextBoolean();

                // Do it and update results
                if (add) {

                    // Add next ObjId
                    final ObjId id = ids.removeOne();
//                    this.log.debug("ObjIdQueueTest: ADD {}", id);
                    lifo.add(id);
                    fifo.add(id);
                    unor.add(id);

                    // Update expected results
                    fifoExpect.add(id);
                    lifoStack.push(id);
                    unorExpect.add(id);

                    addCount++;
                } else {

                    // Remove next ObjId
                    final ObjId lifoNext = next(lifo);
                    final ObjId fifoNext = next(fifo);
                    final ObjId unorNext = next(unor);

//                    this.log.debug("ObjIdQueueTest: NEXT: LIFO={} FIFO={} UNOR={}", lifoNext, fifoNext, unorNext);

                    // Verify queues were not empty
                    assert lifoNext != null;
                    assert fifoNext != null;
                    assert unorNext != null;

                    // Update expected results
                    lifoExpect.add(lifoStack.pop());

                    // Record actual results
                    lifoResult.add(lifoNext);
                    fifoResult.add(fifoNext);
                    unorResult.add(unorNext);

                    nextCount++;
                }
            }

            // Verify queues are now empty
            assert next(lifo) == null;
            assert next(fifo) == null;
            assert next(unor) == null;

//            this.log.debug("ObjIdQueueTest:"
//                + "\n  FIFO-EXPECT: {}\n  FIFO-RESULT: {}"
//                + "\n  LIFO-EXPECT: {}\n  LIFO-RESULT: {}"
//                + "\n  UNOR-EXPECT: {}\n  UNOR-RESULT: {}",
//               fifoExpect, fifoResult,
//               lifoExpect, lifoResult,
//               unorExpect, unorResult);

            // Check results
            assert Objects.equals(fifoResult, fifoExpect);
            assert Objects.equals(lifoResult, lifoExpect);
            assert new java.util.HashSet<>(unorResult).equals(new java.util.HashSet<>(unorExpect));
            assert Objects.equals(unorResult, unorExpect);
        }
    }

    ObjId next(ObjIdQueue queue) {
        try {
            return queue.next();
        } catch (NoSuchElementException e) {
            return null;
        }
    }
}
