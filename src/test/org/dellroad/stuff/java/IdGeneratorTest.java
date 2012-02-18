
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

import org.dellroad.stuff.TestSupport;
import org.testng.annotations.Test;

public class IdGeneratorTest extends TestSupport {

    @Test
    public void testIdGenerator() {
        IdGenerator idg = new IdGenerator();

        Object obj1 = new Object();
        Object obj2 = new Object();
        Object obj3 = new Object();

        long id1 = idg.getId(obj1);
        long id2 = idg.getId(obj2);
        long id3 = idg.getId(obj3);

        assert id1 == 1;
        assert id2 == 2;
        assert id3 == 3;

        assert idg.getId(obj1) == id1;
        assert idg.getId(obj2) == id2;
        assert idg.getId(obj3) == id3;

        idg.setId(new Object(), 4);
        for (long i = 1; i <= 4; i++) {
            try {
                idg.setId(new Object(), i);
                assert false;
            } catch (IllegalArgumentException e) {
                // expected
            }
        }

        Object obj5 = new Object();
        idg.setId(obj5, 5);
        assert idg.getId(obj5) == 5;
        idg.setId(obj5, 5);
        assert idg.getId(obj5) == 5;
        idg.setId(obj5, 5);
        assert idg.getId(obj5) == 5;

        Integer int1 = new Integer(123);
        Integer int2 = new Integer(123);

        long int1id = idg.getId(int1);
        long int2id = idg.getId(int2);

        assert int1id != int2id;

        obj1 = null;
        obj2 = null;
        obj3 = null;
        int1 = null;
        int2 = null;

        System.gc();

        idg.flush();
    }
}

