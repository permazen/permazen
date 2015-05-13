
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.mvcc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;

import org.jsimpledb.TestSupport;
import org.jsimpledb.kv.KeyRange;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class WritesTest extends TestSupport {

    @Test(dataProvider = "writes")
    public void testSerializeWrites(Writes writes) throws Exception {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        writes.serialize(output);
        final ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
        final Writes writes2 = Writes.deserialize(input);
        Assert.assertEquals(writes2.getRemoves(), writes.getRemoves());
        Assert.assertEquals(writes2.getPuts(), writes.getPuts());
        Assert.assertEquals(writes2.getAdjusts(), writes.getAdjusts());
        final ByteArrayOutputStream output2 = new ByteArrayOutputStream();
        writes2.serialize(output2);
        Assert.assertEquals(output2.toByteArray(), output.toByteArray());
    }

    @DataProvider(name = "writes")
    private Writes[][] genWrites() throws Exception {
        final ArrayList<Writes> list = new ArrayList<>();

        final Writes writes1 = new Writes();
        list.add(writes1);

        final Writes writes2 = new Writes();
        writes2.setRemoves(writes2.getRemoves().add(KeyRange.forPrefix(b("6666"))));
        writes2.setRemoves(writes2.getRemoves().add(new KeyRange(b("003333"), b("004444"))));
        list.add(writes2);

        final Writes writes3 = new Writes();
        writes3.getPuts().put(b("1234"), b("5678"));
        writes3.getPuts().put(b("3333"), b("4444"));
        list.add(writes3);

        final Writes writes4 = new Writes();
        writes4.getAdjusts().put(b("77777777"), 1234567890L);
        writes4.getAdjusts().put(b("99999999"), Long.MIN_VALUE);
        list.add(writes4);

        final Writes[][] array = new Writes[list.size()][];
        for (int i = 0; i < list.size(); i++)
            array[i] = new Writes[] { list.get(i) };
        return array;
    }
}

