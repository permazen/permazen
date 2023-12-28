
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.kv.KeyRanges;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;
import io.permazen.test.TestSupport;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Set;

import org.testng.annotations.Test;

public class IndexTest2 extends CoreAPITestSupport {

    private Transaction buildTransaction() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo1\" storageId=\"1\">\n"
          + "    <ReferenceField name=\"ref\" storageId=\"10\">\n"
          + "        <ObjectTypes>\n"
          + "            <ObjectType name=\"Foo2\"/>\n"
          + "        </ObjectTypes>\n"
          + "    </ReferenceField>\n"
          + "  </ObjectType>\n"
          + "  <ObjectType name=\"Foo2\" storageId=\"2\">\n"
          + "    <ReferenceField name=\"ref\" storageId=\"10\">\n"
          + "        <ObjectTypes>\n"
          + "            <ObjectType name=\"Foo3\"/>\n"
          + "        </ObjectTypes>\n"
          + "    </ReferenceField>\n"
          + "  </ObjectType>\n"
          + "  <ObjectType name=\"Foo3\" storageId=\"3\">\n"
          + "    <ReferenceField name=\"ref\" storageId=\"10\">\n"
          + "        <ObjectTypes>\n"
          + "            <ObjectType name=\"Foo1\"/>\n"
          + "        </ObjectTypes>\n"
          + "    </ReferenceField>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        return db.createTransaction(schema);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReferenceEncodingIndex() throws Exception {

        Transaction tx = this.buildTransaction();

        ObjId foo1 = tx.create("Foo1");
        ObjId foo2 = tx.create("Foo2");
        ObjId foo3 = tx.create("Foo3");

        tx.writeSimpleField(foo1, "ref", foo2, false);
        tx.writeSimpleField(foo2, "ref", foo3, false);
        tx.writeSimpleField(foo3, "ref", foo1, false);

        CoreIndex1<ObjId, ObjId> index = (CoreIndex1<ObjId, ObjId>)tx.querySimpleIndex(10);

        final Set<ObjId> set1 = index.asMap().keySet();

        assert set1.contains(foo1) : set1 + " does not contain " + foo1;
        assert set1.contains(foo2) : set1 + " does not contain " + foo2;
        assert set1.contains(foo3) : set1 + " does not contain " + foo3;

        assert new ArrayList<>(set1).size() == 3;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReferenceEncodingFilter() throws Exception {

        Transaction tx = this.buildTransaction();

        // Create objects
        final ObjId[] foos = new ObjId[3];
        for (int i = 0; i < 3; i++)
            foos[i] = tx.create("Foo" + (i + 1));

        // Set refs
        for (int i = 0; i < 3; i++)
            tx.writeSimpleField(foos[i], "ref", foos[(i + 1) % 3], false);

        // Create key ranges for types Foo1, Foo2, Foo3
        final KeyRanges[] ranges = new KeyRanges[3];
        for (int i = 0; i < 3; i++)
            ranges[i] = new KeyRanges(ObjId.getKeyRange(i + 1));

        // Get index
        final CoreIndex1<ObjId, ObjId> index = (CoreIndex1<ObjId, ObjId>)tx.querySimpleIndex(10);

        // Test filtering index values
        for (int i = 0; i < 3; i++) {
            final Set<ObjId> set = index.filter(0, ranges[i]).asMap().keySet();
            assert set.size() == 1;
            assert set.equals(Set.of(foos[i]));
            for (int j = 0; j < 3; j++)
                assert set.contains(foos[j]) == (j == i);
        }

        // Test filtering index targets
        for (int i = 0; i < 3; i++) {
            final Set<ObjId> set = index.filter(1, ranges[i]).asMap().keySet();
            assert set.size() == 1;
            assert set.equals(Set.of(foos[(i + 1) % 3]));
            for (int j = 0; j < 3; j++)
                assert set.contains(foos[j]) == (j == (i + 1) % 3);
        }

        // Test overall index
        TestSupport.checkMap(index.asMap(), buildMap(
          foos[0],  Set.of(foos[2]),
          foos[1],  Set.of(foos[0]),
          foos[2],  Set.of(foos[1])));
    }
}
