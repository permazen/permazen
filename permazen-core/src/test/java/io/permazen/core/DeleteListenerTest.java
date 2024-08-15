
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.primitives.Ints;

import io.permazen.kv.simple.MemoryKVDatabase;
import io.permazen.schema.SchemaModel;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DeleteListenerTest extends CoreAPITestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testDeleteListener() throws Exception {

        final Database db = new Database(new MemoryKVDatabase());

        final String xml =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\"/>\n"
          + "</Schema>\n";
        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

        final Transaction tx = db.createTransaction(schema);

        final ObjId id1 = tx.create("Foo");
        final ObjId id2 = tx.create("Foo");
        final ObjId id3 = tx.create("Foo");

        final int[] notify1 = new int[1];
        final int[] notify2 = new int[1];
        final int[] notify3 = new int[1];

        final DeleteListener listener = (tx2, id, path, referrers) -> {
            Assert.assertEquals(tx2, tx);
            if (id.equals(id1)) {
                notify1[0]++;
                tx.delete(id1);
            } else if (id.equals(id2)) {
                notify2[0]++;
                tx.delete(id2);
            } else if (id.equals(id3)) {
                notify3[0]++;
                tx.delete(id2);
            } else
                throw new AssertionError();
        };

        tx.addDeleteListener(new int[0], null, listener);
        tx.addDeleteListener(new int[0], null, listener);

        tx.delete(id1);

        tx.removeDeleteListener(new int[0], null, listener);

        tx.delete(id2);

        tx.addDeleteListener(new int[0], null, listener);

        tx.delete(id3);

        Assert.assertEquals(notify1[0], 1);
        Assert.assertEquals(notify2[0], 0);
        Assert.assertEquals(notify3[0], 1);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRemoteDeleteListener() throws Exception {

        final Database db = new Database(new MemoryKVDatabase());

        final String xml =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <ReferenceField name=\"ref\" storageId=\"2\" inverseDelete=\"NULLIFY\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n";
        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

        final Transaction tx = db.createTransaction(schema);

        final ObjId id1 = new ObjId(0x0111111111111111L);
        final ObjId id2 = new ObjId(0x0122222222222222L);
        final ObjId id3 = new ObjId(0x0133333333333333L);

        tx.create(id1);
        tx.create(id2);
        tx.create(id3);

        /*
                id1
                ^  \
               /    \
              /      v
            id3 <--- id2
        */

        tx.writeSimpleField(id1, "ref", id2, true);
        tx.writeSimpleField(id2, "ref", id3, true);
        tx.writeSimpleField(id3, "ref", id1, true);

        // Record notifications here
        record Notify(ObjId id, String path, Set<ObjId> referrers) {
            Notify(ObjId id, int[] path, Set<ObjId> referrers) {
                this(id, String.valueOf(Ints.asList(path)), new TreeSet<>(referrers));
            }
        }
        final ArrayList<Notify> list1 = new ArrayList<>();
        final ArrayList<Notify> list2 = new ArrayList<>();
        final ArrayList<Notify> list3 = new ArrayList<>();

        // Add listeners
        final DeleteListener listener1 = (tx2, id, path, referrers) -> list1.add(new Notify(id, path, referrers));
        final DeleteListener listener2 = (tx2, id, path, referrers) -> list2.add(new Notify(id, path, referrers));
        final DeleteListener listener3 = (tx2, id, path, referrers) -> list3.add(new Notify(id, path, referrers));

        tx.addDeleteListener(new int[] { }, null, listener1);
        tx.addDeleteListener(new int[] { 2 }, null, listener2);
        tx.addDeleteListener(new int[] { -2, -2, -2 }, null, listener3);

        // Test re-entrant deletion - this should have no effect
        tx.addDeleteListener(new int[0], null, (tx2, id, path, referrers) -> tx2.delete(id));

        tx.delete(id1);

        Assert.assertEquals(list1, List.of(
          new Notify(id1, new int[] { }, Set.of(id1))
        ));
        Assert.assertEquals(list2, List.of(
          new Notify(id1, new int[] { 2 }, Set.of(id3))
        ));
        Assert.assertEquals(list3, List.of(
          new Notify(id1, new int[] { -2, -2, -2 }, Set.of(id1))
        ));

        tx.delete(id3);

        Assert.assertEquals(list1, List.of(
          new Notify(id1, new int[] { }, Set.of(id1)),
          new Notify(id3, new int[] { }, Set.of(id3))
        ));
        Assert.assertEquals(list2, List.of(
          new Notify(id1, new int[] { 2 }, Set.of(id3)),
          new Notify(id3, new int[] { 2 }, Set.of(id2))
        ));
        Assert.assertEquals(list3, List.of(
          new Notify(id1, new int[] { -2, -2, -2 }, Set.of(id1))
        ));

        // Now refer id2 back to itself...
        tx.writeSimpleField(id2, "ref", id2, true);

        tx.delete(id2);

        Assert.assertEquals(list1, List.of(
          new Notify(id1, new int[] { }, Set.of(id1)),
          new Notify(id3, new int[] { }, Set.of(id3)),
          new Notify(id2, new int[] { }, Set.of(id2))
        ));
        Assert.assertEquals(list2, List.of(
          new Notify(id1, new int[] { 2 }, Set.of(id3)),
          new Notify(id3, new int[] { 2 }, Set.of(id2)),
          new Notify(id2, new int[] { 2 }, Set.of(id2))
        ));
        Assert.assertEquals(list3, List.of(
          new Notify(id1, new int[] { -2, -2, -2 }, Set.of(id1)),
          new Notify(id2, new int[] { -2, -2, -2 }, Set.of(id2))
        ));
    }
}
