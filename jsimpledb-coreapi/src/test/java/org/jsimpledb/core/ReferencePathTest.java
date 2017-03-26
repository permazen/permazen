
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ReferencePathTest extends CoreAPITestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testReferencePaths() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final String schemaXML =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"100\">\n"
          + "    <SimpleField name=\"z\" type=\"boolean\" storageId=\"101\"/>\n"
          + "    <SimpleField name=\"b\" type=\"byte\" storageId=\"102\"/>\n"
          + "    <SimpleField name=\"c\" type=\"char\" storageId=\"103\"/>\n"
          + "    <SimpleField name=\"s\" type=\"short\" storageId=\"104\"/>\n"
          + "    <SimpleField name=\"i\" type=\"int\" storageId=\"105\"/>\n"
          + "    <SimpleField name=\"f\" type=\"float\" storageId=\"106\"/>\n"
          + "    <SimpleField name=\"j\" type=\"long\" storageId=\"107\"/>\n"
          + "    <SimpleField name=\"d\" type=\"double\" storageId=\"108\"/>\n"
          + "    <ReferenceField name=\"ref\" storageId=\"109\"/>\n"
          + "    <SimpleField name=\"str\" type=\"java.lang.String\" storageId=\"110\"/>\n"
          + "    <SetField name=\"set\" storageId=\"120\">\n"
          + "        <ReferenceField storageId=\"121\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list\" storageId=\"130\">\n"
          + "        <ReferenceField storageId=\"131\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map\" storageId=\"140\">\n"
          + "        <ReferenceField storageId=\"141\"/>\n"
          + "        <ReferenceField storageId=\"142\"/>\n"
          + "    </MapField>"
          + "  </ObjectType>\n"
          + "  <ObjectType name=\"Bar\" storageId=\"200\">\n"
          + "    <SimpleField name=\"i\" type=\"int\" storageId=\"105\"/>\n"
          + "    <ReferenceField name=\"ref\" storageId=\"109\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n";
        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream(schemaXML.getBytes("UTF-8")));

        Transaction tx = db.createTransaction(schema, 1, true);

        final ObjId id1 = new ObjId("6411111111111111");
        final ObjId id2 = new ObjId("6422222222222222");
        final ObjId id3 = new ObjId("6433333333333333");
        final ObjId id4 = new ObjId("6444444444444444");

        Assert.assertTrue(tx.create(id1));
        Assert.assertTrue(tx.create(id2));
        Assert.assertTrue(tx.create(id3));
        Assert.assertTrue(tx.create(id4));

        final NavigableSet<ObjId> set1 = (NavigableSet<ObjId>)tx.readSetField(id1, 120, true);
        final List<ObjId> list1 = (List<ObjId>)tx.readListField(id1, 130, true);
        final NavigableMap<ObjId, ObjId> map1 = (NavigableMap<ObjId, ObjId>)tx.readMapField(id1, 140, true);

        final NavigableSet<ObjId> set2 = (NavigableSet<ObjId>)tx.readSetField(id2, 120, true);
        final List<ObjId> list2 = (List<ObjId>)tx.readListField(id2, 130, true);
        final NavigableMap<ObjId, ObjId> map2 = (NavigableMap<ObjId, ObjId>)tx.readMapField(id2, 140, true);

        final NavigableSet<ObjId> set3 = (NavigableSet<ObjId>)tx.readSetField(id3, 120, true);
        final List<ObjId> list3 = (List<ObjId>)tx.readListField(id3, 130, true);
        final NavigableMap<ObjId, ObjId> map3 = (NavigableMap<ObjId, ObjId>)tx.readMapField(id3, 140, true);

        final NavigableSet<ObjId> set4 = (NavigableSet<ObjId>)tx.readSetField(id4, 120, true);
        final List<ObjId> list4 = (List<ObjId>)tx.readListField(id4, 130, true);
        final NavigableMap<ObjId, ObjId> map4 = (NavigableMap<ObjId, ObjId>)tx.readMapField(id4, 140, true);

        tx.writeSimpleField(id1, 109, id2, true);
        tx.writeSimpleField(id2, 109, id3, true);
        tx.writeSimpleField(id3, 109, id2, true);

        set1.addAll(Arrays.asList(id1, id3));
        set2.addAll(Arrays.asList(id2, id4));
        set3.addAll(Arrays.asList(id3));

        list1.addAll(Arrays.asList(id4, id3, id4));
        list2.addAll(Arrays.asList(id1, id1, id2));
        list3.addAll(Arrays.asList(id3, id3, id3));
        list4.addAll(Arrays.asList((ObjId)null));

        map1.put(null, id2);
        map1.put(id3, id1);
        map1.put(id1, id2);
        map1.put(id2, id4);
        map2.put(id3, null);
        map2.put(id2, id2);
        map2.put(id4, id3);
        map3.put(id3, null);
        map3.put(null, id3);
        map4.put(null, null);

        this.log.info("map1: " + map1);
        this.log.info("map2: " + map2);
        this.log.info("map3: " + map3);
        this.log.info("map4: " + map4);

    /*

        id1                      id2                      id3                      id4
         |                        |                        |                        |
         +- ref -> id2            +- ref -> id3            +- ref -> id2            +- ref -> null
         |                        |                        |                        |
         +- set                   +- set                   +- set                   +- set
         |   |                    |   |                    |   |                    |   |
         |   +-- id1              |   +-- id2              |   +-- id3              |   +-- null
         |   |                    |   |                    |                        |
         |   +-- id3              |   +-- id4              +- list                  +- list
         |                        |                        |   |                    |   |
         +- list                  +- list                  |   +-- id3              |   +-- null
         |   |                    |   |                    |   |                    |
         |   +-- id4              |   +-- id1              |   +-- id3              +- map
         |   |                    |   |                    |   |                        |
         |   +-- id3              |   +-- id1              |   +-- id3                  +-- { null, null }
         |   |                    |   |                    |
         |   +-- id4              |   +-- id2              +- map
         |                        |                            |
         +- map                   +- map                       +-- { id3, null }
             |                        |                        |
             +-- { null, id2 }        +-- { id3, null }        +-- { null, id3 }
             |                        |
             +-- { id3, id1 }         +-- { id2, id2 }
             |                        |
             +-- { id1, id2 }         +-- { id4, id3 }
             |
             +-- { id2, id4 }

        ref: 109    set: 121    list: 131   map.key: 141    map.value: 142
    */

    // Verify reference paths

        this.checkForward(tx, buildSet(id1), id1);
        this.checkForward(tx, buildSet(id1, id2), id1, id2);
        this.checkForward(tx, buildSet(id1, id3), id1, id3);
        this.checkForward(tx, buildSet(id1, id2, id3), id1, id2, id3);
        this.checkForward(tx, buildSet(id1, id4), id1, id4);
        this.checkForward(tx, buildSet(id4), id4);
        this.checkForward(tx, buildSet(id1, id2, id3, id4), id1, id2, id3, id4);

        this.checkReverse(tx, buildSet(id1), id1);
        this.checkReverse(tx, buildSet(id1, id2), id1, id2);
        this.checkReverse(tx, buildSet(id1, id3), id1, id3);
        this.checkReverse(tx, buildSet(id1, id2, id3), id1, id2, id3);
        this.checkReverse(tx, buildSet(id1, id4), id1, id4);
        this.checkReverse(tx, buildSet(id4), id4);
        this.checkReverse(tx, buildSet(id1, id2, id3, id4), id1, id2, id3, id4);

        this.checkForward(tx, buildSet(id2), id1, 109);
        this.checkForward(tx, buildSet(id2, id3), id1, id2, 109);
        this.checkForward(tx, buildSet(id2, id3), id1, id2, id3, 109);
        this.checkForward(tx, buildSet(id2, id3), id1, id2, id3, id4, 109);
        this.checkForward(tx, buildSet(), id4, 109);

        this.checkForward(tx, buildSet(), id1, -109);
        this.checkForward(tx, buildSet(id1, id3), id1, id2, -109);
        this.checkForward(tx, buildSet(id1, id2, id3), id1, id2, id3, -109);
        this.checkForward(tx, buildSet(id1, id2, id3), id1, id2, id3, id4, -109);
        this.checkForward(tx, buildSet(), id4, -109);

        this.checkReverse(tx, buildSet(), id1, 109);
        this.checkReverse(tx, buildSet(id1, id3), id1, id2, 109);
        this.checkReverse(tx, buildSet(id1, id2, id3), id1, id2, id3, 109);
        this.checkReverse(tx, buildSet(id1, id2, id3), id1, id2, id3, id4, 109);
        this.checkReverse(tx, buildSet(), id4, 109);

        this.checkForward(tx, buildSet(id1, id2, id3), id2, id4, id3, 141, -131, 121, -109);

        this.checkReverse(tx, buildSet(), id4, -141, 109, -121);

        this.checkReverse(tx, buildSet(id2, id4), id2, id4);
        this.checkReverse(tx, buildSet(id2, id4), id2, id4, -121);
        this.checkReverse(tx, buildSet(id1, id3), id2, id4, 109, -121);
        this.checkReverse(tx, buildSet(id1, id2, id3), id2, id4, -141, 109, -121);

    // Done

        tx.commit();
    }

    private void checkForward(Transaction tx, Set<?> expected, ObjId id1, int... path) {
        this.check(tx, true, new ObjId[] { id1 }, path, expected);
    }

    private void checkForward(Transaction tx, Set<?> expected, ObjId id1, ObjId id2, int... path) {
        this.check(tx, true, new ObjId[] { id1, id2 }, path, expected);
    }

    private void checkForward(Transaction tx, Set<?> expected, ObjId id1, ObjId id2, ObjId id3, int... path) {
        this.check(tx, true, new ObjId[] { id1, id2, id3 }, path, expected);
    }

    private void checkForward(Transaction tx, Set<?> expected, ObjId id1, ObjId id2, ObjId id3, ObjId id4, int... path) {
        this.check(tx, true, new ObjId[] { id1, id2, id3, id4 }, path, expected);
    }

    private void checkReverse(Transaction tx, Set<?> expected, ObjId id1, int... path) {
        this.check(tx, false, new ObjId[] { id1 }, path, expected);
    }

    private void checkReverse(Transaction tx, Set<?> expected, ObjId id1, ObjId id2, int... path) {
        this.check(tx, false, new ObjId[] { id1, id2 }, path, expected);
    }

    private void checkReverse(Transaction tx, Set<?> expected, ObjId id1, ObjId id2, ObjId id3, int... path) {
        this.check(tx, false, new ObjId[] { id1, id2, id3 }, path, expected);
    }

    private void checkReverse(Transaction tx, Set<?> expected, ObjId id1, ObjId id2, ObjId id3, ObjId id4, int... path) {
        this.check(tx, false, new ObjId[] { id1, id2, id3, id4 }, path, expected);
    }

    private void check(Transaction tx, boolean forward, ObjId[] ids, int[] path, Set<?> expected) {
        final List<ObjId> idList = Arrays.asList(ids);
        final NavigableSet<ObjId> actual = forward ? tx.followReferencePath(idList, path) : tx.invertReferencePath(path, idList);
        checkSet(actual, expected);
    }
}

