
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;
import io.permazen.test.TestSupport;
import io.permazen.tuple.Tuple2;
import io.permazen.tuple.Tuple3;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.testng.annotations.Test;

public class CompositeIndexTest extends CoreAPITestSupport {

    @Test
    public void testCompositeIndex2() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "    <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"11\"/>\n"
          + "    <ReferenceField name=\"r\" storageId=\"12\"/>\n"
          + "    <CompositeIndex storageId=\"20\" name=\"ir\">\n"
          + "      <Field name=\"i\"/>\n"
          + "      <Field name=\"r\"/>\n"
          + "    </CompositeIndex>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        Transaction tx = db.createTransaction(schema1);

        ObjId id1 = new ObjId("0a11111111111111");
        ObjId id2 = new ObjId("0a22222222222222");
        ObjId id3 = new ObjId("0a33333333333333");
        ObjId id4 = new ObjId("0a44444444444444");
        ObjId id5 = new ObjId("0a55555555555555");

        tx.create(id1);
        tx.create(id2);
        tx.create(id3);
        tx.create(id4);
        tx.create(id5);

        tx.writeSimpleField(id1, "i", 555, true);
        tx.writeSimpleField(id1, "r", id3, true);

        tx.writeSimpleField(id2, "i", 555, true);
        tx.writeSimpleField(id2, "r", id4, true);

        tx.writeSimpleField(id3, "i", 666, true);
        tx.writeSimpleField(id3, "r", id3, true);

        tx.writeSimpleField(id4, "i", 666, true);
        tx.writeSimpleField(id4, "r", id4, true);

        // id5 same as id4
        tx.writeSimpleField(id5, "i", 666, true);
        tx.writeSimpleField(id5, "r", id4, true);

        final CompositeIndex ci = tx.getSchema().getObjType("Foo").getCompositeIndex("ir");

        final CoreIndex2<?, ?, ObjId> index = tx.queryCompositeIndex2(ci.getStorageId());
        TestSupport.checkSet(index.asSet(), buildSet(
          new Tuple3<>(555, id3, id1),
          new Tuple3<>(555, id4, id2),
          new Tuple3<>(666, id3, id3),
          new Tuple3<>(666, id4, id4),
          new Tuple3<>(666, id4, id5)));

        TestSupport.checkMap(index.asMap(), buildMap(
          new Tuple2<>(555, id3), buildSet(id1),
          new Tuple2<>(555, id4), buildSet(id2),
          new Tuple2<>(666, id3), buildSet(id3),
          new Tuple2<>(666, id4), buildSet(id4, id5)));

        TestSupport.checkSet(index.asMapOfIndex().get(555).asSet(), buildSet(
          new Tuple2<>(id3, id1),
          new Tuple2<>(id4, id2)));

        TestSupport.checkSet(index.asMapOfIndex().get(666).asSet(), buildSet(
          new Tuple2<>(id3, id3),
          new Tuple2<>(id4, id4),
          new Tuple2<>(id4, id5)));

        TestSupport.checkSet(index.asIndex().asSet(), buildSet(
          new Tuple2<>(555, id3),
          new Tuple2<>(555, id4),
          new Tuple2<>(666, id3),
          new Tuple2<>(666, id4)));

        tx.rollback();
    }
}
