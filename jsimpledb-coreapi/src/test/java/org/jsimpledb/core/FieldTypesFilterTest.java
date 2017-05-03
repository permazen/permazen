
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.collect.Lists;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import org.jsimpledb.core.type.UnsignedIntType;
import org.jsimpledb.kv.KVPairIterator;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.test.TestSupport;
import org.jsimpledb.tuple.Tuple2;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FieldTypesFilterTest extends CoreAPITestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testFieldTypesFilter() throws Exception {

        final SimpleKVDatabase kvdb = new SimpleKVDatabase();
        final Database db = new Database(kvdb);

        KVTransaction kvt = kvdb.createTransaction();

        kvt.put(b("aaaa0302"), b(""));
        kvt.put(b("aaaa0303"), b(""));

        kvt.put(b("aaaa0401"), b(""));
        kvt.put(b("aaaa0402"), b(""));
        kvt.put(b("aaaa0404"), b(""));

        kvt.put(b("aaaa0501"), b(""));
        kvt.put(b("aaaa0502"), b(""));
        kvt.put(b("aaaa0503"), b(""));
        kvt.put(b("aaaa0504"), b(""));

        final UnsignedIntType uintType = new UnsignedIntType();

        final FieldTypesFilter filter1 = new FieldTypesFilter(b("aaaa"), uintType, uintType);
        final KVPairIterator i1 = new KVPairIterator(kvt, KeyRange.forPrefix(b("aaaa04")), filter1, false);
        Assert.assertEquals(Lists.newArrayList(i1), Arrays.asList(
            kv("aaaa0401"),
            kv("aaaa0402"),
            kv("aaaa0404")
        ));

        final FieldTypesFilter filter2 = filter1.filter(1, new KeyRanges(b("02"), b("04")));
        final KVPairIterator i2 = new KVPairIterator(kvt, KeyRange.forPrefix(b("aaaa04")), filter2, false);
        Assert.assertEquals(Lists.newArrayList(i2), Arrays.asList(
            kv("aaaa0402")
        ));

        kvt.rollback();

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "    <SimpleField name=\"s\" type=\"java.lang.String\" storageId=\"11\" indexed=\"true\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        Transaction tx = db.createTransaction(schema1, 1, true);

        final ObjId id1 = new ObjId("0a11111111111111");
        final ObjId id2 = new ObjId("0a22222222222222");
        final ObjId id3 = new ObjId("0a33333333333333");

        tx.create(id1);
        tx.create(id2);
        tx.create(id3);

        tx.writeSimpleField(id1, 11, "aaa", true);
        tx.writeSimpleField(id2, 11, "aaa", true);
        tx.writeSimpleField(id3, 11, "bbb", true);

        final CoreIndex<String, ObjId> index = (CoreIndex<String, ObjId>)tx.queryIndex(11);
        TestSupport.checkSet(index.asSet(), buildSet(
          new Tuple2<>("aaa", id1),
          new Tuple2<>("aaa", id2),
          new Tuple2<>("bbb", id3)));
        TestSupport.checkMap(index.asMap(), buildMap(
          "aaa", buildSet(id1, id2),
          "bbb", buildSet(id3)));

        final CoreIndex<String, ObjId> index2 = index.filter(1, new KeyRanges(b("0a20"), b("0a30")));
        TestSupport.checkSet(index2.asSet(), buildSet(
          new Tuple2<>("aaa", id2)));
        TestSupport.checkMap(index2.asMap(), buildMap(
          "aaa", buildSet(id2)));

        tx.commit();
    }
}

