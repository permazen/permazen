
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jsimpledb.TestSupport;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.testng.Assert;
import org.testng.annotations.Test;

public class XMLObjectSerializerTest extends TestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testXMLObjectSerializer() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"i\" type=\"int\" storageId=\"2\"/>\n"
          + "    <SimpleField name=\"z\" type=\"boolean\" storageId=\"3\"/>\n"
          + "    <SimpleField name=\"b\" type=\"byte\" storageId=\"4\"/>\n"
          + "    <SimpleField name=\"c\" type=\"char\" storageId=\"5\"/>\n"
          + "    <SimpleField name=\"s\" type=\"short\" storageId=\"6\"/>\n"
          + "    <SimpleField name=\"f\" type=\"float\" storageId=\"7\"/>\n"
          + "    <SimpleField name=\"j\" type=\"long\" storageId=\"8\"/>\n"
          + "    <SimpleField name=\"d\" type=\"double\" storageId=\"9\"/>\n"
          + "    <SimpleField name=\"str\" type=\"java.lang.String\" storageId=\"10\"/>\n"
          + "    <ReferenceField name=\"r\" storageId=\"11\"/>\n"
          + "    <SimpleField name=\"v\" type=\"java.lang.Void\" storageId=\"12\"/>\n"
          + "    <SimpleField name=\"date\" type=\"java.util.Date\" storageId=\"13\"/>\n"
          + "    <ReferenceField name=\"r2\" storageId=\"14\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        Transaction tx = db.createTransaction(schema1, 1, true);

        ObjId id1 = new ObjId("0100000000000001");
        tx.create(id1, 1);

        tx.writeSimpleField(id1, 2, 123, false);
        tx.writeSimpleField(id1, 3, true, false);
        tx.writeSimpleField(id1, 4, (byte)-7, false);
        tx.writeSimpleField(id1, 5, '\n', false);
        tx.writeSimpleField(id1, 6, (short)0, false);   // default value
        tx.writeSimpleField(id1, 7, 123.45f, false);
        tx.writeSimpleField(id1, 8, 99999999999L, false);
        tx.writeSimpleField(id1, 9, 123.45e37, false);
        tx.writeSimpleField(id1, 10, "hello dolly", false);
        tx.writeSimpleField(id1, 11, id1, false);
        tx.writeSimpleField(id1, 12, null, false);      // default value
        tx.writeSimpleField(id1, 13, new Date(1399604568000L), false);
        tx.writeSimpleField(id1, 14, null, false);

        XMLObjectSerializer s1 = new XMLObjectSerializer(tx);

        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        s1.write(buf, false, true);
        this.compareResult(tx, buf.toByteArray(), "test1.xml");

        buf.reset();
        s1.write(buf, true, true);
        this.compareResult(tx, buf.toByteArray(), "test2.xml");

        tx.commit();

        final SchemaModel schema2 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"20\">\n"
          + "    <SetField name=\"set\" storageId=\"21\">\n"
          + "        <SimpleField type=\"int\" storageId=\"22\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list\" storageId=\"23\">\n"
          + "        <SimpleField type=\"java.lang.Integer\" storageId=\"24\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map\" storageId=\"25\">\n"
          + "        <SimpleField type=\"int\" storageId=\"26\"/>\n"
          + "        <SimpleField type=\"java.lang.String\" storageId=\"27\" indexed=\"true\"/>\n"
          + "    </MapField>"
          + "    <ListField name=\"list2\" storageId=\"28\">\n"
          + "        <ReferenceField storageId=\"29\"/>\n"
          + "    </ListField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        tx = db.createTransaction(schema2, 2, true);

        ObjId id2 = new ObjId("1400000000000001");
        tx.create(id2, 2);

        Set<Integer> set = (Set<Integer>)tx.readSetField(id2, 21, false);
        set.add(123);
        set.add(456);

        List<Integer> list = (List<Integer>)tx.readListField(id2, 23, false);
        list.add(789);
        list.add(null);
        list.add(101112);

        Map<Integer, String> map = (Map<Integer, String>)tx.readMapField(id2, 25, false);
        map.put(55, "fifty\nfive");
        map.put(73, "seventy three");
        map.put(99, null);

        XMLObjectSerializer s2 = new XMLObjectSerializer(tx);

        buf.reset();
        s2.write(buf, false, true);
        this.compareResult(tx, buf.toByteArray(), "test3.xml");

        buf.reset();
        s2.write(buf, true, true);
        this.compareResult(tx, buf.toByteArray(), "test4.xml");

        tx.commit();
    }

    private void compareResult(Transaction tx, byte[] buf, String resource) throws Exception {

        // Read file
        String text = this.readResource(this.getClass().getResource(resource)).trim();

        // Compare generated XML to expected
        this.log.info("verifying XML output with \"" + resource + "\"");
        Assert.assertEquals(new String(buf, "UTF-8"), text);

        // Parse XML back into a snapshot transaction
        final Transaction stx = tx.createSnapshotTransaction();

        XMLObjectSerializer s = new XMLObjectSerializer(stx);
        s.read(new ByteArrayInputStream(text.getBytes("UTF-8")));

        // Compare transaction KV stores
        final Iterator<KVPair> i1 = tx.getKVTransaction().getRange(null, null, false);
        final Iterator<KVPair> i2 = stx.getKVTransaction().getRange(null, null, false);
        while (i1.hasNext() && i2.hasNext()) {
            final KVPair p1 = i1.next();
            final KVPair p2 = i2.next();
            Assert.assertEquals(p1.toString(), p2.toString());
        }
        Assert.assertTrue(!i1.hasNext());
        Assert.assertTrue(!i2.hasNext());
    }
}

