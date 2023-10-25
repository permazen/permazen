
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.util;

import io.permazen.core.CoreAPITestSupport;
import io.permazen.core.Database;
import io.permazen.core.EnumValue;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;
import io.permazen.kv.KVPair;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;
import io.permazen.util.CloseableIterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class XMLObjectSerializerTest extends CoreAPITestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testXMLObjectSerializer() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"3\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"2\"/>\n"
          + "    <SimpleField name=\"z\" encoding=\"urn:fdc:permazen.io:2020:boolean\" storageId=\"3\"/>\n"
          + "    <SimpleField name=\"b\" encoding=\"urn:fdc:permazen.io:2020:byte\" storageId=\"4\"/>\n"
          + "    <SimpleField name=\"c\" encoding=\"urn:fdc:permazen.io:2020:char\" storageId=\"5\"/>\n"
          + "    <SimpleField name=\"s\" encoding=\"urn:fdc:permazen.io:2020:short\" storageId=\"6\"/>\n"
          + "    <SimpleField name=\"f\" encoding=\"urn:fdc:permazen.io:2020:float\" storageId=\"7\"/>\n"
          + "    <SimpleField name=\"j\" encoding=\"urn:fdc:permazen.io:2020:long\" storageId=\"8\"/>\n"
          + "    <SimpleField name=\"d\" encoding=\"urn:fdc:permazen.io:2020:double\" storageId=\"9\"/>\n"
          + "    <SimpleField name=\"str\" encoding=\"urn:fdc:permazen.io:2020:String\" storageId=\"10\"/>\n"
          + "    <ReferenceField name=\"r\" storageId=\"11\"/>\n"
          + "    <SimpleField name=\"v\" encoding=\"urn:fdc:permazen.io:2020:Void\" storageId=\"12\"/>\n"
          + "    <SimpleField name=\"date\" encoding=\"urn:fdc:permazen.io:2020:Date\" storageId=\"13\"/>\n"
          + "    <ReferenceField name=\"r2\" storageId=\"14\"/>\n"
          + "    <EnumField name=\"e1\" storageId=\"15\">\n"
          + "      <Identifier>AAA</Identifier>\n"
          + "      <Identifier>BBB</Identifier>\n"
          + "      <Identifier>CCC</Identifier>\n"
          + "    </EnumField>\n"
          + "    <EnumArrayField name=\"ea1\" storageId=\"16\" dimensions=\"1\">\n"
          + "      <Identifier>DDD</Identifier>\n"
          + "      <Identifier>EEE</Identifier>\n"
          + "    </EnumArrayField>\n"
          + "    <EnumArrayField name=\"ea2\" storageId=\"17\" dimensions=\"2\">\n"
          + "      <Identifier>DDD</Identifier>\n"
          + "      <Identifier>EEE</Identifier>\n"
          + "    </EnumArrayField>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

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
        tx.writeSimpleField(id1, 15, new EnumValue("BBB", 1), false);
        tx.writeSimpleField(id1, 16, new EnumValue[] {
            new EnumValue("DDD", 0), null, new EnumValue("EEE", 1)
        }, false);
        tx.writeSimpleField(id1, 17, new EnumValue[][] {
            { new EnumValue("DDD", 0), null, new EnumValue("EEE", 1) },
            null,
            { },
            { null, new EnumValue("EEE", 1), null },
        }, false);

        XMLObjectSerializer s1 = new XMLObjectSerializer(tx);

        try {
            s1.setFieldTruncationLength(-2);
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }

        Assert.assertEquals(s1.getFieldTruncationLength(), -1);

        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        s1.write(buf, false, true);
        this.compareResult(tx, buf.toByteArray(), "test1.xml");

        // Check we properly derive object ID's when not specified explicitly
        this.compareParse(tx, this.readResource(this.getClass().getResource("test1b.xml")).trim());

        buf.reset();
        s1.write(buf, true, true);
        this.compareResult(tx, buf.toByteArray(), "test2.xml");

        buf.reset();
        s1.setFieldTruncationLength(6);
        s1.write(buf, true, true);
        this.compareResult(tx, buf.toByteArray(), "test2a.xml", false);

        buf.reset();
        s1.setFieldTruncationLength(0);
        s1.write(buf, true, true);
        this.compareResult(tx, buf.toByteArray(), "test2b.xml", false);

        tx.commit();

        s1.setFieldTruncationLength(-1);

        final SchemaModel schema2 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"20\">\n"
          + "    <SetField name=\"set\" storageId=\"21\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"22\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list\" storageId=\"23\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:Integer\" storageId=\"24\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map\" storageId=\"25\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"26\"/>\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:String\" storageId=\"27\" indexed=\"true\"/>\n"
          + "    </MapField>"
          + "    <ListField name=\"list2\" storageId=\"28\">\n"
          + "        <ReferenceField storageId=\"29\"/>\n"
          + "    </ListField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

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

        // Check we properly ignore XML tags in other namespaces
        this.compareParse(tx, this.readResource(this.getClass().getResource("test4b.xml")).trim());

        // Turn off "omitDefaultValueFields"
        s2.setOmitDefaultValueFields(false);
        tx.delete(id1);
        tx.create(id1, 1);
        buf.reset();
        s2.write(buf, true, true);
        this.compareResult(tx, buf.toByteArray(), "test4c.xml");

        tx.commit();
    }

    private void compareResult(Transaction tx, byte[] buf, String resource) throws Exception {
        this.compareResult(tx, buf, resource, true);
    }

    private void compareResult(Transaction tx, byte[] buf, String resource, boolean reparse) throws Exception {

        // Read file
        String text = this.readResource(this.getClass().getResource(resource)).trim();

        // Compare generated XML to expected
        this.log.info("verifying XML output with \"{}\"", resource);
        Assert.assertEquals(new String(buf, StandardCharsets.UTF_8), text);

        // Parse XML back into a snapshot transaction
        if (reparse)
            this.compareParse(tx, text);
    }

    private void compareParse(Transaction tx, String text) throws Exception {

        final Transaction stx = tx.createSnapshotTransaction();
        XMLObjectSerializer s = new XMLObjectSerializer(stx);
        s.read(new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)));

        // Compare transaction KV stores
        try (CloseableIterator<KVPair> i1 = tx.getKVTransaction().getRange(null, null);
            final CloseableIterator<KVPair> i2 = stx.getKVTransaction().getRange(null, null)) {
            while (i1.hasNext() && i2.hasNext()) {
                final KVPair p1 = i1.next();
                final KVPair p2 = i2.next();
                Assert.assertEquals(p1.toString(), p2.toString());
            }
            Assert.assertTrue(!i1.hasNext());
            Assert.assertTrue(!i2.hasNext());
        }
    }
}
