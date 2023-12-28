
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
import io.permazen.schema.SchemaId;
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
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\"/>\n"
          + "    <SimpleField name=\"z\" encoding=\"urn:fdc:permazen.io:2020:boolean\"/>\n"
          + "    <SimpleField name=\"b\" encoding=\"urn:fdc:permazen.io:2020:byte\"/>\n"
          + "    <SimpleField name=\"c\" encoding=\"urn:fdc:permazen.io:2020:char\"/>\n"
          + "    <SimpleField name=\"s\" encoding=\"urn:fdc:permazen.io:2020:short\"/>\n"
          + "    <SimpleField name=\"f\" encoding=\"urn:fdc:permazen.io:2020:float\"/>\n"
          + "    <SimpleField name=\"j\" encoding=\"urn:fdc:permazen.io:2020:long\"/>\n"
          + "    <SimpleField name=\"d\" encoding=\"urn:fdc:permazen.io:2020:double\"/>\n"
          + "    <SimpleField name=\"str\" encoding=\"urn:fdc:permazen.io:2020:String\"/>\n"
          + "    <ReferenceField name=\"r\"/>\n"
          + "    <SimpleField name=\"v\" encoding=\"urn:fdc:permazen.io:2020:Void\"/>\n"
          + "    <SimpleField name=\"date\" encoding=\"urn:fdc:permazen.io:2020:Date\"/>\n"
          + "    <ReferenceField name=\"r2\"/>\n"
          + "    <EnumField name=\"e1\">\n"
          + "      <Identifier>AAA</Identifier>\n"
          + "      <Identifier>BBB</Identifier>\n"
          + "      <Identifier>CCC</Identifier>\n"
          + "    </EnumField>\n"
          + "    <EnumArrayField name=\"ea1\" dimensions=\"1\">\n"
          + "      <Identifier>DDD</Identifier>\n"
          + "      <Identifier>EEE</Identifier>\n"
          + "    </EnumArrayField>\n"
          + "    <EnumArrayField name=\"ea2\" dimensions=\"2\">\n"
          + "      <Identifier>DDD</Identifier>\n"
          + "      <Identifier>EEE</Identifier>\n"
          + "    </EnumArrayField>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));
        schema1.lockDown(true);
        final SchemaId schemaId1 = schema1.getSchemaId();

        Transaction tx = db.createTransaction(schema1);

        ObjId id1 = new ObjId("0100000000000001");
        tx.create(id1, schemaId1);

        tx.writeSimpleField(id1, "i", 123, false);
        tx.writeSimpleField(id1, "z", true, false);
        tx.writeSimpleField(id1, "b", (byte)-7, false);
        tx.writeSimpleField(id1, "c", '\n', false);
        tx.writeSimpleField(id1, "s", (short)0, false);   // default value
        tx.writeSimpleField(id1, "f", 123.45f, false);
        tx.writeSimpleField(id1, "j", 99999999999L, false);
        tx.writeSimpleField(id1, "d", 123.45e37, false);
        tx.writeSimpleField(id1, "str", "hello dolly", false);
        tx.writeSimpleField(id1, "r", id1, false);
        tx.writeSimpleField(id1, "v", null, false);      // default value
        tx.writeSimpleField(id1, "date", new Date(1399604568000L), false);
        tx.writeSimpleField(id1, "r2", null, false);
        tx.writeSimpleField(id1, "e1", new EnumValue("BBB", 1), false);
        tx.writeSimpleField(id1, "ea1", new EnumValue[] {
            new EnumValue("DDD", 0), null, new EnumValue("EEE", 1)
        }, false);
        tx.writeSimpleField(id1, "ea2", new EnumValue[][] {
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
          + "<Schema>\n"
          + "  <ObjectType name=\"Bar\" storageId=\"20\">\n"
          + "    <SetField name=\"set\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:Integer\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map\">\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\"/>\n"
          + "        <SimpleField encoding=\"urn:fdc:permazen.io:2020:String\" indexed=\"true\"/>\n"
          + "    </MapField>"
          + "    <ListField name=\"list2\">\n"
          + "        <ReferenceField/>\n"
          + "    </ListField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));
        schema2.lockDown(true);
        final SchemaId schemaId2 = schema2.getSchemaId();

        tx = db.createTransaction(schema2);

        ObjId id2 = new ObjId("1400000000000001");
        tx.create(id2, schemaId2);

        Set<Integer> set = (Set<Integer>)tx.readSetField(id2, "set", false);
        set.add(123);
        set.add(456);

        List<Integer> list = (List<Integer>)tx.readListField(id2, "list", false);
        list.add(789);
        list.add(null);
        list.add(101112);

        Map<Integer, String> map = (Map<Integer, String>)tx.readMapField(id2, "map", false);
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
        tx.create(id1, schemaId1);
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
        this.assertSameOrDiff(text, new String(buf, StandardCharsets.UTF_8));

        // Parse XML back into a detached transaction
        if (reparse)
            this.compareParse(tx, text);
    }

    private void compareParse(Transaction tx, String text) throws Exception {

        final Transaction stx = tx.createDetachedTransaction();
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
