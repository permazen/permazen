
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import io.permazen.core.CoreAPITestSupport;
import io.permazen.core.Database;
import io.permazen.kv.simple.SimpleKVDatabase;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.testng.Assert;
import org.testng.annotations.Test;

public class LockDownTest extends CoreAPITestSupport {

    @Test
    private void testLockDown() throws Exception {
        final String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"3\">\n"
          + "<ObjectType name=\"Foo\" storageId=\"100\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"10\"/>\n"
          + "  <EnumField name=\"e\" storageId=\"12\">\n"
          + "    <Identifier>FOO</Identifier>\n"
          + "    <Identifier>BAR</Identifier>\n"
          + "  </EnumField>\n"
          + "  <ReferenceField name=\"r\" storageId=\"13\">\n"
          + "    <ObjectTypes>\n"
          + "       <ObjectType storageId=\"100\"/>\n"
          + "    </ObjectTypes>\n"
          + "  </ReferenceField>\n"
          + "  <SetField name=\"set\" storageId=\"14\">\n"
          + "    <SimpleField name=\"element\" type=\"int\" storageId=\"15\"/>\n"
          + "  </SetField>\n"
          + "  <ListField name=\"list\" storageId=\"16\">\n"
          + "    <SimpleField name=\"element\" type=\"int\" storageId=\"17\"/>\n"
          + "  </ListField>\n"
          + "  <MapField name=\"map\" storageId=\"18\">\n"
          + "    <SimpleField name=\"key\" type=\"int\" storageId=\"19\"/>\n"
          + "    <SimpleField name=\"value\" type=\"int\" storageId=\"20\"/>\n"
          + "  </MapField>\n"
          + "  <EnumArrayField name=\"ea\" storageId=\"20\" dimensions=\"2\">\n"
          + "    <Identifier>AAA</Identifier>\n"
          + "    <Identifier>BBB</Identifier>\n"
          + "  </EnumArrayField>\n"
          + "  <CompositeIndex storageId=\"110\" name=\"ir\">\n"
          + "    <IndexedField storageId=\"10\"/>\n"
          + "    <IndexedField storageId=\"12\"/>\n"
          + "    <IndexedField storageId=\"13\"/>\n"
          + "  </CompositeIndex>\n"
          + "</ObjectType>\n"
          + "</Schema>\n";

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);
        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

        // Check calculations before lockdown
        schema.validate();
        final long hash1 = schema.compatibilityHash();

        // Lock down schema
        schema.lockDown();

        // Try to modify it
        try {
            schema.getSchemaObjectTypes().get(100).getSchemaFields().remove(12);
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            schema.getSchemaObjectTypes().remove(100);
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((SimpleSchemaField)schema.getSchemaObjectTypes().get(100).getSchemaFields().get(10)).setType(null);
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((SimpleSchemaField)schema.getSchemaObjectTypes().get(100).getSchemaFields().get(10)).setIndexed(false);
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((SimpleSchemaField)schema.getSchemaObjectTypes().get(100).getSchemaFields().get(10)).setEncodingSignature(123);
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((EnumSchemaField)schema.getSchemaObjectTypes().get(100).getSchemaFields().get(12)).getIdentifiers().clear();
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((EnumArraySchemaField)schema.getSchemaObjectTypes().get(100).getSchemaFields().get(20)).getIdentifiers().clear();
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((EnumArraySchemaField)schema.getSchemaObjectTypes().get(100).getSchemaFields().get(20)).setDimensions(123);
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((ReferenceSchemaField)schema.getSchemaObjectTypes().get(100).getSchemaFields().get(13)).setOnDelete(null);
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((ReferenceSchemaField)schema.getSchemaObjectTypes().get(100).getSchemaFields().get(13)).setCascadeDelete(true);
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((ReferenceSchemaField)schema.getSchemaObjectTypes().get(100).getSchemaFields().get(13)).setAllowDeleted(false);
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            schema.getSchemaObjectTypes().get(100).getSchemaCompositeIndexes().remove(110);
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }

        // Check calculations after lockdown
        schema.validate();
        final long hash2 = schema.compatibilityHash();
        Assert.assertEquals(hash2, hash1);

        schema.validate();
        final long hash3 = schema.compatibilityHash();
        Assert.assertEquals(hash3, hash1);

        // Clone it to make it modifiable again
        final SchemaModel schema2 = schema.clone();
        Assert.assertEquals(schema2, schema);

        schema2.validate();
        final long hash4 = schema2.compatibilityHash();
        Assert.assertEquals(hash4, hash1);

        // We should be able to modify the clone
        ((SimpleSchemaField)schema2.getSchemaObjectTypes().get(100).getSchemaFields().get(10)).setType(null);
        ((SimpleSchemaField)schema2.getSchemaObjectTypes().get(100).getSchemaFields().get(10)).setIndexed(false);
        ((SimpleSchemaField)schema2.getSchemaObjectTypes().get(100).getSchemaFields().get(10)).setEncodingSignature(123);
        ((EnumSchemaField)schema2.getSchemaObjectTypes().get(100).getSchemaFields().get(12)).getIdentifiers().clear();
        ((EnumArraySchemaField)schema2.getSchemaObjectTypes().get(100).getSchemaFields().get(20)).getIdentifiers().clear();
        ((EnumArraySchemaField)schema2.getSchemaObjectTypes().get(100).getSchemaFields().get(20)).setDimensions(123);
        ((ReferenceSchemaField)schema2.getSchemaObjectTypes().get(100).getSchemaFields().get(13)).setOnDelete(null);
        ((ReferenceSchemaField)schema2.getSchemaObjectTypes().get(100).getSchemaFields().get(13)).setCascadeDelete(true);
        ((ReferenceSchemaField)schema2.getSchemaObjectTypes().get(100).getSchemaFields().get(13)).setAllowDeleted(false);
        schema2.getSchemaObjectTypes().get(100).getSchemaCompositeIndexes().remove(110);
        schema2.getSchemaObjectTypes().get(100).getSchemaFields().remove(12);
        schema2.getSchemaObjectTypes().remove(100);
    }
}

