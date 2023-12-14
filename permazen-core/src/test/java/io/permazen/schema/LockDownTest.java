
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
          + "<Schema>\n"
          + "<ObjectType name=\"Foo\" storageId=\"100\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"10\"/>\n"
          + "  <EnumField name=\"e\" storageId=\"12\">\n"
          + "    <Identifier>FOO</Identifier>\n"
          + "    <Identifier>BAR</Identifier>\n"
          + "  </EnumField>\n"
          + "  <ReferenceField name=\"r\" storageId=\"13\">\n"
          + "    <ObjectTypes>\n"
          + "       <ObjectType name=\"Foo\"/>\n"
          + "    </ObjectTypes>\n"
          + "  </ReferenceField>\n"
          + "  <SetField name=\"set\" storageId=\"14\">\n"
          + "    <SimpleField name=\"element\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"15\"/>\n"
          + "  </SetField>\n"
          + "  <ListField name=\"list\" storageId=\"16\">\n"
          + "    <SimpleField name=\"element\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"17\"/>\n"
          + "  </ListField>\n"
          + "  <MapField name=\"map\" storageId=\"18\">\n"
          + "    <SimpleField name=\"key\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"19\"/>\n"
          + "    <SimpleField name=\"value\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"20\"/>\n"
          + "  </MapField>\n"
          + "  <EnumArrayField name=\"ea\" storageId=\"21\" dimensions=\"2\">\n"
          + "    <Identifier>AAA</Identifier>\n"
          + "    <Identifier>BBB</Identifier>\n"
          + "  </EnumArrayField>\n"
          + "  <CompositeIndex storageId=\"110\" name=\"ir\">\n"
          + "    <Field name=\"i\"/>\n"
          + "    <Field name=\"e\"/>\n"
          + "    <Field name=\"r\"/>\n"
          + "  </CompositeIndex>\n"
          + "</ObjectType>\n"
          + "</Schema>\n";

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);
        final SchemaModel schema = SchemaModel.fromXML(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

        // Check calculations before lockdown
        schema.validate();
        final SchemaId hash1 = schema.getSchemaId();
        this.log.debug("hash = \"{}\" for schema\n{}", hash1, schema);

        // Lock down schema
        schema.lockDown();

        // Try to modify it
        try {
            schema.getSchemaObjectTypes().get("Foo").getSchemaFields().remove("e");
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            schema.getSchemaObjectTypes().remove("Foo");
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((SimpleSchemaField)schema.getSchemaObjectTypes().get("Foo").getSchemaFields().get("i")).setEncodingId(null);
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((SimpleSchemaField)schema.getSchemaObjectTypes().get("Foo").getSchemaFields().get("i")).setStorageId(123);
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((EnumSchemaField)schema.getSchemaObjectTypes().get("Foo").getSchemaFields().get("e")).getIdentifiers().clear();
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((EnumArraySchemaField)schema.getSchemaObjectTypes().get("Foo").getSchemaFields().get("ea")).getIdentifiers().clear();
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((EnumArraySchemaField)schema.getSchemaObjectTypes().get("Foo").getSchemaFields().get("ea")).setDimensions(123);
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((ReferenceSchemaField)schema.getSchemaObjectTypes().get("Foo").getSchemaFields().get("r")).setInverseDelete(null);
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((ReferenceSchemaField)schema.getSchemaObjectTypes().get("Foo").getSchemaFields().get("r")).setForwardDelete(true);
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            ((ReferenceSchemaField)schema.getSchemaObjectTypes().get("Foo").getSchemaFields().get("r")).setAllowDeleted(false);
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }
        try {
            schema.getSchemaObjectTypes().get("Foo").getSchemaCompositeIndexes().remove("ir");
            assert false;
        } catch (UnsupportedOperationException e) {
            this.log.debug("got expected {}", e.toString());
        }

        // Check calculations after lockdown
        schema.validate();
        final SchemaId hash2 = schema.getSchemaId();
        Assert.assertEquals(hash2, hash1);

        schema.validate();
        final SchemaId hash3 = schema.getSchemaId();
        Assert.assertEquals(hash3, hash1);

        // Clone it to make it modifiable again
        final SchemaModel schema2 = schema.clone();
        Assert.assertEquals(schema2, schema);

        schema2.validate();
        final SchemaId hash4 = schema2.getSchemaId();
        this.log.debug("hash = \"{}\" for schema\n{}", hash4, schema2);
        Assert.assertEquals(hash4, hash1);

        // We should be able to modify the clone
        ((SimpleSchemaField)schema2.getSchemaObjectTypes().get("Foo").getSchemaFields().get("i")).setEncodingId(null);
        ((SimpleSchemaField)schema2.getSchemaObjectTypes().get("Foo").getSchemaFields().get("i")).setIndexed(false);
        ((EnumSchemaField)schema2.getSchemaObjectTypes().get("Foo").getSchemaFields().get("e")).getIdentifiers().clear();
        ((EnumArraySchemaField)schema2.getSchemaObjectTypes().get("Foo").getSchemaFields().get("ea")).getIdentifiers().clear();
        ((EnumArraySchemaField)schema2.getSchemaObjectTypes().get("Foo").getSchemaFields().get("ea")).setDimensions(123);
        ((ReferenceSchemaField)schema2.getSchemaObjectTypes().get("Foo").getSchemaFields().get("r")).setInverseDelete(null);
        ((ReferenceSchemaField)schema2.getSchemaObjectTypes().get("Foo").getSchemaFields().get("r")).setForwardDelete(true);
        ((ReferenceSchemaField)schema2.getSchemaObjectTypes().get("Foo").getSchemaFields().get("r")).setAllowDeleted(false);
        schema2.getSchemaObjectTypes().get("Foo").getSchemaCompositeIndexes().remove("ir");
        schema2.getSchemaObjectTypes().get("Foo").getSchemaFields().remove("e");
        schema2.getSchemaObjectTypes().remove("Foo");

        final SchemaId hash5 = schema2.getSchemaId();
        this.log.debug("hash = \"{}\" for schema\n{}", hash5, schema2);
        Assert.assertNotEquals(hash5, hash1, "\n" + schema2 + "\n");
    }
}
