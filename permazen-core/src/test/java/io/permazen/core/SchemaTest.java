
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Converter;

import io.permazen.encoding.ConvertedEncoding;
import io.permazen.encoding.DefaultEncodingRegistry;
import io.permazen.encoding.EncodingId;
import io.permazen.encoding.EncodingRegistry;
import io.permazen.encoding.StringEncoding;
import io.permazen.kv.simple.MemoryKVDatabase;
import io.permazen.schema.SchemaModel;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SchemaTest extends CoreAPITestSupport {

    @Test(dataProvider = "cases")
    public void testSchema(boolean valid, String xml) throws Exception {
        xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Schema>\n" + xml + "</Schema>\n";
        final Database db = new Database(new MemoryKVDatabase());

        this.log.info("*** testSchema():\nXML:\n{}", xml);

        // Register custom type
        ((DefaultEncodingRegistry)db.getEncodingRegistry()).add(new BarEncoding());

        // Validate XML
        final SchemaModel schema;
        try {
            schema = SchemaModel.fromXML(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
        } catch (InvalidSchemaException e) {
            assert !valid : "XML was supposed to be valid: " + this.show(e);
            return;
        }

        // Validate schema
        try {
            final TransactionConfig config = TransactionConfig.builder()
              .schemaRemoval(TransactionConfig.SchemaRemoval.NEVER)
              .schemaModel(schema)
              .build();
            db.createTransaction(config).rollback();
            assert valid : "schema was supposed to be invalid";
        } catch (InvalidSchemaException e) {
            assert !valid : "schema was supposed to be valid: " + this.show(e);
        }
    }

    @Test(dataProvider = "upgradeCases")
    public void testUpgradeSchema(boolean valid, String a, String b) throws Exception {
        final String[] xmls = new String[] { a, b };
        for (int i = 0; i < xmls.length; i++) {
            String xml1 = xmls[i];
            String xml2 = xmls[1 - i];

            this.log.info("*** testUpgradeSchema():\nXML1:\n{}XML2:\n{}", xml1, xml2);

            final Database db = new Database(new MemoryKVDatabase());

            xml1 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Schema>\n" + xml1 + "</Schema>\n";
            final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream(xml1.getBytes(StandardCharsets.UTF_8)));
            db.createTransaction(schema1).commit();

            xml2 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Schema>\n" + xml2 + "</Schema>\n";
            final SchemaModel schema2 = SchemaModel.fromXML(new ByteArrayInputStream(xml2.getBytes(StandardCharsets.UTF_8)));
            try {
                final TransactionConfig config = TransactionConfig.builder()
                  .schemaRemoval(TransactionConfig.SchemaRemoval.NEVER)
                  .schemaModel(schema2)
                  .build();
                db.createTransaction(config).rollback();
                assert valid : "upgrade schema was supposed to be invalid";
            } catch (InvalidSchemaException e) {
                assert !valid : "upgrade schema was supposed to be valid: " + this.show(e);
            }
        }
    }

    @Test
    public void testValidateSchema() throws Exception {
        final String header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Schema>\n";
        final String footer = "</Schema>\n";

        final String xml1 = header
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField indexed=\"true\" name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"20\"/>\n"
          + "</ObjectType>\n"
          + footer;

        final String xml2 = header
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField indexed=\"true\" name=\"i\" encoding=\"urn:fdc:permazen.io:2020:float\" storageId=\"20\"/>\n"
          + "</ObjectType>\n"
          + footer;

        final String xml3 = header
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"b\" encoding=\"urn:foo:bar\"/>\n"
          + "</ObjectType>\n"
          + footer;

        this.log.info("*** testValidateSchema():\nXML1:\n{}\nXML2:\n{}\nXML3:\n{}", xml1, xml2, xml3);

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream(xml1.getBytes(StandardCharsets.UTF_8)));
        final SchemaModel schema2 = SchemaModel.fromXML(new ByteArrayInputStream(xml2.getBytes(StandardCharsets.UTF_8)));
        final SchemaModel schema3 = SchemaModel.fromXML(new ByteArrayInputStream(xml3.getBytes(StandardCharsets.UTF_8)));

        final DefaultEncodingRegistry encodingRegistry = new DefaultEncodingRegistry();

        this.validate(encodingRegistry, true, schema1);
        this.validate(encodingRegistry, true, schema1);
        this.validate(encodingRegistry, false, schema3);                       // BarEncoding not registered yet

        encodingRegistry.add(new BarEncoding());
        this.validate(encodingRegistry, true, schema1);
        this.validate(encodingRegistry, true, schema1);
        this.validate(encodingRegistry, true, schema3);                        // BarEncoding is now registered

        this.validate(encodingRegistry, false, schema1, schema2);              // incompatible use of field #20
        this.validate(encodingRegistry, true, schema1, schema3);
        this.validate(encodingRegistry, true, schema2, schema3);
        this.validate(encodingRegistry, false, schema1, schema2, schema3);
    }

    private void validate(EncodingRegistry encodingRegistry, boolean expectedValid, SchemaModel... schemas) {
        final Database db = new Database(new MemoryKVDatabase());
        db.setEncodingRegistry(encodingRegistry);
        try {
            for (SchemaModel schema : schemas) {
                final TransactionConfig config = TransactionConfig.builder()
                  .schemaRemoval(TransactionConfig.SchemaRemoval.NEVER)
                  .schemaModel(schema)
                  .build();
                db.createTransaction(config).commit();
            }
            if (!expectedValid)
                throw new AssertionError("expected invalid schema combination but was valid");
        } catch (InvalidSchemaException e) {
            if (expectedValid)
                throw new AssertionError("expected valid schema combination but got " + e, e);
        }
    }

    public static class Bar {

        private final String value;

        public Bar(String value) {
            this.value = value;
        }

        public String getValue() {
            return this.value;
        }
    }

    @SuppressWarnings("serial")
    public static class BarEncoding extends ConvertedEncoding<Bar, String> {
        public BarEncoding() {
            super(new EncodingId("urn:foo:bar"), Bar.class, () -> new Bar(""),
              new StringEncoding(), Converter.from(Bar::getValue, Bar::new), false);
        }
    }

    @DataProvider(name = "cases")
    public Object[][] cases() {
        return new Object[][] {

          // No object types
          { true,
            ""
          },

          // Garbage
          { false,
            "!@#$%^&"
          },

          // No storage ID
          { true,
            "<!-- test 1 -->\n"
          + "<ObjectType name=\"Foo\"/>\n"
          },

          // Zero storage ID
          { true,
            "<!-- test 2 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"0\"/>\n"
          },

          // Bogus storage ID
          { false,
            "<!-- test 3 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"-123\"/>\n"
          },

          // Explicit storage ID
          { true,
            "<!-- test 4 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"123\"/>\n"
          },

          // Missing object type name
          { false,
            "<!-- test 5 -->\n"
          + "<ObjectType storageId=\"123\"/>\n"
          },

          // Ensure automatically assigned storage IDs don't interfere with explicitly assigned ones
          { true,
            "<!-- test 5b -->\n"
          + "<ObjectType name=\"Foo1\"/>\n"
          + "<ObjectType name=\"Foo2\"/>\n"
          + "<ObjectType name=\"Foo3\"/>\n"
          + "<ObjectType name=\"Foo4\" storageId=\"1\"/>\n"
          },

          // Don't allow duplicate object storage IDs
          { false,
            "<!-- test 6 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"123\"/>\n"
          + "<ObjectType name=\"Foo\" storageId=\"123\"/>\n"
          },

          // Disallow duplicate object names
          { false,
            "<!-- test 7 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"123\"/>\n"
          + "<ObjectType name=\"Foo\" storageId=\"456\"/>\n"
          },

          // Missing encoding
          { false,
            "<!-- test 8 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField name=\"i\"/>\n"
          + "</ObjectType>\n"
          },

          // Missing field name
          { false,
            "<!-- test 9 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\"/>\n"
          + "</ObjectType>\n"
          },

          // Missing field name and encoding
          { false,
            "<!-- test 10 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField storageId=\"456\"/>\n"
          + "</ObjectType>\n"
          },

          { true,
            "<!-- test 11 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\"/>\n"
          + "</ObjectType>\n"
          },

          { true,
            "<!-- test 12 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"0\"/>\n"
          + "</ObjectType>\n"
          },

          // Bogus field storage ID
          { false,
            "<!-- test 13 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"-456\"/>\n"
          + "</ObjectType>\n"
          },

          // Conflicting storage ID's
          { false,
            "<!-- test 14 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"10\"/>\n"
          + "</ObjectType>\n"
          },

          { true,
            "<!-- test 15 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"20\"/>\n"
          + "</ObjectType>\n"
          },

          // Don't allow duplicate field storage IDs
          { false,
            "<!-- test 16 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"aaa\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"2\"/>\n"
          + "  <SimpleField name=\"bbb\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"2\"/>\n"
          + "</ObjectType>\n"
          },

          // Disallow duplicate field names in the same object
          { false,
            "<!-- test 17 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"2\"/>\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"3\"/>\n"
          + "</ObjectType>\n"
          },

          { true,
            "<!-- test 18 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\"/>\n"
          + "</ObjectType>\n"
          + "<ObjectType name=\"Bar\" storageId=\"20\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:float\"/>\n"
          + "</ObjectType>\n"
          },

          // Allow different types for the same field name if different storage ID's
          { true,
            "<!-- test 18a -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" indexed=\"true\" storageId=\"2\"/>\n"
          + "</ObjectType>\n"
          + "<ObjectType name=\"Bar\" storageId=\"20\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:float\" indexed=\"true\" storageId=\"3\"/>\n"
          + "</ObjectType>\n"
          },

          // Allow same storage ID for the field if same encoding
          { true,
            "<!-- test 18b -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"2\"/>\n"
          + "</ObjectType>\n"
          + "<ObjectType name=\"Bar\" storageId=\"20\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"2\"/>\n"
          + "</ObjectType>\n"
          },

          // Disallow different types for the same field and storage ID
          { false,
            "<!-- test 18d -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"2\"/>\n"
          + "</ObjectType>\n"
          + "<ObjectType name=\"Bar\" storageId=\"20\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:float\" storageId=\"2\"/>\n"
          + "</ObjectType>\n"
          },

          // Allow duplicate fields in different objects
          { true,
            "<!-- test 19 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <ReferenceField name=\"i\" storageId=\"2\"/>\n"  // default inverseDelete is EXCEPTION
          + "</ObjectType>\n"
          + "<ObjectType name=\"Bar\" storageId=\"20\">\n"
          + "  <ReferenceField name=\"i\" storageId=\"2\" inverseDelete=\"IGNORE\"/>\n"
          + "</ObjectType>\n"
          },

          { true,
            "<!-- test 20 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"2\"/>\n"
          + "</ObjectType>\n"
          + "<ObjectType name=\"Bar\" storageId=\"20\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"2\"/>\n"
          + "</ObjectType>\n"
          },

          // Wrong set sub-field name
          { false,
            "<!-- test 21 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" name=\"dummy\" storageId=\"21\"/>\n"
          + "  </SetField>\n"
          + "</ObjectType>\n"
          },

          { true,
            "<!-- test 22 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"21\"/>\n"
          + "  </SetField>\n"
          + "</ObjectType>\n"
          + "<ObjectType name=\"Bar\" storageId=\"11\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"21\"/>\n"
          + "  </SetField>\n"
          + "</ObjectType>\n"
          },

          { true,
            "<!-- test 22.5 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"21\" indexed=\"true\"/>\n"       // indexed
          + "  </SetField>\n"
          + "</ObjectType>\n"
          + "<ObjectType name=\"Bar\" storageId=\"11\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"21\" indexed=\"false\"/>\n"      // not indexed
          + "  </SetField>\n"
          + "</ObjectType>\n"
          },

            // Inconsistent sub-field storage ID
          { false,
            "<!-- test 23 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"21\"/>\n"
          + "  </SetField>\n"
          + "</ObjectType>\n"
          + "<ObjectType name=\"Bar\" storageId=\"20\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"22\"/>\n"
          + "  </SetField>\n"
          + "</ObjectType>\n"
          },

            // Inconsistent super-field storage ID
          { false,
            "<!-- test 23.5 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"22\"/>\n"
          + "  </SetField>\n"
          + "</ObjectType>\n"
          + "<ObjectType name=\"Bar\" storageId=\"20\">\n"
          + "  <SetField name=\"set\" storageId=\"21\">\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"22\"/>\n"
          + "  </SetField>\n"
          + "</ObjectType>\n"
          },

          { true,
            "<!-- test 24 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <CounterField name=\"counter\" storageId=\"20\"/>\n"
          + "</ObjectType>\n"
          },

          { true,
            "<!-- test 25 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <CounterField name=\"counter\"/>\n"
          + "</ObjectType>\n"
          },

          // Counter fields cannot be sub-fields
          { false,
            "<!-- test 26 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <CounterField name=\"counter\" storageId=\"20\"/>\n"
          + "  </SetField>\n"
          + "</ObjectType>\n"
          },

          // Missing counter field name
          { false,
            "<!-- test 27 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <CounterField storageId=\"20\"/>\n"
          + "</ObjectType>\n"
          },

          // Allow duplicate field storage IDs in different objects
          { true,
            "<!-- test 28 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"2\"/>\n"
          + "</ObjectType>\n"
          + "<ObjectType name=\"Bar\" storageId=\"20\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"2\"/>\n"
          + "</ObjectType>\n"
          },

          // Invalid names
          { false,
            "<!-- test 29 -->\n"
          + "<ObjectType name=\" Foo\" storageId=\"10\"/>\n"
          },

          // Invalid names
          { false,
            "<!-- test 30 -->\n"
          + "<ObjectType name=\"\" storageId=\"10\"/>\n"
          },

          // Invalid names
          { false,
            "<!-- test 31 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"2foo\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"2\"/>\n"
          + "</ObjectType>\n"
          },

          // Sub-fields can have names but they must be the right ones
          { true,
            "<!-- test 32 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField name=\"element\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"22\"/>\n"
          + "  </SetField>\n"
          + "</ObjectType>\n"
          },

          { true,
            "<!-- test 33 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <MapField name=\"map\" storageId=\"20\">\n"
          + "    <SimpleField name=\"key\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"22\"/>\n"
          + "    <SimpleField name=\"value\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"23\"/>\n"
          + "  </MapField>\n"
          + "</ObjectType>\n"
          },

          // Wrong set sub-field name
          { false,
            "<!-- test 34 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField name=\"item\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"22\"/>\n"
          + "  </SetField>\n"
          + "</ObjectType>\n"
          },

          // Wrong map sub-field names
          { false,
            "<!-- test 35 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <MapField name=\"map\" storageId=\"20\">\n"
          + "    <SimpleField name=\"KEY\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"22\"/>\n"
          + "    <SimpleField name=\"VALUE\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"23\"/>\n"
          + "  </MapField>\n"
          + "</ObjectType>\n"
          },

          // Missing name
          { false,
            "<!-- test 36 -->\n"
          + "<ObjectType storageId=\"10\"/>\n"
          },

          // Correct encoding URN for BarEncoding
          { true,
            "<!-- test 37 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"bar\" encoding=\"urn:foo:bar\" storageId=\"20\"/>\n"
          + "</ObjectType>\n"
          },

          // Unknown encoding URN
          { false,
            "<!-- test 38 -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"bar\" encoding=\"urn:something:else\" storageId=\"20\"/>\n"
          + "</ObjectType>\n"
          },

        };
    }

    @DataProvider(name = "upgradeCases")
    public Object[][] upgradeCases() {
        return new Object[][] {

          // Change a field's type, not both indexed
          { true,
            "<!-- test 1a -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <CounterField name=\"counter\"/>\n"
          + "</ObjectType>\n",

            "<!-- test 1b -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <ReferenceField name=\"ref1\"/>\n"
          + "</ObjectType>\n",
          },

          { true,
            "<!-- test 1.1a -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" name=\"dummy\"/>\n"
          + "</ObjectType>\n",

            "<!-- test 1.1b -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <ReferenceField name=\"ref1\" storageId=\"20\"/>\n"
          + "</ObjectType>\n",
          },

          { true,
            "<!-- test 1.2a -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\"/>\n"
          + "  </SetField>\n"
          + "</ObjectType>\n",

            "<!-- test 1.2b -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" encoding=\"urn:fdc:permazen.io:2020:int\" indexed=\"true\"/>\n"
          + "</ObjectType>\n",
          },

          { true,
            "<!-- test 1.3a -->\n"
          + "<ObjectType name=\"Foo\">\n"
          + "  <SetField name=\"set\">\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" indexed=\"true\"/>\n"
          + "  </SetField>\n"
          + "  <ReferenceField name=\"ref1\"/>\n"
          + "  <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" name=\"i\" storageId=\"31\"/>\n"
          + "</ObjectType>\n",

            "<!-- test 1.3b -->\n"
          + "<ObjectType name=\"Foo\">\n"
          + "  <ReferenceField name=\"ref1\"/>\n"
          + "  <SetField name=\"set\">\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" indexed=\"true\"/>\n"
          + "  </SetField>\n"
          + "</ObjectType>\n",
          },

          // Change reference field inverseDelete
          { true,
            "<!-- test 2a -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <ReferenceField name=\"ref1\" storageId=\"20\" inverseDelete=\"EXCEPTION\"/>\n"
          + "</ObjectType>\n",

            "<!-- test 2b -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <ReferenceField name=\"ref1\" storageId=\"20\" inverseDelete=\"NULLIFY\"/>\n"
          + "</ObjectType>\n",
          },

          // Move a field
          { true,
            "<!-- test 3a -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <ReferenceField name=\"ref1\" storageId=\"11\"/>\n"
          + "</ObjectType>\n"
          + "<ObjectType name=\"Bar\" storageId=\"20\">\n"
          + "</ObjectType>\n",

            "<!-- test 3b -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "</ObjectType>\n"
          + "<ObjectType name=\"Bar\" storageId=\"20\">\n"
          + "  <ReferenceField name=\"ref1\" storageId=\"11\"/>\n"
          + "</ObjectType>\n",
          },

          // Change sub-field encodings - compatible storage ID's
          { true,
            "<!-- test 4a -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <MapField name=\"set\">\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\"/>\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:String\"/>\n"
          + "  </MapField>\n"
          + "</ObjectType>\n",

            "<!-- test 4b -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <MapField name=\"set\">\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:String\"/>\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\"/>\n"
          + "  </MapField>\n"
          + "</ObjectType>\n",
          },

          // Change sub-field encodings - incompatible storage ID's
          { false,
            "<!-- test 5a -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <MapField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"21\"/>\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:String\" storageId=\"22\"/>\n"
          + "  </MapField>\n"
          + "</ObjectType>\n",

            "<!-- test 5b -->\n"
          + "<ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "  <MapField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:String\" storageId=\"21\"/>\n"
          + "    <SimpleField encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"22\"/>\n"
          + "  </MapField>\n"
          + "</ObjectType>\n",
          },

        };
    }
}
