
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.io.ByteArrayInputStream;

import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SchemaTest extends TestSupport {

    @Test(dataProvider = "cases")
    private void testSchema(boolean validXML, boolean validSchema, String xml) throws Exception {
        xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Schema>\n" + xml + "</Schema>\n";
        final SimpleKVDatabase kvstore = new SimpleKVDatabase(100, 200);
        final Database db = new Database(kvstore);

        // Validate XML
        final SchemaModel schema;
        try {
            schema = SchemaModel.fromXML(new ByteArrayInputStream(xml.getBytes("UTF-8")));
            assert validXML : "XML was supposed to be invalid";
        } catch (IllegalArgumentException e) {
            assert !validXML : "XML was supposed to be valid: " + this.show(e);
            return;
        }

        // Validate schema
        try {
            db.createTransaction(schema, 1, true);
            assert validSchema : "schema was supposed to be invalid";
        } catch (InvalidSchemaException e) {
            assert !validSchema : "schema was supposed to be valid: " + this.show(e);
        }
    }

    @DataProvider(name = "cases")
    public Object[][] cases() {
        return new Object[][] {
          { true, true,
            ""
          },

          { false, false,
            "!@#$%^&"
          },

          { false, false,
            "<!-- test 1 -->\n"
          + "<Object name=\"Foo\"/>\n"
          },

          { true, false,
            "<!-- test 2 -->\n"
          + "<Object name=\"Foo\" storageId=\"0\"/>\n"
          },

          { true, false,
            "<!-- test 3 -->\n"
          + "<Object name=\"Foo\" storageId=\"-123\"/>\n"
          },

          { true, true,
            "<!-- test 4 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\"/>\n"
          },

          { true, false,
            "<!-- test 5 -->\n"
          + "<Object storageId=\"123\"/>\n"
          },

          // Allow duplicate object types
          { true, true,
            "<!-- test 6 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\"/>\n"
          + "<Object name=\"Foo\" storageId=\"123\"/>\n"
          },

          // Allow duplicate object names
          { true, true,
            "<!-- test 7 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\"/>\n"
          + "<Object name=\"Foo\" storageId=\"456\"/>\n"
          },

          { false, false,
            "<!-- test 8 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField name=\"i\"/>\n"
          + "</Object>\n"
          },

          { false, false,
            "<!-- test 9 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField type=\"int\"/>\n"
          + "</Object>\n"
          },

          { false, false,
            "<!-- test 10 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField storageId=\"456\"/>\n"
          + "</Object>\n"
          },

          { false, false,
            "<!-- test 11 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField name=\"i\" type=\"int\"/>\n"
          + "</Object>\n"
          },

          { true, false,
            "<!-- test 12 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"0\"/>\n"
          + "</Object>\n"
          },

          { true, false,
            "<!-- test 13 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"-456\"/>\n"
          + "</Object>\n"
          },

          { true, false,
            "<!-- test 14 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"10\"/>\n"
          + "</Object>\n"
          },

          { true, true,
            "<!-- test 15 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"20\"/>\n"
          + "</Object>\n"
          },

          // Allow duplicate fields
          { true, true,
            "<!-- test 16 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"2\"/>\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"2\"/>\n"
          + "</Object>\n"
          },

          // Allow duplicate names
          { true, true,
            "<!-- test 17 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"2\"/>\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"3\"/>\n"
          + "</Object>\n"
          },

          { true, false,
            "<!-- test 18 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"2\"/>\n"
          + "</Object>\n"
          + "<Object name=\"Bar\" storageId=\"20\">\n"
          + "  <SimpleField name=\"i\" type=\"float\" storageId=\"2\"/>\n"
          + "</Object>\n"
          },

          { true, false,
            "<!-- test 19 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <ReferenceField name=\"i\" storageId=\"2\"/>\n"  // default onDelete is EXCEPTION
          + "</Object>\n"
          + "<Object name=\"Bar\" storageId=\"20\">\n"
          + "  <ReferenceField name=\"i\" storageId=\"2\" onDelete=\"NOTHING\"/>\n"
          + "</Object>\n"
          },

          { true, true,
            "<!-- test 20 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"2\"/>\n"
          + "</Object>\n"
          + "<Object name=\"Bar\" storageId=\"20\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"2\"/>\n"
          + "</Object>\n"
          },

          { true, false,
            "<!-- test 21 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField type=\"int\" name=\"dummy\" storageId=\"21\"/>\n"
          + "  </SetField>\n"
          + "</Object>\n"
          },

          { true, true,
            "<!-- test 22 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField type=\"int\" storageId=\"21\"/>\n"
          + "  </SetField>\n"
          + "</Object>\n"
          + "<Object name=\"Bar\" storageId=\"11\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField type=\"int\" storageId=\"21\"/>\n"
          + "  </SetField>\n"
          + "</Object>\n"
          },

          { true, false,
            "<!-- test 23 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField type=\"int\" storageId=\"21\"/>\n"
          + "  </SetField>\n"
          + "</Object>\n"
          + "<Object name=\"Bar\" storageId=\"20\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField type=\"int\" storageId=\"22\"/>\n"
          + "  </SetField>\n"
          + "</Object>\n"
          },

        };
    }
}

