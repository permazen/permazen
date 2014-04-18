
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.io.ByteArrayInputStream;

import org.jsimpledb.TestSupport;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SchemaTest extends TestSupport {

    @Test(dataProvider = "cases")
    private void testSchema(boolean valid, String xml) throws Exception {
        xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Schema>\n" + xml + "</Schema>\n";
        final SimpleKVDatabase kvstore = new SimpleKVDatabase(100, 200);
        final Database db = new Database(kvstore);

        // Validate XML
        final SchemaModel schema;
        try {
            schema = SchemaModel.fromXML(new ByteArrayInputStream(xml.getBytes("UTF-8")));
        } catch (InvalidSchemaException e) {
            assert !valid : "XML was supposed to be valid: " + this.show(e);
            return;
        }

        // Validate schema
        try {
            db.validateSchema(schema);
            assert valid : "schema was supposed to be invalid";
        } catch (InvalidSchemaException e) {
            assert !valid : "schema was supposed to be valid: " + this.show(e);
        }
    }

    @DataProvider(name = "cases")
    public Object[][] cases() {
        return new Object[][] {
          { true,
            ""
          },

          { false,
            "!@#$%^&"
          },

          { false,
            "<!-- test 1 -->\n"
          + "<Object name=\"Foo\"/>\n"
          },

          { false,
            "<!-- test 2 -->\n"
          + "<Object name=\"Foo\" storageId=\"0\"/>\n"
          },

          { false,
            "<!-- test 3 -->\n"
          + "<Object name=\"Foo\" storageId=\"-123\"/>\n"
          },

          { true,
            "<!-- test 4 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\"/>\n"
          },

          { false,
            "<!-- test 5 -->\n"
          + "<Object storageId=\"123\"/>\n"
          },

          // Don't allow duplicate object storage IDs
          { false,
            "<!-- test 6 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\"/>\n"
          + "<Object name=\"Foo\" storageId=\"123\"/>\n"
          },

          // Allow duplicate object names
          { true,
            "<!-- test 7 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\"/>\n"
          + "<Object name=\"Foo\" storageId=\"456\"/>\n"
          },

          { false,
            "<!-- test 8 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField name=\"i\"/>\n"
          + "</Object>\n"
          },

          { false,
            "<!-- test 9 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField type=\"int\"/>\n"
          + "</Object>\n"
          },

          { false,
            "<!-- test 10 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField storageId=\"456\"/>\n"
          + "</Object>\n"
          },

          { false,
            "<!-- test 11 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField name=\"i\" type=\"int\"/>\n"
          + "</Object>\n"
          },

          { false,
            "<!-- test 12 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"0\"/>\n"
          + "</Object>\n"
          },

          { false,
            "<!-- test 13 -->\n"
          + "<Object name=\"Foo\" storageId=\"123\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"-456\"/>\n"
          + "</Object>\n"
          },

          { false,
            "<!-- test 14 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"10\"/>\n"
          + "</Object>\n"
          },

          { true,
            "<!-- test 15 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"20\"/>\n"
          + "</Object>\n"
          },

          // Don't allow duplicate field storage IDs
          { false,
            "<!-- test 16 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"aaa\" type=\"int\" storageId=\"2\"/>\n"
          + "  <SimpleField name=\"bbb\" type=\"int\" storageId=\"2\"/>\n"
          + "</Object>\n"
          },

          // Allow duplicate field names
          { true,
            "<!-- test 17 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"2\"/>\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"3\"/>\n"
          + "</Object>\n"
          },

          { false,
            "<!-- test 18 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"2\"/>\n"
          + "</Object>\n"
          + "<Object name=\"Bar\" storageId=\"20\">\n"
          + "  <SimpleField name=\"i\" type=\"float\" storageId=\"2\"/>\n"
          + "</Object>\n"
          },

          { false,
            "<!-- test 19 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <ReferenceField name=\"i\" storageId=\"2\"/>\n"  // default onDelete is EXCEPTION
          + "</Object>\n"
          + "<Object name=\"Bar\" storageId=\"20\">\n"
          + "  <ReferenceField name=\"i\" storageId=\"2\" onDelete=\"NOTHING\"/>\n"
          + "</Object>\n"
          },

          { true,
            "<!-- test 20 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"2\"/>\n"
          + "</Object>\n"
          + "<Object name=\"Bar\" storageId=\"20\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"2\"/>\n"
          + "</Object>\n"
          },

          { false,
            "<!-- test 21 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <SimpleField type=\"int\" name=\"dummy\" storageId=\"21\"/>\n"
          + "  </SetField>\n"
          + "</Object>\n"
          },

          { true,
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

          { false,
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

          { true,
            "<!-- test 24 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <CounterField name=\"counter\" storageId=\"20\"/>\n"
          + "</Object>\n"
          },

          { false,
            "<!-- test 25 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <CounterField name=\"counter\"/>\n"
          + "</Object>\n"
          },

          { false,
            "<!-- test 26 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SetField name=\"set\" storageId=\"20\">\n"
          + "    <CounterField name=\"counter\" storageId=\"20\"/>\n"
          + "  </SetField>\n"
          + "</Object>\n"
          },

          { false,
            "<!-- test 27 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <CounterField storageId=\"20\"/>\n"
          + "</Object>\n"
          },

          // Allow duplicate field storage IDs in different objects
          { true,
            "<!-- test 28 -->\n"
          + "<Object name=\"Foo\" storageId=\"10\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"2\"/>\n"
          + "</Object>\n"
          + "<Object name=\"Bar\" storageId=\"20\">\n"
          + "  <SimpleField name=\"i\" type=\"int\" storageId=\"2\"/>\n"
          + "</Object>\n"
          },

        };
    }
}

