
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.Database;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SchemaGenerationTest extends TestSupport {

    @Test
    public void testBogusAbstract() throws Exception {
        final JSimpleDBFactory factory = new JSimpleDBFactory();
        factory.setDatabase(new Database(new SimpleKVDatabase()));
        factory.setSchemaVersion(1);
        factory.setModelClasses(Foo.class, BogusAbstract.class);
        try {
            factory.newJSimpleDB();
            assert false;
        } catch (IllegalArgumentException e) {
            this.log.info("got expected " + e);
        }
    }

    @Test(dataProvider = "cases")
    public void testSchemaGeneration(String expected, Class<?>[] types) throws Exception {
        final JSimpleDBFactory factory = new JSimpleDBFactory();
        factory.setDatabase(new Database(new SimpleKVDatabase()));
        factory.setSchemaVersion(1);
        factory.setModelClasses(types);
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        factory.newJSimpleDB().getSchemaModel().toXML(buf, true);
        final String actual = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        Assert.assertEquals(actual, expected);
    }

    @DataProvider(name = "cases")
    public Object[][] genPaths() {
        return new Object[][] {

        // protected setter
        {   "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"2\">\n"
          + "    <ObjectType storageId=\"9029\" name=\"Foo\">\n"
          + "        <SimpleField storageId=\"51616\" name=\"value\" type=\"long\"/>\n"
          + "    </ObjectType>\n"
          + "</Schema>\n",
          new Class<?>[] { Foo.class } },

        };
    }

// Model Classes

    @JSimpleClass
    public abstract static class Foo implements JObject {

        protected abstract long getValue();
        protected abstract void setValue(long value);
    }

    @JSimpleClass
    public abstract static class BogusAbstract extends Foo implements Comparable<Foo> {
    }
}

