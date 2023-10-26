
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JTransient;
import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.test.TestSupport;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SchemaGenerationTest extends TestSupport {

    @Test(dataProvider = "invalidCases")
    public void testBogusAbstract(Class<?>[] classes) throws Exception {
        final PermazenFactory factory = new PermazenFactory();
        factory.setDatabase(new Database(new SimpleKVDatabase()));
        factory.setSchemaVersion(1);
        factory.setModelClasses(Foo.class, BogusAbstract.class);
        try {
            factory.newPermazen();
            assert false;
        } catch (IllegalArgumentException e) {
            this.log.info("got expected {}", e.toString());
        }
    }

    @Test(dataProvider = "validCases")
    public void testSchemaGeneration(String expected, Class<?>[] types) throws Exception {
        final PermazenFactory factory = new PermazenFactory();
        factory.setDatabase(new Database(new SimpleKVDatabase()));
        factory.setSchemaVersion(1);
        factory.setModelClasses(types);
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        factory.newPermazen().getSchemaModel().toXML(buf, true);
        final String actual = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        Assert.assertEquals(actual.replaceAll("\\r\\n?", "\n"), expected);
    }

    @DataProvider(name = "invalidCases")
    public Class<?>[][][] genInvalid() {
        return new Class<?>[][][] {
            { { Foo.class, BogusAbstract.class } },         // unimplemented abstract method (from interface)
            { { Foo.class, Foo2.class } },                  // unimplemented abstract method (from @JTransient)
        };
    }

    @DataProvider(name = "validCases")
    public Object[][] genValid() {
        return new Object[][] {

        // protected setter
        {   "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "    <ObjectType storageId=\"1\" name=\"Foo\">\n"
          + "        <SimpleField storageId=\"51616\" name=\"value\" encoding=\"urn:fdc:permazen.io:2020:long\"/>\n"
          + "    </ObjectType>\n"
          + "</Schema>\n",
          new Class<?>[] { Foo.class } },

        // non-abstract method with autogenNonAbstract() = true
        {   "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "    <ObjectType storageId=\"1\" name=\"Foo\">\n"
          + "        <SimpleField storageId=\"51616\" name=\"value\" encoding=\"urn:fdc:permazen.io:2020:long\"/>\n"
          + "    </ObjectType>\n"
          + "    <ObjectType storageId=\"3\" name=\"Foo3\">\n"
          + "        <SimpleField storageId=\"7189\" name=\"concreteField\" encoding=\"urn:fdc:permazen.io:2020:int\"/>\n"
          + "        <SimpleField storageId=\"51616\" name=\"value\" encoding=\"urn:fdc:permazen.io:2020:long\"/>\n"
          + "    </ObjectType>\n"
          + "</Schema>\n",
          new Class<?>[] { Foo.class, Foo3.class } },

        };
    }

// Model Classes

    @PermazenType(storageId = 1)
    public abstract static class Foo implements JObject {

        protected abstract long getValue();
        protected abstract void setValue(long value);
    }

    @PermazenType
    public abstract static class BogusAbstract extends Foo implements Comparable<Foo> {
    }

    @PermazenType(storageId = 2)
    public abstract static class Foo2 extends Foo {

        @JTransient
        protected abstract int getNotAField();
        protected abstract void setNotAField(int x);
    }

    @PermazenType(storageId = 3, autogenNonAbstract = true)
    public abstract static class Foo3 extends Foo {

        private int x;

        public int getConcreteField() {
            return this.x;
        }
        public void setConcreteField(int x) {
            this.x = x;
        }
    }
}
