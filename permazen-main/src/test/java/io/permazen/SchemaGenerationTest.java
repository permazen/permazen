
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenTransient;
import io.permazen.annotation.PermazenType;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SchemaGenerationTest extends MainTestSupport {

    @Test(dataProvider = "invalidCases")
    public void testBogusAbstract(Class<?>[] classes) throws Exception {
        final PermazenConfig config = PermazenConfig.builder()
          .modelClasses(Foo.class, BogusAbstract.class)
          .build();
        try {
            config.newPermazen();
            assert false : "should have failed";
        } catch (IllegalArgumentException e) {
            this.log.info("got expected {}", e.toString());
        }
    }

    @Test(dataProvider = "validCases")
    public void testSchemaGeneration(String expected, Class<?>[] types) throws Exception {
        final PermazenConfig config = PermazenConfig.builder()
          .modelClasses(types)
          .build();
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        config.newPermazen().getSchemaModel(false).toXML(buf, true, true);
        final String actual = new String(buf.toByteArray(), StandardCharsets.UTF_8)
          .replaceAll("<!--((?!--).)*-->", ""); // strip schema ID comments
        Assert.assertEquals(actual.replaceAll("\\r\\n?", "\n"), expected);
    }

    @DataProvider(name = "invalidCases")
    public Class<?>[][][] genInvalid() {
        return new Class<?>[][][] {
            { { Foo.class, BogusAbstract.class } },         // unimplemented abstract method (from interface)
            { { Foo.class, Foo2.class } },                  // unimplemented abstract method (from @PermazenTransient)
        };
    }

    @DataProvider(name = "validCases")
    public Object[][] genValid() {
        return new Object[][] {

        // protected setter
        {   "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "    <ObjectType storageId=\"1\" name=\"Foo\">\n"
          + "        <SimpleField name=\"value\" encoding=\"urn:fdc:permazen.io:2020:long\"/>\n"
          + "    </ObjectType>\n"
          + "</Schema>\n",
          new Class<?>[] { Foo.class } },

        // non-abstract method with autogenNonAbstract() = true
        {   "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "    <ObjectType storageId=\"1\" name=\"Foo\">\n"
          + "        <SimpleField name=\"value\" encoding=\"urn:fdc:permazen.io:2020:long\"/>\n"
          + "    </ObjectType>\n"
          + "    <ObjectType storageId=\"3\" name=\"Foo3\">\n"
          + "        <SimpleField name=\"concreteField\" encoding=\"urn:fdc:permazen.io:2020:int\"/>\n"
          + "        <SimpleField name=\"value\" encoding=\"urn:fdc:permazen.io:2020:long\"/>\n"
          + "    </ObjectType>\n"
          + "</Schema>\n",
          new Class<?>[] { Foo.class, Foo3.class } },

        };
    }

// Model Classes

    @PermazenType(storageId = 1)
    public abstract static class Foo implements PermazenObject {

        protected abstract long getValue();
        protected abstract void setValue(long value);
    }

    @PermazenType
    public abstract static class BogusAbstract extends Foo implements Comparable<Foo> {
    }

    @PermazenType(storageId = 2)
    public abstract static class Foo2 extends Foo {

        @PermazenTransient
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
