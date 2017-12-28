
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenType;
import io.permazen.test.TestSupport;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ToStringTest extends TestSupport {

    @Test(dataProvider = "cases")
    private <T extends Person> void testToString(Class<T> cl, String pattern) throws Exception {
        final Permazen jdb = BasicTest.getPermazen(cl);
        final JTransaction jtx = jdb.createTransaction();
        JTransaction.setCurrent(jtx);
        try {

            final T person = jtx.create(cl);

            person.setName("fred");
            person.setAge(23);

            final String expected = pattern
              .replaceAll("@OBJID@", person.getObjId().toString())
              .replaceAll("@TYPENAME@", cl.getSimpleName())
              .replaceAll("@TYPEID@", "" + person.getJClass().getStorageId())
              .replaceAll("@VERSION@", "" + person.getSchemaVersion());

            Assert.assertEquals(person.toString(), expected);

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @DataProvider(name = "cases")
    public Object[][] testToStringCases() throws Exception {
        return new Object[][] {
            { Person1.class,  "object @OBJID@ type @TYPENAME@#@TYPEID@ version @VERSION@\n age = 23\nname = \"fred\"" },
            { Person2.class,  "object @OBJID@ type @TYPENAME@#@TYPEID@ version @VERSION@\n age = 23\nname = \"fred\"" },
            { Person3.class,  "fred:23" },
        };
    }

// Model Classes

    public interface Person extends JObject {

        String getName();
        void setName(String x);

        int getAge();
        void setAge(int x);
    }

    @PermazenType
    public interface Person1 extends Person {
    }

    @PermazenType
    public abstract static class Person2 implements Person {
    }

    @PermazenType
    public abstract static class Person3 implements Person {

        @Override
        public String toString() {
            return this.getName() + ":" + this.getAge();
        }
    }
}
