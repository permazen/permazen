
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.util.List;
import java.util.NavigableMap;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JListField;
import org.jsimpledb.annotation.JMapField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReferencePathTest extends TestSupport {

    @Test(dataProvider = "paths")
    public void testReferencePath(Class<?> startType, Class<?> targetType, int targetField,
      int targetSuperField, int[] refs, Boolean lastIsSubField, String pathString) throws Exception {

        // Prepare args
        final boolean valid = targetType != null;
        if (refs == null)
            refs = new int[0];

        // Parse path
        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class, MeanPerson.class);
        final ReferencePath path;
        try {
            path = new ReferencePath(jdb, startType, pathString, lastIsSubField);
            assert valid : "path was supposed to be invalid";
        } catch (IllegalArgumentException e) {
            assert !valid : "path was supposed to be valid but got: " + e;
            return;
        }

        // Verify parse
        Assert.assertEquals(path.startType, startType);
        Assert.assertTrue(path.targetTypes.contains(targetType));
        Assert.assertEquals(path.targetFieldStorageId, targetField);
        Assert.assertEquals(path.targetSuperFieldStorageId, targetSuperField);
        Assert.assertEquals(path.getReferenceFields(), refs);
        Assert.assertEquals(path.path, pathString);
        Assert.assertEquals(path.toString(), pathString);
    }

    @DataProvider(name = "paths")
    public Object[][] genPaths() {
        return new Object[][] {

        //  startType       targetType          targetField     refs                    lastIsSubField
        //                                              targetSuperField                        path

          { Person.class,     null,             -1,     -1,     null,                   null,   "" },
          { Person.class,     null,             -1,     -1,     null,                   null,   "." },
          { Person.class,     null,             -1,     -1,     null,                   null,   "!@#$%^&" },
          { Person.class,     null,             -1,     -1,     null,                   null,   ".Person" },
          { Person.class,     null,             -1,     -1,     null,                   null,   "Person." },
          { Person.class,     null,             -1,     -1,     null,                   null,   "Person..z" },
          { Person.class,     null,             -1,     -1,     null,                   null,   "Person.unknown" },

          { Person.class,     Person.class,     101,    0,      null,                   null,   "z" },
          { Person.class,     null,             -1,     -1,     null,                   null,   "Person.z" },

          { MeanPerson.class, MeanPerson.class, 150,    0,      null,                   null,   "enemies" },
          { MeanPerson.class, MeanPerson.class, 150,    0,      null,                   false,  "enemies" },
          { MeanPerson.class, null,             -1,     -1,     null,                   true,   "enemies" },

          { MeanPerson.class, MeanPerson.class, 151,    150,    null,                   null,   "enemies.element" },
          { MeanPerson.class, MeanPerson.class, 151,    150,    null,                   true,   "enemies.element" },
          { MeanPerson.class, null,             -1,     -1,     null,                   false,  "enemies.element" },

          { MeanPerson.class, Person.class,     101,    0,      new int[] { 151 },      null,   "enemies.element.z" },
          { MeanPerson.class, Person.class,     141,    140,    new int[] { 151 },      null,   "enemies.element.ratings.key" },
          { MeanPerson.class, Person.class,     101,    0,      new int[] { 151, 141 }, null,   "enemies.element.ratings.key.z" },

      // Sub-field
          { Person.class,     MeanPerson.class, 150,    0,      null,                   false,  "enemies" },

        };
    }

    @Test
    public void testWackyPaths() throws Exception {
        final JSimpleDB jdb = BasicTest.getJSimpleDB(WackyPaths1.class, WackyPaths2.class, WackyPaths3.class);

        // this should be OK - two hops through two reference fields: foo->element->name
        new ReferencePath(jdb, WackyPaths1.class, "foo.element.name", null);

        // this should be OK - one hop through one set element sub-field: foo.element->name
        new ReferencePath(jdb, WackyPaths3.class, "foo.element.name", null);

        // these should be OK - we are disambiguating which "foo" field by specifying the storage ID
        new ReferencePath(jdb, JObject.class, "foo#123.element.name", null);
        new ReferencePath(jdb, JObject.class, "foo#456.element.name", null);

        // this should fail - ambiguous path of references
        try {
            new ReferencePath(jdb, JObject.class, "foo.element.name", null);
            assert false : "path was supposed to be invalid";
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

// Model Classes

    public interface HasFriend {

        @JField(storageId = 110)
        Person getFriend();
        void setFriend(Person x);
    }

    @JSimpleClass(storageId = 100)
    public abstract static class Person implements HasFriend, JObject {

        @JField(storageId = 101)
        public abstract boolean getZ();
        public abstract void setZ(boolean value);

        @JMapField(storageId = 140,
          key = @JField(storageId = 141, indexed = true),
          value = @JField(storageId = 142, indexed = true))
        public abstract NavigableMap<Person, Float> getRatings();
    }

    @JSimpleClass(storageId = 200)
    public abstract static class MeanPerson extends Person {

        @JListField(storageId = 150, element = @JField(storageId = 151))
        public abstract List<Person> getEnemies();
    }

// Wacky paths

    @JSimpleClass
    public abstract static class WackyPaths1 implements JObject {

        @JField(storageId = 123)
        public abstract JObject getFoo();
        public abstract void setFoo(JObject x);

        public abstract String getName();
        public abstract void setName(String x);
    }

    @JSimpleClass
    public abstract static class WackyPaths2 implements JObject {

        public abstract JObject getElement();
        public abstract void setElement(JObject x);
    }

    @JSimpleClass
    public abstract static class WackyPaths3 implements JObject {

        @JListField(storageId = 456)
        public abstract List<JObject> getFoo();
    }
}

