
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.primitives.Ints;

import java.util.List;
import java.util.NavigableMap;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JListField;
import org.jsimpledb.annotation.JMapField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReferencePathTest extends TestSupport {

    private JSimpleDB jdb;

    @BeforeClass
    public void setup() {
        this.jdb = BasicTest.getJSimpleDB(Person.class, MeanPerson.class);
    }

    @Test(dataProvider = "paths")
    public void testReferencePath(Class<?> startType, Class<?> targetType, int targetField,
      int targetSuperField, int[] refs, boolean withTargetField, Boolean lastIsSubField, String pathString) throws Exception {

        // Prepare args
        final boolean valid = targetType != null;
        if (refs == null)
            refs = new int[0];

        // Parse path
        final ReferencePath path;
        try {
            path = new ReferencePath(this.jdb, startType, pathString, withTargetField, lastIsSubField);
            assert valid : "path was supposed to be invalid";
        } catch (IllegalArgumentException e) {
            if (valid)
                throw new RuntimeException("path was supposed to be valid but parse failed", e);
            return;
        }

        // Verify parse
        Assert.assertEquals(path.startType, startType);
        Assert.assertTrue(path.getTargetTypes().contains(targetType), path.getTargetTypes() + " does not contain " + targetType);
        Assert.assertEquals(path.targetFieldStorageId, targetField, "wrong target field");
        Assert.assertEquals(path.targetSuperFieldStorageId, targetSuperField, "wrong target superfield");
        Assert.assertEquals(Ints.asList(path.getReferenceFields()), Ints.asList(refs), "wrong reference fields");
        Assert.assertEquals(path.path, pathString, "wrong path");
        Assert.assertEquals(path.toString(), pathString, "wrong path.toString()");
    }

    @DataProvider(name = "paths")
    public Object[][] genPaths() {
        return new Object[][] {

        //  startType                 targetSuperField          withTargetField
        //  targetType        targetField     refs                     lastIsSubField

          { Person.class,
            null,             -1,     -1,     null,             true,  null,   "" },
          { Person.class,
            Person.class,     0,      0,      ii(),             false, null,   "" },
          { Person.class,
            null,             -1,     -1,     null,             true,  null,   "." },
          { Person.class,
            null,             -1,     -1,     null,             true,  null,   "!@#$%^&" },
          { Person.class,
            null,             -1,     -1,     null,             true,  null,   ".Person" },
          { Person.class,
            null,             -1,     -1,     null,             true,  null,   "Person." },
          { Person.class,
            null,             -1,     -1,     null,             true,  null,   "Person..z" },
          { Person.class,
            null,             -1,     -1,     null,             true,  null,   "Person.unknown" },

          { Person.class,
            Person.class,     101,    0,      null,             true,  null,   "z" },
          { Person.class,
            null,             -1,     -1,     null,             true,  null,   "Person.z" },

          { MeanPerson.class,
            MeanPerson.class, 150,    0,      null,             true,  null,   "enemies" },
          { MeanPerson.class,
            MeanPerson.class, 150,    0,      null,             true,  false,  "enemies" },
          { MeanPerson.class,
            null,             -1,     -1,     null,             true,  true,   "enemies" },

          { MeanPerson.class,
            MeanPerson.class, 151,    150,    null,             true,  null,   "enemies.element" },
          { MeanPerson.class,
            MeanPerson.class, 151,    150,    null,             true,  true,   "enemies.element" },
          { MeanPerson.class,
            null,             -1,     -1,     null,             true,  false,  "enemies.element" },

          { MeanPerson.class,
            Person.class,     101,    0,      ii(151),          true,  null,   "enemies.element.z" },
          { MeanPerson.class,
            Person.class,     141,    140,    ii(151),          true,  null,   "enemies.element.ratings.key" },
          { MeanPerson.class,
            Person.class,     101,    0,      ii(151, 141),     true,  null,   "enemies.element.ratings.key.z" },

      // Sub-field
          { Person.class,
            MeanPerson.class, 150,    0,      null,             true,  false,  "enemies" },

      // Reverse paths

        //  startType                 targetSuperField          withTargetField
        //  targetType        targetField     refs                     lastIsSubField

          { Person.class,
            null,             -1,     -1,     null,             true,  false,  "^MeanPerson:enemies.element^" },
          { Person.class,
            null,             -1,     -1,     null,             false, false,  "^MeanPerson:enemies^" },
          { Person.class,
            null,             -1,     -1,     null,             false, true,   "^MeanPerson:enemies^" },
          { Person.class,
            MeanPerson.class, 0,      0,      ii(-151),         false, false,  "^MeanPerson:enemies.element^" },
          { Person.class,
            null,             -1,     -1,     null,             false, false,  "^MeanPerson:enemies.element^.z" },
          { Person.class,
            MeanPerson.class, 101,    0,      ii(-151),         true,  false,  "^MeanPerson:enemies.element^.z" },
        };
    }

    private static int[] ii(int... ints) {
        return ints;
    }

    @Test
    public void testWackyPaths() throws Exception {
        final JSimpleDB jdb2 = BasicTest.getJSimpleDB(WackyPaths1.class, WackyPaths2.class, WackyPaths3.class);

        // this should be OK - two hops through two reference fields: foo->element->name
        new ReferencePath(jdb2, WackyPaths1.class, "foo.element.name", true, null);

        // this should be OK - one hop through one set element sub-field: foo.element->name
        new ReferencePath(jdb2, WackyPaths3.class, "foo.element.name", true, null);

        // these should be OK - we are disambiguating which "foo" field by specifying the storage ID
        new ReferencePath(jdb2, JObject.class, "foo#123.element.name", true, null);
        new ReferencePath(jdb2, JObject.class, "foo#456.element.name", true, null);

        // this should be OK - second "foo" must be in the context of WackyPaths3
        new ReferencePath(jdb2, JObject.class, "foo.^java.lang.Object:bar^.foo.element", false, null);

        // this should fail - ambiguous path of references
        try {
            new ReferencePath(jdb2, JObject.class, "foo.element.name", true, null);
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

        public abstract JObject getBar();
        public abstract void setBar(JObject x);
    }
}

