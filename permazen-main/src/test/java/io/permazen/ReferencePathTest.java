
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.primitives.Ints;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenListField;
import io.permazen.annotation.PermazenMapField;
import io.permazen.annotation.PermazenType;

import java.util.List;
import java.util.NavigableMap;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReferencePathTest extends MainTestSupport {

    private Permazen jdb1;
    private Permazen jdb2;

    @BeforeClass
    public void setup() {
        this.jdb1 = BasicTest.newPermazen(Person.class, MeanPerson.class, Dog.class);
        this.jdb2 = BasicTest.newPermazen(WackyPaths1.class, WackyPaths2.class, WackyPaths3.class);
    }

    @Test(dataProvider = "paths1")
    public void testReferencePath1(Class<?> startType, Class<?> targetType, boolean singular, int[] refs, String pathString)
      throws Exception {
        this.testReferencePath(this.jdb1, startType, targetType, singular, refs, pathString);
    }

    @Test(dataProvider = "paths2")
    public void testReferencePath2(Class<?> startType, Class<?> targetType, boolean singular, int[] refs, String pathString)
      throws Exception {
        this.testReferencePath(this.jdb2, startType, targetType, singular, refs, pathString);
    }

    public void testReferencePath(Permazen pdb, Class<?> startType,
      Class<?> targetType, boolean singular, int[] refs, String pathString) throws Exception {

        // Prepare args
        final boolean valid = targetType != null;
        if (refs == null)
            refs = new int[0];

        // Parse path
        final ReferencePath path;
        try {
            path = pdb.parseReferencePath(startType, pathString);
            assert valid : "path was supposed to be invalid:"
              + "\n  startType=" + startType
              + "\n  targetType=" + targetType
              + "\n  pathString=" + pathString;
        } catch (IllegalArgumentException e) {
            if (valid)
                throw new AssertionError("path was supposed to be valid but parse failed", e);
            this.log.info("got expected " + e);
            return;
        }

        // Verify parse
        Assert.assertEquals(path.isSingular(), singular, "wrong singular");
        Assert.assertEquals(path.getTargetType(), targetType, path.getTargetType()
          + " (=> " + path.getTargetTypes() + ") != " + targetType);
        Assert.assertEquals(Ints.asList(path.getReferenceFields()), Ints.asList(refs), "wrong reference fields");
        Assert.assertEquals(path.toString(), pathString, "wrong path.toString()");
    }

    @DataProvider(name = "paths1")
    public Object[][] genPaths1() {
        return new Object[][] {

        //  startType           targetType          singular    refs                path
          { Person.class,       null,               true,       null,               "." },
          { Person.class,       null,               true,       null,               "->" },
          { Person.class,       null,               true,       null,               "<-" },
          { Person.class,       null,               true,       null,               "!@#$%^&" },
          { Person.class,       null,               true,       null,               "->Person" },
          { Person.class,       null,               true,       null,               "->->Person" },
          { Person.class,       null,               true,       null,               "Person->" },
          { Person.class,       null,               true,       null,               "->Person..z" },
          { Person.class,       null,               true,       null,               "->Person.unknown" },
          { Person.class,       null,               true,       null,               "->z" },
          { Person.class,       null,               true,       null,               "Person->z" },

          { Person.class,       Person.class,       true,       ii(),               "" },
          { Person.class,       Person.class,       true,       ii(110),            "->friend" },
          { Person.class,       Dog.class,          false,      ii(-310),           "<-Dog.owner" },
          { Dog.class,          null,               true,       null,               "<-Dog.owner" },    // Dogs can't be owners
          { Person.class,       MeanPerson.class,   false,      ii(110, -151),      "->friend<-Person.enemies" },
          { MeanPerson.class,   Person.class,       false,      ii(151),            "->enemies" },
          { MeanPerson.class,   Person.class,       false,      ii(151),            "->enemies.element" },
          { MeanPerson.class,   null,               true,       null,               "->enemies.smelement" },
          { MeanPerson.class,   null,               true,       null,               "->enemies.element->ratings" },
          { MeanPerson.class,   Person.class,       false,      ii(151, 141),       "->enemies.element->ratings.key" },
          { MeanPerson.class,   null,               false,      null,               "->enemies.element->ratings.value" },
          { Person.class,       MeanPerson.class,   false,      ii(-151),           "<-MeanPerson.enemies" },
          { Person.class,       MeanPerson.class,   false,      ii(-151),           "<-MeanPerson.enemies.element" },
          { Person.class,       null,               false,      null,               "<-MeanPerson.enemies.element->z" },
          { Person.class,       Person.class,       false,      ii(151, -151, 151), "->enemies<-MeanPerson.enemies->enemies" },
        };
    }

    @DataProvider(name = "paths2")
    public Object[][] genPaths2() {
        return new Object[][] {

        //  startType               targetType              singulr refs                path
          { WackyPaths1.class,      WackyPaths1.class,      true,   ii(),               "" },
          { WackyPaths2.class,      WackyPaths2.class,      true,   ii(),               "" },
          { WackyPaths3.class,      WackyPaths3.class,      true,   ii(),               "" },

          { WackyPaths1.class,      PermazenObject.class,   true,   ii(123),            "->foo" },
          { WackyPaths1.class,      WackyPaths1.class,      true,   ii(123, 321),       "->foo->bar" },
          { WackyPaths1.class,      PermazenObject.class,   true,   ii(123, 321, 123),  "->foo->bar->foo" },

          { PermazenObject.class,   null,                   true,   null,               "->foo" },          // ambiguous
          { PermazenObject.class,   null,                   true,   null,               "->foo->element" }, // ambiguous
          { PermazenObject.class,   PermazenObject.class,   true,   ii(123),            "->foo#123" },      // disambiguated

          { PermazenObject.class,   PermazenObject.class,   true,   ii(123),            "->WackyPaths1.foo" }, // disambiguated
          { PermazenObject.class,   null,                   true,   null,               "->WackyPaths2.foo" }, // no such field
          { PermazenObject.class,   WackyPaths2.class,      false,  ii(789),            "->WackyPaths3.foo" }, // disambiguated

          { WackyPaths1.class,      null,                   true,   null,               "->foo.element" },  // wrong start type
          { WackyPaths2.class,      null,                   true,   null,               "->foo.element" },  // wrong start type
          { WackyPaths3.class,      WackyPaths2.class,      false,  ii(789),            "->foo.element" },
          { WackyPaths3.class,      WackyPaths2.class,      false,  ii(789),            "->foo"         },  // abbreviated form
          { PermazenObject.class,   WackyPaths2.class,      false,  ii(789),            "->foo.element" },

          { PermazenObject.class,   WackyPaths1.class,      false,  ii(-321, 321),      "<-WackyPaths2.bar->bar" },
          { PermazenObject.class,   WackyPaths1.class,      false,  ii(-321, 321),      "<-io.permazen.PermazenObject.bar->bar" },
        };
    }

    private static int[] ii(int... ints) {
        return ints;
    }

// Model Classes

    public interface HasFriend {

        @PermazenField(storageId = 110)
        Person getFriend();
        void setFriend(Person x);
    }

    @PermazenType(storageId = 100)
    public abstract static class Person implements HasFriend, PermazenObject {

        @PermazenField(storageId = 101)
        public abstract boolean getZ();
        public abstract void setZ(boolean value);

        @PermazenMapField(storageId = 140,
          key = @PermazenField(storageId = 141, indexed = true),
          value = @PermazenField(storageId = 142, indexed = true))
        public abstract NavigableMap<Person, Float> getRatings();
    }

    @PermazenType(storageId = 200)
    public abstract static class MeanPerson extends Person {

        @PermazenListField(storageId = 150, element = @PermazenField(storageId = 151))
        public abstract List<Person> getEnemies();
    }

    @PermazenType(storageId = 300)
    public abstract static class Dog implements PermazenObject {

        @PermazenField(storageId = 310)
        public abstract Person getOwner();
        public abstract void setOwner(Person x);
    }

// Wacky paths

    @PermazenType
    public abstract static class WackyPaths1 implements PermazenObject {

        @PermazenField(storageId = 123)
        public abstract PermazenObject getFoo();
        public abstract void setFoo(PermazenObject x);

        public abstract String getName();
        public abstract void setName(String x);
    }

    @PermazenType
    public abstract static class WackyPaths2 implements PermazenObject {

        public abstract PermazenObject getElement();
        public abstract void setElement(PermazenObject x);

        @PermazenField(storageId = 321)
        public abstract WackyPaths1 getBar();
        public abstract void setBar(WackyPaths1 x);
    }

    @PermazenType
    public abstract static class WackyPaths3 implements PermazenObject {

        @PermazenListField(storageId = 456,
          element = @PermazenField(storageId = 789))
        public abstract List<WackyPaths2> getFoo();
    }
}
