
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenListField;
import io.permazen.annotation.PermazenMapField;
import io.permazen.annotation.PermazenSetField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.DeleteAction;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DeleteCascadeTest extends MainTestSupport {

    @Test
    public void testDeleteCascade() {

        final Permazen pdb = BasicTest.newPermazen(Person.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            Person root = ptx.create(Person.class);

            Person ref = ptx.create(Person.class);
            root.setRef(ref);

            Person set1 = ptx.create(Person.class);
            Person set2 = ptx.create(Person.class);
            Person set3 = ptx.create(Person.class);
            root.getSet().add(set1);
            root.getSet().add(set2);
            root.getSet().add(set3);
            root.getSet().add(null);

            Person list1 = ptx.create(Person.class);
            Person list2 = ptx.create(Person.class);
            Person list3 = ptx.create(Person.class);
            root.getList().add(null);
            root.getList().add(list1);
            root.getList().add(list2);
            root.getList().add(null);
            root.getList().add(list3);
            root.getList().add(list3);
            root.getList().add(list2);
            root.getList().add(null);
            root.getList().add(list1);
            root.getList().add(null);

            Person map1a1 = ptx.create(Person.class);
            Person map1a2 = ptx.create(Person.class);
            Person map1b1 = ptx.create(Person.class);
            Person map1b2 = ptx.create(Person.class);
            Person map1c1 = ptx.create(Person.class);
            Person map1c2 = ptx.create(Person.class);
            root.getMap1().put(map1a1, map1a2);
            root.getMap1().put(map1b1, map1b2);
            root.getMap1().put(map1c1, map1c2);

            Person map2a1 = ptx.create(Person.class);
            Person map2a2 = ptx.create(Person.class);
            Person map2b1 = ptx.create(Person.class);
            Person map2b2 = ptx.create(Person.class);
            Person map2c1 = ptx.create(Person.class);
            root.getMap2().put(map2a1, map2a2);
            root.getMap2().put(map2b1, map2b2);
            root.getMap2().put(map2c1, map2b2);

            boolean r = root.delete();
            Assert.assertTrue(r);

            Assert.assertFalse(ref.exists());

            Assert.assertFalse(set1.exists());
            Assert.assertFalse(set2.exists());
            Assert.assertFalse(set3.exists());

            Assert.assertFalse(list1.exists());
            Assert.assertFalse(list2.exists());
            Assert.assertFalse(list3.exists());

            Assert.assertFalse(map1a1.exists());
            Assert.assertFalse(map1b1.exists());
            Assert.assertFalse(map1c1.exists());
            Assert.assertTrue(map1a2.exists());
            Assert.assertTrue(map1b2.exists());
            Assert.assertTrue(map1c2.exists());

            Assert.assertTrue(map2a1.exists());
            Assert.assertTrue(map2b1.exists());
            Assert.assertTrue(map2c1.exists());
            Assert.assertFalse(map2a2.exists());
            Assert.assertFalse(map2b2.exists());

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testDeleteCircular() {

        final Permazen pdb = BasicTest.newPermazen(Person.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            Person p1 = ptx.create(Person.class);
            Person p2 = ptx.create(Person.class);
            Person p3 = ptx.create(Person.class);

            p1.setRef(p2);
            p2.setRef(p3);
            p3.setRef(p1);

            p1.delete();

            Assert.assertTrue(ptx.getAll(Person.class).isEmpty());

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public abstract static class Person implements PermazenObject {

        @PermazenField(inverseDelete = DeleteAction.IGNORE, forwardDelete = true, allowDeleted = true)
        public abstract Person getRef();
        public abstract void setRef(Person ref);

        @PermazenSetField(element = @PermazenField(inverseDelete = DeleteAction.IGNORE, forwardDelete = true, allowDeleted = true))
        public abstract Set<Person> getSet();

        @PermazenListField(element = @PermazenField(inverseDelete = DeleteAction.REMOVE, forwardDelete = true))
        public abstract List<Person> getList();

        @PermazenMapField(
          key = @PermazenField(inverseDelete = DeleteAction.IGNORE, forwardDelete = true, allowDeleted = true),
          value = @PermazenField(inverseDelete = DeleteAction.IGNORE, allowDeleted = true))
        public abstract Map<Person, Person> getMap1();

        @PermazenMapField(
          key = @PermazenField(inverseDelete = DeleteAction.IGNORE, allowDeleted = true),
          value = @PermazenField(inverseDelete = DeleteAction.IGNORE, forwardDelete = true, allowDeleted = true))
        public abstract Map<Person, Person> getMap2();
    }
}
