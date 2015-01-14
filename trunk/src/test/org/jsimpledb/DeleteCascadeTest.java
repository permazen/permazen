
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JListField;
import org.jsimpledb.annotation.JMapField;
import org.jsimpledb.annotation.JSetField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.DeleteAction;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DeleteCascadeTest extends TestSupport {

    @Test
    public void testDeleteCascade() {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            Person root = jtx.create(Person.class);

            Person ref = jtx.create(Person.class);
            root.setRef(ref);

            Person set1 = jtx.create(Person.class);
            Person set2 = jtx.create(Person.class);
            Person set3 = jtx.create(Person.class);
            root.getSet().add(set1);
            root.getSet().add(set2);
            root.getSet().add(set3);
            root.getSet().add(null);

            Person list1 = jtx.create(Person.class);
            Person list2 = jtx.create(Person.class);
            Person list3 = jtx.create(Person.class);
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

            Person map1a1 = jtx.create(Person.class);
            Person map1a2 = jtx.create(Person.class);
            Person map1b1 = jtx.create(Person.class);
            Person map1b2 = jtx.create(Person.class);
            Person map1c1 = jtx.create(Person.class);
            Person map1c2 = jtx.create(Person.class);
            root.getMap1().put(map1a1, map1a2);
            root.getMap1().put(map1b1, map1b2);
            root.getMap1().put(map1c1, map1c2);

            Person map2a1 = jtx.create(Person.class);
            Person map2a2 = jtx.create(Person.class);
            Person map2b1 = jtx.create(Person.class);
            Person map2b2 = jtx.create(Person.class);
            Person map2c1 = jtx.create(Person.class);
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

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @JSimpleClass
    public abstract static class Person implements JObject {

        @JField(onDelete = DeleteAction.NOTHING, cascadeDelete = true)
        public abstract Person getRef();
        public abstract void setRef(Person ref);

        @JSetField(element = @JField(onDelete = DeleteAction.NOTHING, cascadeDelete = true))
        public abstract Set<Person> getSet();

        @JListField(element = @JField(onDelete = DeleteAction.UNREFERENCE, cascadeDelete = true))
        public abstract List<Person> getList();

        @JMapField(
          key = @JField(onDelete = DeleteAction.NOTHING, cascadeDelete = true),
          value = @JField(onDelete = DeleteAction.NOTHING))
        public abstract Map<Person, Person> getMap1();

        @JMapField(
          key = @JField(onDelete = DeleteAction.NOTHING),
          value = @JField(onDelete = DeleteAction.NOTHING, cascadeDelete = true))
        public abstract Map<Person, Person> getMap2();
    }
}

