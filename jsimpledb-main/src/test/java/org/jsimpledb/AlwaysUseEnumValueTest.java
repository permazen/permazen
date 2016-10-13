
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.util.Arrays;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.annotation.OnVersionChange;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.EnumValue;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AlwaysUseEnumValueTest extends TestSupport {

    @Test
    public void testAlwaysUseEnumValue() {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final StorageIdGenerator storageIdGenerator = new DefaultStorageIdGenerator();
        final Database db = new Database(kvstore);

        final ObjId id;

    // Version 1

        JSimpleDB jdb = new JSimpleDB(db, 1, storageIdGenerator, Arrays.<Class<?>>asList(Person1.class));
        JTransaction tx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            final Person1 p1 = tx.create(Person1.class);
            id = p1.getObjId();
            p1.setEnum1(Enum1.CCC);

            tx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }

    // Version 2

        jdb = new JSimpleDB(db, 2, storageIdGenerator, Arrays.<Class<?>>asList(Person2.class));
        tx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            tx.get(id, Person2.class).upgrade();

            tx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Enums

    public enum Enum1 {
        AAA,    // 0
        BBB,    // 1
        CCC;    // 2
    }

    public interface HasEnum {
        Enum1 getEnum1();
        void setEnum1(Enum1 enum1);
    }

    @JSimpleClass(storageId = 1)
    public abstract static class Person1 implements HasEnum, JObject {
    }

// Version 2

    @JSimpleClass(storageId = 1)
    public abstract static class Person2 implements HasEnum, JObject {

        @OnVersionChange(oldVersion = 1, newVersion = 2, alwaysUseEnumValue = false)
        private void vc1(Map<String, Object> oldValues) {
            Assert.assertEquals(oldValues, buildMap(
              "enum1", Enum1.CCC                                    // not wrapped
            ));
        }

        @OnVersionChange(oldVersion = 1, newVersion = 2, alwaysUseEnumValue = true)
        private void vc2(Map<String, Object> oldValues) {
            Assert.assertEquals(oldValues, buildMap(
              "enum1", new EnumValue("CCC", 2)                      // wrapped
            ));
        }
    }
}
