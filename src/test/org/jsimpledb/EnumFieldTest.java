
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Map;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.annotation.OnVersionChange;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.EnumField;
import org.jsimpledb.core.EnumValue;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EnumFieldTest extends TestSupport {

    @Test
    public void testEnumFieldUpgrade() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();

    // Version 1

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <EnumField name=\"enumField\" type=\"" + MyEnum.class.getName() + "\" storageId=\"2\">\n"
          + "       <Identifier>FOO</Identifier>\n"
          + "       <Identifier>BAR</Identifier>\n"
          + "       <Identifier>JAN</Identifier>\n"
          + "    </EnumField>\n"
          + "    <EnumField name=\"missingEnumField\" type=\"non.existent\" storageId=\"3\">\n"
          + "       <Identifier>FOO</Identifier>\n"
          + "       <Identifier>BAR</Identifier>\n"
          + "       <Identifier>JAN</Identifier>\n"
          + "    </EnumField>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        final Database db = new Database(kvstore);

        Transaction tx = db.createTransaction(schema1, 1, true);

        final ObjId id1 = tx.create(1);

        Assert.assertEquals(((EnumField)tx.getSchema().getObjType(1).getField(2)).getFieldType().getEnumType(), MyEnum.class);
        Assert.assertNull(((EnumField)tx.getSchema().getObjType(1).getField(3)).getFieldType().getEnumType());

    // Verify only valid values are accepted

        tx.writeSimpleField(id1, 2, new EnumValue("FOO", 0), false);
        Assert.assertEquals(tx.readSimpleField(id1, 2, false), new EnumValue("FOO", 0));
        tx.writeSimpleField(id1, 2, new EnumValue("BAR", 1), false);
        Assert.assertEquals(tx.readSimpleField(id1, 2, false), new EnumValue("BAR", 1));
        tx.writeSimpleField(id1, 2, new EnumValue("JAN", 2), false);
        Assert.assertEquals(tx.readSimpleField(id1, 2, false), new EnumValue("JAN", 2));

        try {
            tx.writeSimpleField(id1, 2, new EnumValue("FOO", 1), false);
            assert false : "expected IllegalArgumentException";
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            tx.writeSimpleField(id1, 2, new EnumValue("BLAH", 2), false);
            assert false : "expected IllegalArgumentException";
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            tx.writeSimpleField(id1, 2, new EnumValue("BLAH", 4), false);
            assert false : "expected IllegalArgumentException";
        } catch (IllegalArgumentException e) {
            // expected
        }

        tx.writeSimpleField(id1, 2, new EnumValue("FOO", 0), false);
        tx.writeSimpleField(id1, 3, new EnumValue("BAR", 1), false);

        tx.commit();

    // Version 2

        JSimpleDB jdb = new JSimpleDB(db, 2, null, Arrays.<Class<?>>asList(Foo.class));
        JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final Foo foo = jtx.get(id1, Foo.class);

            foo.upgrade();

            Assert.assertEquals(foo.getEnumField(), MyEnum.FOO);
            Assert.assertEquals(foo.getMissingEnumField(), MyEnum.BAR);

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testEnumConflict() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        try {
            new JSimpleDB(db, 1, null, Arrays.<Class<?>>asList(EnumConflict1.class, EnumConflict2.class));
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            log.info("got expected exception: " + e);
        }
    }

    @Test
    public void testFindEnum() throws Exception {

        // Try with matching name and ordinal
        final Enum1 e = Enum1.BBB;
        final Enum1 e2 = new EnumValue(e).find(Enum1.class);
        Assert.assertEquals(e2, e);

        // Try with matching name only
        final Enum1 e3 = new EnumValue(e.name(), e.ordinal() + 1).find(Enum1.class);
        Assert.assertNull(e3);

        // Try with matching ordinal only
        final Enum1 e4 = new EnumValue(e.name() + "x", e.ordinal()).find(Enum1.class);
        Assert.assertNull(e4);
    }

// Model Classes

    public enum MyEnum {
        FOO,    // 0
        BAR,    // 1
        JAN;    // 2
    }

    @JSimpleClass(storageId = 1)
    public abstract static class Foo implements JObject {

        @JField(storageId = 2)
        public abstract MyEnum getEnumField();
        public abstract void setEnumField(MyEnum value);

        @JField(storageId = 3)
        public abstract MyEnum getMissingEnumField();
        public abstract void setMissingEnumField(MyEnum value);

        @OnVersionChange
        private void versionChange(int oldVersion, int newVersion, Map<Integer, Object> oldValues) {
            Assert.assertEquals(oldValues.get(2), MyEnum.FOO);
            Assert.assertEquals(oldValues.get(3), new EnumValue("BAR", 1));
        }
    }

// EnumConflict

    public enum Enum1 {
        AAA,
        BBB,
        CCC;
    }

    public enum Enum2 {
        AAA,
        BBB,
        CCC;
    }

    @JSimpleClass(storageId = 10)
    public abstract static class EnumConflict1 implements JObject {

        @JField(storageId = 2)
        public abstract Enum1 getEnumField();
        public abstract void setEnumField(Enum1 value);
    }

    @JSimpleClass(storageId = 20)
    public abstract static class EnumConflict2 implements JObject {

        @JField(storageId = 2)
        public abstract Enum2 getEnumField();
        public abstract void setEnumField(Enum2 value);
    }

}

