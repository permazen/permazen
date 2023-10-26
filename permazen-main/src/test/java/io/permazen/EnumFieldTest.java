
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.OnVersionChange;
import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.core.EnumValue;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;
import io.permazen.test.TestSupport;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class EnumFieldTest extends TestSupport {

    @Test
    public void testEnumFieldUpgrade() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();

    // Version 1

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <EnumField name=\"enumField\" storageId=\"2\">\n"
          + "       <Identifier>FOO</Identifier>\n"
          + "       <Identifier>BAR</Identifier>\n"
          + "       <Identifier>JAN</Identifier>\n"
          + "    </EnumField>\n"
          + "    <EnumField name=\"missingEnumField\" storageId=\"3\">\n"
          + "       <Identifier>FOO</Identifier>\n"
          + "       <Identifier>BAR</Identifier>\n"
          + "       <Identifier>JAN</Identifier>\n"
          + "    </EnumField>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        final Database db = new Database(kvstore);

        Transaction tx = db.createTransaction(schema1, 1, true);

        final ObjId id1 = tx.create(1);

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

        Permazen jdb = new Permazen(db, 2, null, Arrays.<Class<?>>asList(Foo.class));
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
    public void testEnumNoConflict() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        new Permazen(db, 1, null, Arrays.<Class<?>>asList(EnumNoConflict1.class, EnumNoConflict2.class));
    }

    @Test
    public void testEnumConflict() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

        try {
            new Permazen(db, 1, null, Arrays.<Class<?>>asList(EnumConflict1.class, EnumConflict2.class));
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

    @Test
    public void testEnumGetSetValue() throws Exception {
        final Permazen permazen = BasicTest.getPermazen(Foo.class);
        final JTransaction jtx = permazen.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final Foo jobj = jtx.create(Foo.class);
            jobj.setEnumField(MyEnum.FOO);

            final JEnumField field = permazen.getJClass(Foo.class).getJField(2, JEnumField.class);
            Assert.assertEquals(jobj.getEnumField(), MyEnum.FOO);
            Assert.assertEquals(field.getValue(jobj), MyEnum.FOO);
            field.setValue(jobj, MyEnum.BAR);
            Assert.assertEquals(jobj.getEnumField(), MyEnum.BAR);
            Assert.assertEquals(field.getValue(jobj), MyEnum.BAR);

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testEnumArrays() throws Exception {
        final Permazen permazen = BasicTest.getPermazen(EnumArrays.class);
        final JTransaction jtx = permazen.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final MyEnum[] value1a = new MyEnum[] { null, MyEnum.BAR, MyEnum.FOO, null, null, MyEnum.FOO, MyEnum.FOO };
            final MyEnum[] value1b = new MyEnum[] { MyEnum.BAR, MyEnum.BAR, MyEnum.BAR, null, MyEnum.FOO };
            final MyEnum[][] value2a = new MyEnum[][] { null, value1a, null, value1b, null };
            final MyEnum[][] value2b = new MyEnum[][] { value1b, value1a };

            final EnumArrays jobj = jtx.create(EnumArrays.class);

            Assert.assertEquals(jobj.getEnums(), null);
            jobj.setEnums(value1a);
            Assert.assertTrue(Arrays.deepEquals(jobj.getEnums(), value1a));
            jobj.setEnums(value1b);
            Assert.assertTrue(Arrays.deepEquals(jobj.getEnums(), value1b));
            jobj.setEnums(null);
            Assert.assertEquals(jobj.getEnums(), null);

            Assert.assertEquals(jobj.getEnums2(), null);
            jobj.setEnums2(value2a);
            Assert.assertTrue(Arrays.deepEquals(jobj.getEnums2(), value2a));
            jobj.setEnums2(value2b);
            Assert.assertTrue(Arrays.deepEquals(jobj.getEnums2(), value2b));
            jobj.setEnums2(null);
            Assert.assertEquals(jobj.getEnums2(), null);

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testListOfEnumUpgrade() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();

    // Version 1

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"1\">\n"
          + "    <ListField name=\"enumList\" storageId=\"2\">\n"
          + "      <EnumField storageId=\"3\">\n"
          + "         <Identifier>TAN</Identifier>\n"               /* note "TAN" instead of "FOO" */
          + "         <Identifier>BAR</Identifier>\n"
          + "         <Identifier>JAN</Identifier>\n"
          + "      </EnumField>\n"
          + "    </ListField>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        final Database db = new Database(kvstore);

        Transaction tx = db.createTransaction(schema1, 1, true);

        final ObjId id1 = tx.create(1);

        final List<EnumValue> list = (List<EnumValue>)tx.readListField(id1, 2, true);
        list.add(new EnumValue("TAN", 0));
        list.add(new EnumValue("BAR", 1));
        list.add(new EnumValue("JAN", 2));

        tx.commit();

    // Version 2

        Permazen jdb = new Permazen(db, 2, null, Arrays.<Class<?>>asList(Foo2.class));
        JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final Foo2 foo2 = jtx.get(id1, Foo2.class);

            foo2.upgrade();

            Assert.assertEquals(foo2.getEnumList(), Collections.emptyList());

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    public enum MyEnum {
        FOO,    // 0
        BAR,    // 1
        JAN;    // 2
    }

    @PermazenType(storageId = 1)
    public abstract static class Foo implements JObject {

        @JField(storageId = 2)
        public abstract MyEnum getEnumField();
        public abstract void setEnumField(MyEnum value);

        @JField(storageId = 3)
        public abstract MyEnum getMissingEnumField();
        public abstract void setMissingEnumField(MyEnum value);

        @OnVersionChange
        private void versionChange(int oldVersion, int newVersion, Map<Integer, Object> oldValues) {
            Assert.assertEquals(oldValues.get(2), new EnumValue(MyEnum.FOO));
            Assert.assertEquals(oldValues.get(3), new EnumValue("BAR", 1));
        }
    }

    @PermazenType
    public abstract static class EnumArrays implements JObject {

        public abstract MyEnum[] getEnums();
        public abstract void setEnums(MyEnum[] value);

        public abstract MyEnum[][] getEnums2();
        public abstract void setEnums2(MyEnum[][] value);
    }

    @PermazenType(storageId = 1)
    public abstract static class Foo2 implements JObject {

        @io.permazen.annotation.JListField(storageId = 2, element = @JField(storageId = 3))
        public abstract List<MyEnum> getEnumList();

        @OnVersionChange
        private void versionChange(int oldVersion, int newVersion, Map<String, Object> oldValues) {
            Assert.assertEquals(oldValues.get("enumList"), Arrays.asList(
              new EnumValue("TAN", 0),
              new EnumValue("BAR", 1),
              new EnumValue("JAN", 2)));
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

    @PermazenType(storageId = 10)
    public abstract static class EnumNoConflict1 implements JObject {

        @JField(storageId = 2)
        public abstract Enum1 getEnumField();
        public abstract void setEnumField(Enum1 value);
    }

    @PermazenType(storageId = 20)
    public abstract static class EnumNoConflict2 implements JObject {

        @JField(storageId = 2)
        public abstract Enum2 getEnumField();
        public abstract void setEnumField(Enum2 value);
    }

    @PermazenType(storageId = 10)
    public abstract static class EnumConflict1 implements JObject {

        @JField(storageId = 2, indexed = true)
        public abstract Enum1 getEnumField();
        public abstract void setEnumField(Enum1 value);
    }

    @PermazenType(storageId = 20)
    public abstract static class EnumConflict2 implements JObject {

        @JField(storageId = 2, indexed = true)
        public abstract Enum2 getEnumField();
        public abstract void setEnumField(Enum2 value);
    }
}
