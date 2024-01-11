
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnSchemaChange;
import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.core.EnumValue;
import io.permazen.core.InvalidSchemaException;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;
import io.permazen.core.TransactionConfig;
import io.permazen.kv.simple.MemoryKVDatabase;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class EnumFieldTest extends MainTestSupport {

    @Test
    public void testEnumFieldUpgrade() throws Exception {

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
        schema1.lockDown(true);
        final SchemaId schemaId1 = schema1.getSchemaId();

        final Database db = new Database(new MemoryKVDatabase());

        final TransactionConfig txConfig1 = TransactionConfig.builder()
          .schemaModel(schema1)
          .build();
        Transaction tx = db.createTransaction(txConfig1);

        final ObjId id1 = tx.create("Foo");

    // Verify only valid values are accepted

        tx.writeSimpleField(id1, "enumField", new EnumValue("FOO", 0), false);
        Assert.assertEquals(tx.readSimpleField(id1, "enumField", false), new EnumValue("FOO", 0));
        tx.writeSimpleField(id1, "enumField", new EnumValue("BAR", 1), false);
        Assert.assertEquals(tx.readSimpleField(id1, "enumField", false), new EnumValue("BAR", 1));
        tx.writeSimpleField(id1, "enumField", new EnumValue("JAN", 2), false);
        Assert.assertEquals(tx.readSimpleField(id1, "enumField", false), new EnumValue("JAN", 2));

        try {
            tx.writeSimpleField(id1, "enumField", new EnumValue("FOO", 1), false);
            assert false : "expected IllegalArgumentException";
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            tx.writeSimpleField(id1, "enumField", new EnumValue("BLAH", 2), false);
            assert false : "expected IllegalArgumentException";
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            tx.writeSimpleField(id1, "enumField", new EnumValue("BLAH", 4), false);
            assert false : "expected IllegalArgumentException";
        } catch (IllegalArgumentException e) {
            // expected
        }

        tx.writeSimpleField(id1, "enumField",        new EnumValue("FOO", 0), false);
        tx.writeSimpleField(id1, "missingEnumField", new EnumValue("BAR", 1), false);

        tx.commit();

    // Version 2

        Permazen pdb = BasicTest.newPermazen(db, Foo.class);
        final SchemaId schemaId2 = pdb.getSchemaModel().getSchemaId();
        PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final Foo foo = ptx.get(id1, Foo.class);
            Assert.assertEquals(foo.getSchemaId(), schemaId1);

            foo.migrateSchema();
            Assert.assertEquals(foo.getSchemaId(), schemaId2);

            Assert.assertEquals(foo.getEnumField(), MyEnum.FOO);
            Assert.assertEquals(foo.getMissingEnumField(), MyEnum.BAR);

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testEnumNoConflict() throws Exception {

        final Database db = new Database(new MemoryKVDatabase());

        BasicTest.newPermazen(db, EnumNoConflict1.class, EnumNoConflict2.class);
    }

    @Test
    public void testEnumConflict() throws Exception {
        try {
            BasicTest.newPermazen(EnumConflict1.class, EnumConflict2.class);
            assert false : "expected exception";
        } catch (InvalidSchemaException e) {
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
        final Permazen permazen = BasicTest.newPermazen(Foo.class);
        final PermazenTransaction ptx = permazen.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final Foo pobj = ptx.create(Foo.class);
            pobj.setEnumField(MyEnum.FOO);

            final PermazenEnumField field = permazen.getPermazenClass(Foo.class).getField("enumField", PermazenEnumField.class);
            Assert.assertEquals(pobj.getEnumField(), MyEnum.FOO);
            Assert.assertEquals(field.getValue(pobj), MyEnum.FOO);
            field.setValue(pobj, MyEnum.BAR);
            Assert.assertEquals(pobj.getEnumField(), MyEnum.BAR);
            Assert.assertEquals(field.getValue(pobj), MyEnum.BAR);

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testEnumArrays() throws Exception {
        final Permazen permazen = BasicTest.newPermazen(EnumArrays.class);
        final PermazenTransaction ptx = permazen.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final MyEnum[] value1a = new MyEnum[] { null, MyEnum.BAR, MyEnum.FOO, null, null, MyEnum.FOO, MyEnum.FOO };
            final MyEnum[] value1b = new MyEnum[] { MyEnum.BAR, MyEnum.BAR, MyEnum.BAR, null, MyEnum.FOO };
            final MyEnum[][] value2a = new MyEnum[][] { null, value1a, null, value1b, null };
            final MyEnum[][] value2b = new MyEnum[][] { value1b, value1a };

            final EnumArrays pobj = ptx.create(EnumArrays.class);

            Assert.assertEquals(pobj.getEnums(), null);
            pobj.setEnums(value1a);
            Assert.assertTrue(Arrays.deepEquals(pobj.getEnums(), value1a));
            pobj.setEnums(value1b);
            Assert.assertTrue(Arrays.deepEquals(pobj.getEnums(), value1b));
            pobj.setEnums(null);
            Assert.assertEquals(pobj.getEnums(), null);

            Assert.assertEquals(pobj.getEnums2(), null);
            pobj.setEnums2(value2a);
            Assert.assertTrue(Arrays.deepEquals(pobj.getEnums2(), value2a));
            pobj.setEnums2(value2b);
            Assert.assertTrue(Arrays.deepEquals(pobj.getEnums2(), value2b));
            pobj.setEnums2(null);
            Assert.assertEquals(pobj.getEnums2(), null);

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testListOfEnumUpgrade() throws Exception {

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

        final Database db = new Database(new MemoryKVDatabase());

        final TransactionConfig txConfig1 = TransactionConfig.builder()
          .schemaModel(schema1)
          .build();
        Transaction tx = db.createTransaction(txConfig1);

        final ObjId id1 = tx.create("Foo");

        final List<EnumValue> list = (List<EnumValue>)tx.readListField(id1, "enumList", true);
        list.add(new EnumValue("TAN", 0));
        list.add(new EnumValue("BAR", 1));
        list.add(new EnumValue("JAN", 2));

        tx.commit();

    // Version 2

        Permazen pdb = BasicTest.newPermazen(db, Foo2.class);
        PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final Foo2 foo2 = ptx.get(id1, Foo2.class);

            foo2.migrateSchema();

            Assert.assertEquals(foo2.getEnumList(), Collections.emptyList());

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    public enum MyEnum {
        FOO,    // 0
        BAR,    // 1
        JAN;    // 2
    }

    @PermazenType(storageId = 1)
    public abstract static class Foo implements PermazenObject {

        @PermazenField(storageId = 2)
        public abstract MyEnum getEnumField();
        public abstract void setEnumField(MyEnum value);

        @PermazenField(storageId = 3)
        public abstract MyEnum getMissingEnumField();
        public abstract void setMissingEnumField(MyEnum value);

        @OnSchemaChange
        private void versionChange(Map<String, Object> oldValues) {
            Assert.assertEquals(oldValues.get(2), new EnumValue(MyEnum.FOO));
            Assert.assertEquals(oldValues.get(3), new EnumValue("BAR", 1));
        }
    }

    @PermazenType
    public abstract static class EnumArrays implements PermazenObject {

        public abstract MyEnum[] getEnums();
        public abstract void setEnums(MyEnum[] value);

        public abstract MyEnum[][] getEnums2();
        public abstract void setEnums2(MyEnum[][] value);
    }

    @PermazenType(name = "Foo")
    public abstract static class Foo2 implements PermazenObject {

        @io.permazen.annotation.PermazenListField
        public abstract List<MyEnum> getEnumList();

        @OnSchemaChange
        private void versionChange(Map<String, Object> oldValues) {
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

    public enum Enum3 {
        DDD,
        EEE,
        FFF;
    }

    @PermazenType(storageId = 10)
    public abstract static class EnumNoConflict1 implements PermazenObject {

        @PermazenField(storageId = 2, indexed = true)
        public abstract Enum1 getEnumField();
        public abstract void setEnumField(Enum1 value);
    }

    @PermazenType(storageId = 20)
    public abstract static class EnumNoConflict2 implements PermazenObject {

        @PermazenField(storageId = 2, indexed = true)
        public abstract Enum2 getEnumField();
        public abstract void setEnumField(Enum2 value);
    }

    @PermazenType(storageId = 10)
    public abstract static class EnumConflict1 implements PermazenObject {

        @PermazenField(storageId = 2)
        public abstract Enum1 getEnumField();
        public abstract void setEnumField(Enum1 value);
    }

    @PermazenType(storageId = 20)
    public abstract static class EnumConflict2 implements PermazenObject {

        @PermazenField(storageId = 2)
        public abstract Enum3 getEnumField();
        public abstract void setEnumField(Enum3 value);
    }
}
