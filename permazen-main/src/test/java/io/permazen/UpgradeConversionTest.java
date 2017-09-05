
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.JSimpleClass;
import io.permazen.annotation.OnVersionChange;
import io.permazen.core.Database;
import io.permazen.core.EnumValue;
import io.permazen.core.ObjId;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.test.TestSupport;

import java.util.Arrays;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UpgradeConversionTest extends TestSupport {

    @Test
    public void testUpgradeConversion() {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

    // Version 1

        final ObjId id1;
        final ObjId id2;

        final boolean f1 = true;
        final byte f2 = (byte)-123;
        final char f3 = 'a';
        final short f4 = 12345;
        final int f5 = Integer.MAX_VALUE;
        final float f6 = -123.45f;
        final long f7 = 1L << 43;
        final double f8 = -Double.MIN_VALUE;
        final Integer f9 = null;
        final String f10 = "67890";
        final String f11 = "foobar";
        final Enum1 f12 = Enum1.RIGHT;
        final Enum1 f13 = Enum1.LEFT;
        final float[][] f14 = new float[][] { { 1.25f, -3f }, null, { 999999f }, { } };
        final char[][] f15 = new char[][] { { 'x', 'y', 'z' }, null, { } };
        final Enum1 f16 = Enum1.RIGHT;
        final Enum1 f17 = Enum1.LEFT;
        final long f18 = 0x73456789abL;
        final int f19 = -12345678;
        final long f21 = 0x3373373373L;

        JSimpleDB jdb = new JSimpleDB(db, 1, new DefaultStorageIdGenerator(), Arrays.<Class<?>>asList(Person1.class));
        JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final Person1 jobj1 = jtx.create(Person1.class);
            id1 = jobj1.getObjId();
            final Person1 jobj2 = jtx.create(Person1.class);
            id2 = jobj2.getObjId();

            jobj1.setField1(f1);
            jobj1.setField2(f2);
            jobj1.setField3(f3);
            jobj1.setField4(f4);
            jobj1.setField5(f5);
            jobj1.setField6(f6);
            jobj1.setField7(f7);
            jobj1.setField8(f8);
            jobj1.setField9(f9);
            jobj1.setField10(f10);
            jobj1.setField11(f11);
            jobj1.setField12(f12);
            jobj1.setField13(f13);
            jobj1.setField14(f14);
            jobj1.setField15(f15);
            jobj1.setField16(f16);

            jobj1.getField18().set(f18);
            jobj1.setField19(f19);
            jobj1.getField21().set(f21);

            jobj2.setField17(Enum1.LEFT);

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }

    // Version 2

        jdb = new JSimpleDB(db, 2, new DefaultStorageIdGenerator(), Arrays.<Class<?>>asList(Person2.class));
        jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

        // Successful conversions

            final Person2 jobj1 = jtx.get(id1, Person2.class);

            Assert.assertEquals(jobj1.getField1(), f1 ? 1 : 0);
            Assert.assertEquals(jobj1.getField2(), (float)f2);
            Assert.assertEquals(jobj1.getField3(), (short)f3);
            Assert.assertEquals(jobj1.getField4(), (byte)f4);
            Assert.assertEquals(jobj1.getField5(), f5 != 0);
            Assert.assertEquals(jobj1.getField6(), (int)f6);
            Assert.assertEquals(jobj1.getField7(), (char)f7);
            Assert.assertEquals(jobj1.getField8(), "" + f8);
            Assert.assertEquals(jobj1.getField9(), 0);
            Assert.assertEquals(jobj1.getField10(), Integer.decode(f10));
            Assert.assertEquals(jobj1.getField11(), null);
            Assert.assertEquals(jobj1.getField12(), Enum2.RIGHT);
            Assert.assertEquals(jobj1.getField13(), null);
            Assert.assertTrue(Arrays.deepEquals(jobj1.getField14(),
              new String[][] { { "" + 1.25f, "" + -3f }, null, { "" + 999999f }, { } }));
            Assert.assertTrue(Arrays.deepEquals(jobj1.getField15(),
              new byte[][] { { (byte)'x', (byte)'y', (byte)'z' }, null, { } }));
            Assert.assertEquals(jobj1.getField16(), null);
            Assert.assertEquals(jobj1.getField18(), (byte)f18);
            Assert.assertEquals(jobj1.getField19().get(), (long)f19);
            Assert.assertEquals(jobj1.getField20(), 0);                 // field did not exist before
            Assert.assertEquals(jobj1.getField21().get(), f21);

        // Failed conversion

            final Person2 jobj2 = jtx.get(id2, Person2.class);

            try {
                jobj2.upgrade();
                assert false : "expected UpgradeConversionException";
            } catch (UpgradeConversionException e) {
                this.log.info("got expected " + e);
            }

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Version 1

    public enum Enum1 {
        LEFT,
        RIGHT
    }

    @JSimpleClass(storageId = 100)
    public abstract static class Person1 implements JObject {

        public abstract boolean getField1();
        public abstract void setField1(boolean x);

        public abstract byte getField2();
        public abstract void setField2(byte x);

        public abstract char getField3();
        public abstract void setField3(char x);

        public abstract short getField4();
        public abstract void setField4(short x);

        public abstract int getField5();
        public abstract void setField5(int x);

        public abstract float getField6();
        public abstract void setField6(float x);

        public abstract long getField7();
        public abstract void setField7(long x);

        public abstract double getField8();
        public abstract void setField8(double x);

        public abstract Integer getField9();
        public abstract void setField9(Integer x);

        public abstract String getField10();
        public abstract void setField10(String x);

        public abstract String getField11();
        public abstract void setField11(String x);

        public abstract Enum1 getField12();
        public abstract void setField12(Enum1 x);

        public abstract Enum1 getField13();
        public abstract void setField13(Enum1 x);

        public abstract float[][] getField14();
        public abstract void setField14(float[][] x);

        public abstract char[][] getField15();
        public abstract void setField15(char[][] x);

        public abstract Enum1 getField16();
        public abstract void setField16(Enum1 x);

        public abstract Enum1 getField17();
        public abstract void setField17(Enum1 x);

        public abstract Counter getField18();

        public abstract int getField19();
        public abstract void setField19(int x);

        // No field20 exists

        public abstract Counter getField21();
    }

// Version 2

    public enum Enum2 {
        RIGHT,
        WRONG
    }

    @JSimpleClass(storageId = 100)
    public abstract static class Person2 implements JObject {

        @JField(upgradeConversion = UpgradeConversionPolicy.ATTEMPT)
        public abstract int getField1();
        public abstract void setField1(int x);

        @JField(upgradeConversion = UpgradeConversionPolicy.ATTEMPT)
        public abstract float getField2();
        public abstract void setField2(float x);

        @JField(upgradeConversion = UpgradeConversionPolicy.ATTEMPT)
        public abstract short getField3();
        public abstract void setField3(short x);

        @JField(upgradeConversion = UpgradeConversionPolicy.ATTEMPT)
        public abstract byte getField4();
        public abstract void setField4(byte x);

        @JField(upgradeConversion = UpgradeConversionPolicy.ATTEMPT)
        public abstract boolean getField5();
        public abstract void setField5(boolean x);

        @JField(upgradeConversion = UpgradeConversionPolicy.ATTEMPT)
        public abstract int getField6();
        public abstract void setField6(int x);

        @JField(upgradeConversion = UpgradeConversionPolicy.ATTEMPT)
        public abstract char getField7();
        public abstract void setField7(char x);

        @JField(upgradeConversion = UpgradeConversionPolicy.ATTEMPT)
        public abstract String getField8();
        public abstract void setField8(String x);

        @JField(upgradeConversion = UpgradeConversionPolicy.ATTEMPT)
        public abstract int getField9();
        public abstract void setField9(int x);

        @JField(upgradeConversion = UpgradeConversionPolicy.ATTEMPT)
        public abstract Integer getField10();
        public abstract void setField10(Integer x);

        @JField(upgradeConversion = UpgradeConversionPolicy.ATTEMPT)
        public abstract Enum1 getField11();
        public abstract void setField11(Enum1 x);

        @JField(upgradeConversion = UpgradeConversionPolicy.ATTEMPT)
        public abstract Enum2 getField12();
        public abstract void setField12(Enum2 x);

        @JField(upgradeConversion = UpgradeConversionPolicy.ATTEMPT)
        public abstract Enum2 getField13();
        public abstract void setField13(Enum2 x);

        @JField(upgradeConversion = UpgradeConversionPolicy.ATTEMPT)
        public abstract String[][] getField14();
        public abstract void setField14(String[][] x);

        @JField(upgradeConversion = UpgradeConversionPolicy.ATTEMPT)
        public abstract byte[][] getField15();
        public abstract void setField15(byte[][] x);

        @JField(upgradeConversion = UpgradeConversionPolicy.RESET)
        public abstract Enum2 getField16();
        public abstract void setField16(Enum2 x);

        @JField(upgradeConversion = UpgradeConversionPolicy.REQUIRE)
        public abstract Enum2 getField17();
        public abstract void setField17(Enum2 x);

        @JField(upgradeConversion = UpgradeConversionPolicy.REQUIRE)
        public abstract byte getField18();
        public abstract void setField18(byte x);

        @JField(upgradeConversion = UpgradeConversionPolicy.REQUIRE)
        public abstract Counter getField19();

        @JField(upgradeConversion = UpgradeConversionPolicy.REQUIRE)
        public abstract int getField20();
        public abstract void setField20(int x);

        @JField(upgradeConversion = UpgradeConversionPolicy.REQUIRE)
        public abstract Counter getField21();
    }
}

