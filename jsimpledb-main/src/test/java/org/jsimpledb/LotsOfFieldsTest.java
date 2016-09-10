
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JListField;
import org.jsimpledb.annotation.JMapField;
import org.jsimpledb.annotation.JSetField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.DeletedObjectException;
import org.jsimpledb.core.ReferencedObjectException;
import org.jsimpledb.index.Index;
import org.jsimpledb.index.Index2;
import org.jsimpledb.test.TestSupport;
import org.jsimpledb.tuple.Tuple2;
import org.jsimpledb.tuple.Tuple3;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LotsOfFieldsTest extends TestSupport {

    @Test
    public void testLotsOfFields() throws Exception {

        final Class<?>[] classes = new Class<?>[] {
            Fields0.class,
            Fields7.class, 
            Fields8.class, 
            Fields15.class, 
            Fields16.class, 
            Fields31.class, 
            Fields32.class, 
            Fields39.class, 
            Fields43.class, 
            Fields63.class, 
            Fields64.class, 
            Fields65.class
        };
        final JSimpleDB jdb = BasicTest.getJSimpleDB(classes);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            for (int i = 0; i < classes.length; i++) {
                final Class<? extends JObject> cl = classes[i].asSubclass(JObject.class);
                final JObject jobj = jtx.create(cl);
                final int numFields = Integer.parseInt(cl.getSimpleName().substring(6));
                this.verifyFields(jobj, numFields, 0);
                this.setFields(jobj, numFields, 3);
                this.verifyFields(jobj, numFields, 3);
                jobj.resetCachedFieldValues();
                this.verifyFields(jobj, numFields, 3);
                jobj.delete();
                jobj.recreate();
                this.verifyFields(jobj, numFields, 0);
                this.setFields(jobj, numFields, 5);
                this.verifyFields(jobj, numFields, 5);
            }

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    private void setFields(JObject jobj, int numFields, int value) throws Exception {
        for (int i = 0; i < numFields; i++)
            jobj.getClass().getMethod("setField" + i, int.class).invoke(jobj, value * i);
    }

    private void verifyFields(JObject jobj, int numFields, int expected) throws Exception {
        for (int i = 0; i < numFields; i++) {
            final int actual = (Integer)jobj.getClass().getMethod("getField" + i).invoke(jobj);
            Assert.assertEquals(actual, expected * i);
        }
    }

    @Test
    public void testCopyCacheReset() throws Exception {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Fields65.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {
            final Fields65 f1 = jtx.create(Fields65.class);
            final Fields65 f2 = jtx.create(Fields65.class);

            f1.setField55(123);
            f2.setField55(456);

            Assert.assertEquals(f1.getField55(), 123);
            Assert.assertEquals(f2.getField55(), 456);

            f1.copyTo(jtx, f2.getObjId(), new CopyState());

            Assert.assertEquals(f1.getField55(), 123);
            Assert.assertEquals(f2.getField55(), 123);

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @JSimpleClass
    public abstract static class Fields0 implements JObject {
    }

    @JSimpleClass
    public abstract static class Fields7 extends Fields0 {
        public abstract int getField0();
        public abstract void setField0(int value);
        public abstract int getField1();
        public abstract void setField1(int value);
        public abstract int getField2();
        public abstract void setField2(int value);
        public abstract int getField3();
        public abstract void setField3(int value);
        public abstract int getField4();
        public abstract void setField4(int value);
        public abstract int getField5();
        public abstract void setField5(int value);
        public abstract int getField6();
        public abstract void setField6(int value);
    }

    @JSimpleClass
    public abstract static class Fields8 extends Fields7 {
        public abstract int getField7();
        public abstract void setField7(int value);
    }

    @JSimpleClass
    public abstract static class Fields15 extends Fields8 {
        public abstract int getField8();
        public abstract void setField8(int value);
        public abstract int getField9();
        public abstract void setField9(int value);
        public abstract int getField10();
        public abstract void setField10(int value);
        public abstract int getField11();
        public abstract void setField11(int value);
        public abstract int getField12();
        public abstract void setField12(int value);
        public abstract int getField13();
        public abstract void setField13(int value);
        public abstract int getField14();
        public abstract void setField14(int value);
    }

    @JSimpleClass
    public abstract static class Fields16 extends Fields15 {
        public abstract int getField15();
        public abstract void setField15(int value);
    }

    @JSimpleClass
    public abstract static class Fields31 extends Fields16 {
        public abstract int getField16();
        public abstract void setField16(int value);
        public abstract int getField17();
        public abstract void setField17(int value);
        public abstract int getField18();
        public abstract void setField18(int value);
        public abstract int getField19();
        public abstract void setField19(int value);
        public abstract int getField20();
        public abstract void setField20(int value);
        public abstract int getField21();
        public abstract void setField21(int value);
        public abstract int getField22();
        public abstract void setField22(int value);
        public abstract int getField23();
        public abstract void setField23(int value);
        public abstract int getField24();
        public abstract void setField24(int value);
        public abstract int getField25();
        public abstract void setField25(int value);
        public abstract int getField26();
        public abstract void setField26(int value);
        public abstract int getField27();
        public abstract void setField27(int value);
        public abstract int getField28();
        public abstract void setField28(int value);
        public abstract int getField29();
        public abstract void setField29(int value);
        public abstract int getField30();
        public abstract void setField30(int value);
    }

    @JSimpleClass
    public abstract static class Fields32 extends Fields31 {
        public abstract int getField31();
        public abstract void setField31(int value);
    }

    @JSimpleClass
    public abstract static class Fields39 extends Fields32 {
        public abstract int getField32();
        public abstract void setField32(int value);
        public abstract int getField33();
        public abstract void setField33(int value);
        public abstract int getField34();
        public abstract void setField34(int value);
        public abstract int getField35();
        public abstract void setField35(int value);
        public abstract int getField36();
        public abstract void setField36(int value);
        public abstract int getField37();
        public abstract void setField37(int value);
        public abstract int getField38();
        public abstract void setField38(int value);
    }

    @JSimpleClass
    public abstract static class Fields43 extends Fields39 {
        public abstract int getField39();
        public abstract void setField39(int value);
        public abstract int getField40();
        public abstract void setField40(int value);
        public abstract int getField41();
        public abstract void setField41(int value);
        public abstract int getField42();
        public abstract void setField42(int value);
    }

    @JSimpleClass
    public abstract static class Fields63 extends Fields43 {
        public abstract int getField43();
        public abstract void setField43(int value);
        public abstract int getField44();
        public abstract void setField44(int value);
        public abstract int getField45();
        public abstract void setField45(int value);
        public abstract int getField46();
        public abstract void setField46(int value);
        public abstract int getField47();
        public abstract void setField47(int value);
        public abstract int getField48();
        public abstract void setField48(int value);
        public abstract int getField49();
        public abstract void setField49(int value);
        public abstract int getField50();
        public abstract void setField50(int value);
        public abstract int getField51();
        public abstract void setField51(int value);
        public abstract int getField52();
        public abstract void setField52(int value);
        public abstract int getField53();
        public abstract void setField53(int value);
        public abstract int getField54();
        public abstract void setField54(int value);
        public abstract int getField55();
        public abstract void setField55(int value);
        public abstract int getField56();
        public abstract void setField56(int value);
        public abstract int getField57();
        public abstract void setField57(int value);
        public abstract int getField58();
        public abstract void setField58(int value);
        public abstract int getField59();
        public abstract void setField59(int value);
        public abstract int getField60();
        public abstract void setField60(int value);
        public abstract int getField61();
        public abstract void setField61(int value);
        public abstract int getField62();
        public abstract void setField62(int value);
    }

    @JSimpleClass
    public abstract static class Fields64 extends Fields63 {
        public abstract int getField63();
        public abstract void setField63(int value);
    }

    @JSimpleClass
    public abstract static class Fields65 extends Fields64 {
        public abstract int getField64();
        public abstract void setField64(int value);
    }
}
