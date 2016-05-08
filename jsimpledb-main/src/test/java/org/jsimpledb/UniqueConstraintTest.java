
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.util.Date;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.test.TestSupport;
import org.testng.annotations.Test;

public class UniqueConstraintTest extends TestSupport {

    @Test
    public void testUniqueConstraint() throws Exception {

        JSimpleDB jdb = BasicTest.getJSimpleDB(UniqueName.class, UniqueValue.class, UniqueNull.class, UniqueEnum.class);
        JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);

        JTransaction.setCurrent(jtx);
        try {

        // Verify default name value (null) must be unique

            final UniqueName foo1 = jtx.create(UniqueName.class);
            final UniqueName foo2 = jtx.create(UniqueName.class);
            final UniqueName foo3 = jtx.create(UniqueName.class);

            try {
                jtx.validate();
                assert false;
            } catch (ValidationException e) {
                // expected
            }

        // Now make them distinct

            foo1.setName("foo1");
            foo2.setName("foo2");
            foo3.setName("foo3");

            jtx.validate();

        // Now make them the same

            foo2.setName("foo3");

            try {
                jtx.validate();
                assert false;
            } catch (ValidationException e) {
                // expected
            }

        // Now make them have the excluded value

            foo1.setName("frob");
            foo2.setName("frob");
            foo3.setName("frob");

            jtx.validate();

        // Delete them

            foo1.setName("same");
            foo2.setName("same");
            foo3.setName("same");
            foo1.delete();
            foo2.delete();
            foo3.delete();

            jtx.validate();

        // Now test uniqueExclude()

            final UniqueValue bar1 = jtx.create(UniqueValue.class);
            final UniqueValue bar2 = jtx.create(UniqueValue.class);
            final UniqueValue bar3 = jtx.create(UniqueValue.class);
            final UniqueValue bar4 = jtx.create(UniqueValue.class);
            final UniqueValue bar5 = jtx.create(UniqueValue.class);
            final UniqueValue bar6 = jtx.create(UniqueValue.class);
            final UniqueValue bar7 = jtx.create(UniqueValue.class);
            final UniqueValue bar8 = jtx.create(UniqueValue.class);

            bar1.setValue(Float.NaN);
            bar2.setValue(Float.NaN);
            bar3.setValue(123.45f);
            bar4.setValue(123.45f);
            bar5.setValue(Float.POSITIVE_INFINITY);
            bar6.setValue(Float.POSITIVE_INFINITY);
            bar7.setValue(Float.NaN);
            bar8.setValue(Float.NaN);

            jtx.validate();

        // More test

            bar1.setValue(15.3f);
            bar2.setValue(15.3f);

            try {
                jtx.validate();
                assert false;
            } catch (ValidationException e) {
                // expected
            }

        // More test

            bar2.setValue(15.4f);

            jtx.validate();

        // Check uniqueExcludeNull

            final UniqueNull null1 = jtx.create(UniqueNull.class);
            final UniqueNull null2 = jtx.create(UniqueNull.class);
            final UniqueNull null3 = jtx.create(UniqueNull.class);

            jtx.validate();

        // Check UniqueEnum

            final UniqueEnum enum1 = jtx.create(UniqueEnum.class);
            final UniqueEnum enum2 = jtx.create(UniqueEnum.class);
            final UniqueEnum enum3 = jtx.create(UniqueEnum.class);

            enum1.setColor(Color.RED);
            enum2.setColor(Color.GREEN);
            enum3.setColor(Color.BLUE);

            jtx.validate();

            enum2.setColor(Color.RED);

            try {
                jtx.validate();
                assert false;
            } catch (ValidationException e) {
                // expected
            }

            enum2.setColor(Color.BLUE);

            jtx.validate();

        // Done

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }

    }

// Model Classes

    @JSimpleClass
    public abstract static class UniqueName implements JObject {

        @JField(indexed = true, uniqueExclude = "frob", unique = true)
        public abstract String getName();
        public abstract void setName(String name);
    }

    @JSimpleClass
    public abstract static class UniqueValue implements JObject {

        @JField(indexed = true, unique = true, uniqueExclude = { "NaN", "123.45", "Infinity" })
        public abstract float getValue();
        public abstract void setValue(float value);
    }

    @JSimpleClass
    public abstract static class UniqueNull implements JObject {

        @JField(indexed = true, unique = true, uniqueExcludeNull = true)
        public abstract Date getDate();
        public abstract void setDate(Date date);
    }

    @JSimpleClass
    public abstract static class UniqueEnum implements JObject {

        @JField(indexed = true, unique = true, uniqueExclude = "BLUE")
        public abstract Color getColor();
        public abstract void setColor(Color color);
    }

    public enum Color {
        RED,
        GREEN,
        BLUE,
        YELLOW;
    }
}

