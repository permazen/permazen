
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.annotation.ValueRange;
import io.permazen.annotation.Values;
import io.permazen.core.ObjId;

import java.util.Date;

import org.testng.annotations.Test;

public class UniqueConstraintTest extends MainTestSupport {

    @Test
    public void testUniqueConstraint() throws Exception {

        Permazen pdb = BasicTest.newPermazen(
          UniqueName.class,
          UniqueValue.class,
          UniqueValueRange.class,
          UniqueNull.class,
          UniqueEnum.class);
        PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);

        PermazenTransaction.setCurrent(ptx);
        try {

        // Verify default name value (null) must be unique

            final UniqueName foo1 = ptx.create(UniqueName.class);
            final UniqueName foo2 = ptx.create(UniqueName.class);
            final UniqueName foo3 = ptx.create(UniqueName.class);

            try {
                ptx.validate();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        // Now make them distinct

            foo1.setName("foo1");
            foo2.setName("foo2");
            foo3.setName("foo3");

            ptx.validate();

        // Now make them the same

            foo2.setName("foo3");

            try {
                ptx.validate();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        // Now make them have the excluded value

            foo1.setName("frob");
            foo2.setName("frob");
            foo3.setName("frob");

            ptx.validate();

        // Delete them

            foo1.setName("same");
            foo2.setName("same");
            foo3.setName("same");
            foo1.delete();
            foo2.delete();
            foo3.delete();

            ptx.validate();

        // Now test uniqueExcludes()

            final UniqueValue bar1 = ptx.create(UniqueValue.class);
            final UniqueValue bar2 = ptx.create(UniqueValue.class);
            final UniqueValue bar3 = ptx.create(UniqueValue.class);
            final UniqueValue bar4 = ptx.create(UniqueValue.class);
            final UniqueValue bar5 = ptx.create(UniqueValue.class);
            final UniqueValue bar6 = ptx.create(UniqueValue.class);
            final UniqueValue bar7 = ptx.create(UniqueValue.class);
            final UniqueValue bar8 = ptx.create(UniqueValue.class);

            bar1.setValue(Float.NaN);
            bar2.setValue(Float.NaN);
            bar3.setValue(123.45f);
            bar4.setValue(123.45f);
            bar5.setValue(Float.POSITIVE_INFINITY);
            bar6.setValue(Float.POSITIVE_INFINITY);
            bar7.setValue(Float.NaN);
            bar8.setValue(Float.NaN);

            ptx.validate();

        // More test

            bar1.setValue(15.3f);
            bar2.setValue(15.3f);

            try {
                ptx.validate();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        // More test

            bar2.setValue(15.4f);

            ptx.validate();

        // Test uniqueExcludes() with ranges

            final UniqueValueRange uvr = ptx.create(UniqueValueRange.class);

            final UniqueValueRange uvr10 = ptx.create(UniqueValueRange.class);
            final UniqueValueRange uvr11 = ptx.create(UniqueValueRange.class);
            final UniqueValueRange uvr19 = ptx.create(UniqueValueRange.class);
            final UniqueValueRange uvr20 = ptx.create(UniqueValueRange.class);
            final UniqueValueRange uvr30 = ptx.create(UniqueValueRange.class);
            final UniqueValueRange uvr31 = ptx.create(UniqueValueRange.class);
            final UniqueValueRange uvr39 = ptx.create(UniqueValueRange.class);
            final UniqueValueRange uvr40 = ptx.create(UniqueValueRange.class);

            uvr10.setValue(10);
            uvr11.setValue(11);
            uvr19.setValue(19);
            uvr20.setValue(20);
            uvr30.setValue(30);
            uvr31.setValue(31);
            uvr39.setValue(39);
            uvr40.setValue(40);

            ptx.validate();

            for (int v : new int[] { 5, 9, 10, 11, 15, 19, 21, 25, 29, 31, 35, 40, 41, 45 }) {
                uvr.setValue(v);
                ptx.validate();
            }

            for (int v : new int[] { 20, 30 }) {
                uvr.setValue(v);
                try {
                    ptx.validate();
                    assert false;
                } catch (ValidationException e) {
                    this.log.debug("got expected {}", e.toString());
                }
            }

        // Check uniqueExcludes() of null

            final UniqueNull null1 = ptx.create(UniqueNull.class);
            final UniqueNull null2 = ptx.create(UniqueNull.class);
            final UniqueNull null3 = ptx.create(UniqueNull.class);

            ptx.validate();

        // Check UniqueEnum

            final UniqueEnum enum1 = ptx.create(UniqueEnum.class);
            final UniqueEnum enum2 = ptx.create(UniqueEnum.class);
            final UniqueEnum enum3 = ptx.create(UniqueEnum.class);

            enum1.setColor(Color.RED);
            enum2.setColor(Color.GREEN);
            enum3.setColor(Color.BLUE);

            ptx.validate();

            enum2.setColor(Color.RED);

            try {
                ptx.validate();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

            enum2.setColor(Color.BLUE);

            ptx.validate();

        // Done

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testDuplicateAfterCopyClone() throws Exception {

        Permazen pdb = BasicTest.newPermazen(UniqueName.class);
        PermazenTransaction ptx;

        ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final UniqueName u1 = ptx.create(UniqueName.class);
            u1.setName("Jeffrey");

            final CopyState copyState = new CopyState(false);
            copyState.getObjectIdMap().put(u1.getObjId(), null);                // map to new object id
            final UniqueName u2 = (UniqueName)u1.copyTo(ptx, -1, copyState);

            assert u1.exists();
            assert u2.exists();

            assert !u1.getObjId().equals(u2.getObjId());

            assert u1.getName().equals("Jeffrey");
            assert u2.getName().equals("Jeffrey");

            try {
                ptx.commit();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testDuplicateAfterCopyIn() throws Exception {

        Permazen pdb = BasicTest.newPermazen(UniqueName.class);
        PermazenTransaction ptx;

        ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final UniqueName u1 = ptx.create(UniqueName.class);
            u1.setName("Jeffrey");

            final PermazenTransaction stx = ptx.getDetachedTransaction();
            final UniqueName u2s = stx.create(UniqueName.class);
            u2s.setName("Jeffrey");

            final CopyState copyState = new CopyState(this.random.nextBoolean());

            final UniqueName u2 = (UniqueName)u2s.copyTo(ptx, -1, copyState);

            assert u1.exists();
            assert u2s.exists();
            assert u2.exists();

            assert !u1.getObjId().equals(u2.getObjId());
            assert u2.getObjId().equals(u2s.getObjId());

            assert u1.getName().equals("Jeffrey");
            assert u2s.getName().equals("Jeffrey");
            assert u2.getName().equals("Jeffrey");

            try {
                ptx.commit();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testDuplicateAfterCopyOnTopOf() throws Exception {

        Permazen pdb = BasicTest.newPermazen(UniqueName.class);
        PermazenTransaction ptx;

        final ObjId id1;
        final ObjId id2;

        // Create two objects with different names
        ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final UniqueName u1 = ptx.create(UniqueName.class);
            final UniqueName u2 = ptx.create(UniqueName.class);
            u1.setName("Jeffrey");
            u2.setName("Flapjack");

            assert u1.exists();
            assert u2.exists();

            id1 = u1.getObjId();
            id2 = u2.getObjId();

            assert !id1.equals(id2);

            assert u1.getName().equals("Jeffrey");
            assert u2.getName().equals("Flapjack");

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }

        // Copy on top of the second object, which already exists, but where the copy has the same name
        // Configure the CopyState to suppress notifications. The bug was that this also inadvertently
        // suppressed the notification that was supposed to trigger validation.
        ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final UniqueName u2s = ptx.getDetachedTransaction().get(id2, UniqueName.class);
            u2s.recreate();
            u2s.setName("Jeffrey");

            final CopyState copyState = new CopyState(true);
            u2s.copyTo(ptx, -1, copyState);

            final UniqueName u1 = ptx.get(id1, UniqueName.class);
            final UniqueName u2 = ptx.get(id2, UniqueName.class);

            assert u1.exists();
            assert u2.exists();

            assert !id1.equals(id2);

            assert u1.getName().equals("Jeffrey");
            assert u2.getName().equals("Jeffrey");

            try {
                ptx.commit();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testSameStorageIdUnique() throws Exception {

        Permazen pdb = BasicTest.newPermazen(UniqueName.class, UniqueName2.class);
        PermazenTransaction ptx;

        // test 1
        ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final UniqueName u1a = ptx.create(UniqueName.class);
            final UniqueName u1b = ptx.create(UniqueName.class);
            u1a.setName("joe");
            u1b.setName("joe");

            try {
                ptx.commit();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        } finally {
            PermazenTransaction.setCurrent(null);
        }

        // test 2
        ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final UniqueName2 u2a = ptx.create(UniqueName2.class);
            final UniqueName2 u2b = ptx.create(UniqueName2.class);
            u2a.setName("joe");
            u2b.setName("joe");

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }

        // test 3
        ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final UniqueName u1 = ptx.create(UniqueName.class);
            final UniqueName2 u2 = ptx.create(UniqueName2.class);
            u1.setName("fred");
            u2.setName("fred");

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testSameStorageIdInherited() throws Exception {

        Permazen pdb = BasicTest.newPermazen(UniqueName3.class, UniqueName4.class);
        PermazenTransaction ptx;

        ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final UniqueName3 u3a = ptx.create(UniqueName3.class);
            final UniqueName3 u3b = ptx.create(UniqueName3.class);
            u3a.setName("joe");
            u3b.setName("joe");

            try {
                ptx.commit();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        } finally {
            PermazenTransaction.setCurrent(null);
        }

        ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final UniqueName4 u4a = ptx.create(UniqueName4.class);
            final UniqueName4 u4b = ptx.create(UniqueName4.class);
            u4a.setName("joe");
            u4b.setName("joe");

            try {
                ptx.commit();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        } finally {
            PermazenTransaction.setCurrent(null);
        }

        ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final UniqueName3 u3 = ptx.create(UniqueName3.class);
            final UniqueName4 u4 = ptx.create(UniqueName4.class);
            u3.setName("fred");
            u4.setName("fred");

            try {
                ptx.commit();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testInvalid1() throws Exception {
        try {
            BasicTest.newPermazen(UniqueInvalid1.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            this.log.info("got expected {}", e.toString());
        }
    }

    @Test
    public void testInvalid2() throws Exception {
        try {
            BasicTest.newPermazen(UniqueInvalid2.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            this.log.info("got expected {}", e.toString());
        }
    }

    @Test
    public void testInvalid3() throws Exception {
        try {
            BasicTest.newPermazen(UniqueInvalid3.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            this.log.info("got expected {}", e.toString());
        }
    }

// Model Classes

    @PermazenType
    public abstract static class UniqueName implements PermazenObject {

        @PermazenField(indexed = true, uniqueExcludes = @Values("frob"), unique = true)
        public abstract String getName();
        public abstract void setName(String name);
    }

    @PermazenType
    public abstract static class UniqueName2 implements PermazenObject {

        @PermazenField(indexed = true, unique = false)
        public abstract String getName();
        public abstract void setName(String name);
    }

    @PermazenType
    public abstract static class UniqueValue implements PermazenObject {

        @PermazenField(indexed = true, unique = true, uniqueExcludes = @Values({ "NaN", "123.45", "Infinity" }))
        public abstract float getValue();
        public abstract void setValue(float value);
    }

    @PermazenType
    public abstract static class UniqueValueRange implements PermazenObject {

        @PermazenField(
          indexed = true,
          unique = true,
          uniqueExcludes = @Values(ranges = {
            @ValueRange(min = "10", max = "20"),    // defaults: inclusiveMin = true, exclusiveMax = false
            @ValueRange(min = "30", max = "40", inclusiveMin = false, inclusiveMax = true)
          }))
        public abstract int getValue();
        public abstract void setValue(int value);
    }

    @PermazenType
    public abstract static class UniqueNull implements PermazenObject {

        @PermazenField(indexed = true, unique = true, uniqueExcludes = @Values(nulls = true))
        public abstract Date getDate();
        public abstract void setDate(Date date);
    }

    @PermazenType
    public abstract static class UniqueEnum implements PermazenObject {

        @PermazenField(indexed = true, unique = true, uniqueExcludes = @Values("BLUE"))
        public abstract Color getColor();
        public abstract void setColor(Color color);
    }

    @PermazenType
    public abstract static class UniqueInvalid1 implements PermazenObject {

        @PermazenField(indexed = true, unique = true, uniqueExcludes = @Values(nulls = true))
        public abstract float getValue();
        public abstract void setValue(float value);
    }

    @PermazenType
    public abstract static class UniqueInvalid2 implements PermazenObject {

        @PermazenField(indexed = true, unique = true, uniqueExcludes = @Values(nonNulls = true))
        public abstract float getValue();
        public abstract void setValue(float value);
    }

    @PermazenType
    public abstract static class UniqueInvalid3 implements PermazenObject {

        @PermazenField(indexed = true, unique = true, uniqueExcludes = @Values(nonNulls = true, value = "12.34"))
        public abstract float getValue();
        public abstract void setValue(float value);
    }

    public enum Color {
        RED,
        GREEN,
        BLUE,
        YELLOW;
    }

// Inherited unique constraint

    public interface HasName {

        @PermazenField(indexed = true, unique = true)
        String getName();
        void setName(String name);
    }

    @PermazenType
    public abstract static class UniqueName3 implements PermazenObject, HasName {
    }

    @PermazenType
    public abstract static class UniqueName4 implements PermazenObject, HasName {

        @Override
        public abstract String getName();
        @Override
        public abstract void setName(String name);
    }
}
