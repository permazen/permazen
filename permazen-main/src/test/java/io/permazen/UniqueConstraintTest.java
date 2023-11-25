
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.ObjId;
import io.permazen.test.TestSupport;

import java.util.Date;

import org.testng.annotations.Test;

public class UniqueConstraintTest extends TestSupport {

    @Test
    public void testUniqueConstraint() throws Exception {

        Permazen jdb = BasicTest.getPermazen(UniqueName.class, UniqueValue.class, UniqueNull.class, UniqueEnum.class);
        JTransaction jtx = jdb.createTransaction(ValidationMode.AUTOMATIC);

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
                this.log.debug("got expected {}", e.toString());
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
                this.log.debug("got expected {}", e.toString());
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
                this.log.debug("got expected {}", e.toString());
            }

        // More test

            bar2.setValue(15.4f);

            jtx.validate();

        // Check uniqueExclude of null

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
                this.log.debug("got expected {}", e.toString());
            }

            enum2.setColor(Color.BLUE);

            jtx.validate();

        // Done

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testDuplicateAfterCopyClone() throws Exception {

        Permazen jdb = BasicTest.getPermazen(UniqueName.class);
        JTransaction jtx;

        jtx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final UniqueName u1 = jtx.create(UniqueName.class);
            u1.setName("Jeffrey");
            final UniqueName u2 = (UniqueName)u1.cascadeCopyTo(jtx, null, -1, true);

            assert u1.exists();
            assert u2.exists();

            assert !u1.getObjId().equals(u2.getObjId());

            assert u1.getName().equals("Jeffrey");
            assert u2.getName().equals("Jeffrey");

            try {
                jtx.commit();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testDuplicateAfterCopyIn() throws Exception {

        Permazen jdb = BasicTest.getPermazen(UniqueName.class);
        JTransaction jtx;

        jtx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final UniqueName u1 = jtx.create(UniqueName.class);
            u1.setName("Jeffrey");

            final JTransaction stx = jtx.getDetachedTransaction();
            final UniqueName u2s = stx.create(UniqueName.class);
            u2s.setName("Jeffrey");

            final CopyState copyState = new CopyState();
            copyState.setSuppressNotifications(this.random.nextBoolean());

            final UniqueName u2 = (UniqueName)u2s.copyTo(jtx, copyState);

            assert u1.exists();
            assert u2s.exists();
            assert u2.exists();

            assert !u1.getObjId().equals(u2.getObjId());
            assert u2.getObjId().equals(u2s.getObjId());

            assert u1.getName().equals("Jeffrey");
            assert u2s.getName().equals("Jeffrey");
            assert u2.getName().equals("Jeffrey");

            try {
                jtx.commit();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testDuplicateAfterCopyOnTopOf() throws Exception {

        Permazen jdb = BasicTest.getPermazen(UniqueName.class);
        JTransaction jtx;

        final ObjId id1;
        final ObjId id2;

        // Create two objects with different names
        jtx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final UniqueName u1 = jtx.create(UniqueName.class);
            final UniqueName u2 = jtx.create(UniqueName.class);
            u1.setName("Jeffrey");
            u2.setName("Flapjack");

            assert u1.exists();
            assert u2.exists();

            id1 = u1.getObjId();
            id2 = u2.getObjId();

            assert !id1.equals(id2);

            assert u1.getName().equals("Jeffrey");
            assert u2.getName().equals("Flapjack");

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }

        // Copy on top of the second object, which already exists, but where the copy has the same name
        // Configure the CopyState to suppress notifications. The bug was that this also inadvertently
        // suppressed the notification that was supposed to trigger validation.
        jtx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final UniqueName u2s = jtx.getDetachedTransaction().get(id2, UniqueName.class);
            u2s.recreate();
            u2s.setName("Jeffrey");

            final CopyState copyState = new CopyState();
            copyState.setSuppressNotifications(true);
            u2s.copyTo(jtx, copyState);

            final UniqueName u1 = jtx.get(id1, UniqueName.class);
            final UniqueName u2 = jtx.get(id2, UniqueName.class);

            assert u1.exists();
            assert u2.exists();

            assert !id1.equals(id2);

            assert u1.getName().equals("Jeffrey");
            assert u2.getName().equals("Jeffrey");

            try {
                jtx.commit();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testSameStorageIdUnique() throws Exception {

        Permazen jdb = BasicTest.getPermazen(UniqueName.class, UniqueName2.class);
        JTransaction jtx;

        // test 1
        jtx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final UniqueName u1a = jtx.create(UniqueName.class);
            final UniqueName u1b = jtx.create(UniqueName.class);
            u1a.setName("joe");
            u1b.setName("joe");

            try {
                jtx.commit();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        } finally {
            JTransaction.setCurrent(null);
        }

        // test 2
        jtx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final UniqueName2 u2a = jtx.create(UniqueName2.class);
            final UniqueName2 u2b = jtx.create(UniqueName2.class);
            u2a.setName("joe");
            u2b.setName("joe");

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }

        // test 3
        jtx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final UniqueName u1 = jtx.create(UniqueName.class);
            final UniqueName2 u2 = jtx.create(UniqueName2.class);
            u1.setName("fred");
            u2.setName("fred");

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testSameStorageIdInherited() throws Exception {

        Permazen jdb = BasicTest.getPermazen(UniqueName3.class, UniqueName4.class);
        JTransaction jtx;

        jtx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final UniqueName3 u3a = jtx.create(UniqueName3.class);
            final UniqueName3 u3b = jtx.create(UniqueName3.class);
            u3a.setName("joe");
            u3b.setName("joe");

            try {
                jtx.commit();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        } finally {
            JTransaction.setCurrent(null);
        }

        jtx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final UniqueName4 u4a = jtx.create(UniqueName4.class);
            final UniqueName4 u4b = jtx.create(UniqueName4.class);
            u4a.setName("joe");
            u4b.setName("joe");

            try {
                jtx.commit();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        } finally {
            JTransaction.setCurrent(null);
        }

        jtx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final UniqueName3 u3 = jtx.create(UniqueName3.class);
            final UniqueName4 u4 = jtx.create(UniqueName4.class);
            u3.setName("fred");
            u4.setName("fred");

            try {
                jtx.commit();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public abstract static class UniqueName implements JObject {

        @JField(indexed = true, uniqueExclude = "frob", unique = true)
        public abstract String getName();
        public abstract void setName(String name);
    }

    @PermazenType
    public abstract static class UniqueName2 implements JObject {

        @JField(indexed = true, unique = false)
        public abstract String getName();
        public abstract void setName(String name);
    }

    @PermazenType
    public abstract static class UniqueValue implements JObject {

        @JField(indexed = true, unique = true, uniqueExclude = { "NaN", "123.45", "Infinity" })
        public abstract float getValue();
        public abstract void setValue(float value);
    }

    @PermazenType
    public abstract static class UniqueNull implements JObject {

        @JField(indexed = true, unique = true, uniqueExclude = JField.NULL)
        public abstract Date getDate();
        public abstract void setDate(Date date);
    }

    @PermazenType
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

// Inherited unique constraint

    public interface HasName {

        @JField(indexed = true, unique = true)
        String getName();
        void setName(String name);
    }

    @PermazenType
    public abstract static class UniqueName3 implements JObject, HasName {
    }

    @PermazenType
    public abstract static class UniqueName4 implements JObject, HasName {

        @Override
        public abstract String getName();
        @Override
        public abstract void setName(String name);
    }
}
