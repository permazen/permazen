
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenType;

import org.testng.annotations.Test;

public class SingletonTest extends MainTestSupport {

    @Test
    public void testSingleton() throws Exception {

        Permazen pdb = BasicTest.newPermazen(Singleton.class);
        PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);

        PermazenTransaction.setCurrent(ptx);
        try {

        // One is enough

            final Singleton obj1 = ptx.getSingleton(Singleton.class);

            ptx.validate();

            assert ptx.getSingleton(Singleton.class) == obj1;

        // Two is too many

            final Singleton obj2 = ptx.create(Singleton.class);

            try {
                ptx.validate();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        // Three's a crowd

            final Singleton obj3 = ptx.create(Singleton.class);

            try {
                ptx.validate();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        // Delete 1 - still too many

            obj1.delete();

            obj2.revalidate();
            obj3.revalidate();

            try {
                ptx.validate();
                assert false : "" + ptx.getAll(Singleton.class);
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

        // Delete 3 - now there's only one

            obj3.delete();

            obj2.revalidate();

            ptx.validate();

            assert ptx.getSingleton(Singleton.class) == obj2;

        // Done

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }

    // Now turn off validation

        ptx = pdb.createTransaction(ValidationMode.DISABLED);

        PermazenTransaction.setCurrent(ptx);
        try {

        // Check

            assert ptx.getAll(Singleton.class).size() == 1;

        // Create a duplicate

            final Singleton obj4 = ptx.create(Singleton.class);

            assert ptx.getAll(Singleton.class).size() == 2;

        // Done

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType(singleton = true)
    public abstract static class Singleton implements PermazenObject {

        public abstract String getName();
        public abstract void setName(String name);
    }
}
