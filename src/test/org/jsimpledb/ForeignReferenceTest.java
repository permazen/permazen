
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.annotation.JSimpleClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ForeignReferenceTest extends TestSupport {

    @Test(dataProvider = "cases")
    public void testWrongTransaction(Assigner assigner) {

        final JSimpleDB jdb = BasicTest.getJSimpleDB(Person.class);

        final Person p1;

        // Tx #1
        JTransaction tx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {
            p1 = (Person)tx.create(Person.class).copyOut();
        } finally {
            tx.commit();
            JTransaction.setCurrent(null);
        }

        // Tx #2
        tx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {
            final Person p2 = tx.create(Person.class);

            // Try with foreign references disabled
            try {
                assigner.assign(p2, p1);
                assert false : "allowed setting foreign reference";
            } catch (IllegalArgumentException e) {
                // expected
            }

            // Try with foreign references enabled
            tx.setAllowForeignReferences(true);
            try {
                assigner.assign(p2, p1);
            } catch (IllegalArgumentException e) {
                assert false : "disallowed setting foreign reference";
            }
        } finally {
            tx.commit();
            JTransaction.setCurrent(null);
        }
    }

    @DataProvider(name = "cases")
    public Assigner[][] genCases() {
        return new Assigner[][] {
        /*
            { (p1, p2) -> p1.setFriend(p2) },
            { (p1, p2) -> p1.getList().add(p2) },
            { (p1, p2) -> p1.getSet().add(p2) },
            { (p1, p2) -> p1.getMap1().put(123, p2) },
            { (p1, p2) -> p1.getMap2().put(p2, 123) },
        */
          {
            new Assigner() {
                @Override
                public void assign(Person p1, Person p2) {
                    p1.setFriend(p2);
                }
            }
          },
          {
            new Assigner() {
                @Override
                public void assign(Person p1, Person p2) {
                    p1.getList().add(p2);
                }
            }
          },
          {
            new Assigner() {
                @Override
                public void assign(Person p1, Person p2) {
                    p1.getSet().add(p2);
                }
            }
          },
          {
            new Assigner() {
                @Override
                public void assign(Person p1, Person p2) {
                    p1.getMap1().put(123, p2);
                }
            }
          },
          {
            new Assigner() {
                @Override
                public void assign(Person p1, Person p2) {
                    p1.getMap2().put(p2, 123);
                }
            }
          },
        };
    }

    //@FunctionalInterface
    private interface Assigner {
        void assign(Person p1, Person p2);
    }

// Model Classes

    @JSimpleClass
    public abstract static class Person implements JObject {

        public abstract Person getFriend();
        public abstract void setFriend(Person person);

        public abstract List<Person> getList();
        public abstract NavigableSet<Person> getSet();
        public abstract NavigableMap<Integer, Person> getMap1();
        public abstract NavigableMap<Person, Integer> getMap2();
    }
}

