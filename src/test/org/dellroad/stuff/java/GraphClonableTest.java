
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

import org.dellroad.stuff.TestSupport;
import org.testng.annotations.Test;

public class GraphClonableTest extends TestSupport {

    @Test
    public void test1() throws Exception {

        // Setup graph
        final People people1 = this.setup1();

        // Clone graph
        final GraphCloneRegistry registry = new GraphCloneRegistry();
        final People people2 = new People(
          registry.getGraphClone(people1.father),
          registry.getGraphClone(people1.mother),
          registry.getGraphClone(people1.alice),
          registry.getGraphClone(people1.bob)
        );

        // Check objects are different
        IdentityHashMap<Object, Void> map = new IdentityHashMap<Object, Void>();
        map.put(people1.father, null);
        map.put(people1.mother, null);
        map.put(people1.alice, null);
        map.put(people1.bob, null);
        map.put(people2.father, null);
        map.put(people2.mother, null);
        map.put(people2.alice, null);
        map.put(people2.bob, null);
        assert map.size() == 8;

        // Check equality
        assert people1.father.sameSimpleFields(people2.father);
        assert people1.mother.sameSimpleFields(people2.mother);
        assert people1.alice.sameSimpleFields(people2.alice);
        assert people1.bob.sameSimpleFields(people2.bob);

        // Check references
        assert people2.mother.getSpouse() == people2.father;
        assert people2.father.getSpouse() == people2.mother;
        assert people2.father.getSpouse().getSpouse() == people2.father;
        assert people2.mother.getFriends().size() == 2;
        assert people2.mother.getFriends().contains(people2.mother);
        assert people2.mother.getFriends().contains(people2.alice);
    }

    @Test
    public void test2() throws Exception {
        try {
            new GraphCloneRegistry().getGraphClone(new BadClass1());
            assert false;
        } catch (IllegalStateException e) {
            // ok
        }
    }

    @Test
    public void test3() throws Exception {
        BadClass1 x = new BadClass1();
        BadClass1 y = new BadClass1();
        x.other = y;
        y.other = x;
        try {
            new GraphCloneRegistry().getGraphClone(x);
            assert false;
        } catch (IllegalStateException e) {
            // ok
        }
    }

    @Test
    public void test4() throws Exception {
        try {
            new GraphCloneRegistry().getGraphClone(new BadClass2());
            assert false;
        } catch (IllegalStateException e) {
            // ok
        }
    }

    @Test
    public void test5() throws Exception {
        try {
            new GraphCloneRegistry().getGraphClone(new BadClass3());
            assert false;
        } catch (IllegalStateException e) {
            // ok
        }
    }

    private People setup1() {

        final Person father = new Person();
        final Person mother = new Person();
        final Person alice = new Person();
        final Person bob = new Person();

        father.setAge(46);
        father.setName("Father");
        father.getNicknames().add("Daddy");
        father.getNicknames().add("Pops");

        mother.setAge(43);
        mother.setName("Mother");
        mother.getNicknames().add("Mommy");
        mother.getNicknames().add("Ma");

        alice.setAge(41);
        alice.setName("Alice");

        bob.setAge(42);
        bob.setName("Bob");
        mother.getNicknames().add("Bobby");
        mother.getNicknames().add(null);

        father.setSpouse(mother);
        father.getFriends().add(alice);
        father.getFriends().add(bob);

        mother.setSpouse(father);
        mother.getFriends().add(mother);
        mother.getFriends().add(alice);

        alice.getFriends().add(father);
        alice.getFriends().add(bob);

        bob.getFriends().add(alice);

        return new People(father, mother, alice, bob);
    }

    public class People {
        // CHECKSTYLE OFF: VisibilityModifierCheck
        public final Person father;
        public final Person mother;
        public final Person alice;
        public final Person bob;
        // CHECKSTYLE ON: VisibilityModifierCheck
        public People(Person father, Person mother, Person alice, Person bob) {
            this.father = father;
            this.mother = mother;
            this.alice = alice;
            this.bob = bob;
        }
    }

    public class Person implements Cloneable, GraphCloneable {

        // Regular fields
        private int age;
        private String name;
        private List<String> nicknames = new ArrayList<String>();

        // GraphCloneable fields - values may be null and/or even refer back to me
        private Person spouse;
        private List<Person> friends = new ArrayList<Person>();

        public int getAge() {
            return this.age;
        }
        public void setAge(int age) {
            this.age = age;
        }

        public String getName() {
            return this.name;
        }
        public void setName(String name) {
            this.name = name;
        }

        public List<String> getNicknames() {
            return this.nicknames;
        }
        public void setNicknames(List<String> nicknames) {
            this.nicknames = nicknames;
        }

        public Person getSpouse() {
            return this.spouse;
        }
        public void setSpouse(Person spouse) {
            this.spouse = spouse;
        }

        public List<Person> getFriends() {
            return this.friends;
        }
        public void setFriends(List<Person> friends) {
            this.friends = friends;
        }

        @Override
        public void createGraphClone(GraphCloneRegistry registry) throws CloneNotSupportedException {
            final Person clone = (Person)super.clone();
            registry.setGraphClone(clone);
            clone.nicknames = new ArrayList<String>(this.nicknames);
            clone.spouse = registry.getGraphClone(this.spouse);
            clone.friends = new ArrayList<Person>(this.friends.size());
            for (Person friend : this.friends)
                clone.friends.add(registry.getGraphClone(friend));
        }

        public boolean sameSimpleFields(Person that) {
            return this.age == that.age
            && this.name.equals(that.name)
            && this.nicknames.equals(that.nicknames)
            && (this.spouse != null) == (that.spouse != null)
            && this.friends.size() == that.friends.size();
        }
    }

    public class BadClass1 implements GraphCloneable {

        protected BadClass1 other;

        @Override
        public void createGraphClone(GraphCloneRegistry registry) throws CloneNotSupportedException {
            final BadClass1 clone = new BadClass1();
            clone.other = registry.getGraphClone(this.other);   // should throw exception
        }
    }

    public class BadClass2 implements GraphCloneable {

        private BadClass2 other;

        @Override
        public void createGraphClone(GraphCloneRegistry registry) throws CloneNotSupportedException {
            final BadClass2 clone = new BadClass2();
            registry.setGraphClone(clone);
            registry.setGraphClone(clone);                      // should throw exception
        }
    }

    public class BadClass3 implements GraphCloneable {

        private BadClass3 other;

        @Override
        public void createGraphClone(GraphCloneRegistry registry) throws CloneNotSupportedException {
            // should cause exception after returning
        }
    }

}

