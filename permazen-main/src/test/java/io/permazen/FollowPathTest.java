
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.FollowPath;
import io.permazen.annotation.PermazenType;
import io.permazen.test.TestSupport;

import java.util.NavigableSet;
import java.util.Optional;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class FollowPathTest extends TestSupport {

    @Test
    public void testFollowPath() {
        final Permazen jdb = BasicTest.getPermazen(Family.class, Car.class, Bike.class, Dad.class, Mom.class, GoodChild.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final Family family = jtx.create(Family.class);

            final Mom mom = jtx.create(Mom.class);
            final Dad dad = jtx.create(Dad.class);

            mom.setFamily(family);
            dad.setFamily(family);

            Assert.assertSame(family.getMom().orElse(null), mom);
            Assert.assertSame(family.getDad().orElse(null), dad);

            Assert.assertSame(mom.getHusband().orElse(null), dad);
            Assert.assertSame(dad.getWife().orElse(null), mom);

            TestSupport.checkSet(family.getMembers(), buildSet(mom, dad));
            TestSupport.checkSet(mom.getAllFamilyMembers(), buildSet(mom, dad));
            TestSupport.checkSet(dad.getAllFamilyMembers(), buildSet(mom, dad));

            final Child child1 = jtx.create(GoodChild.class);
            final Child child2 = jtx.create(GoodChild.class);
            final Child child3 = jtx.create(GoodChild.class);

            child1.setFamily(family);
            child2.setFamily(family);
            child3.setFamily(family);

            TestSupport.checkSet(family.getMembers(), buildSet(mom, dad, child1, child2, child3));
            TestSupport.checkSet(mom.getAllFamilyMembers(), buildSet(mom, dad, child1, child2, child3));
            TestSupport.checkSet(mom.getAllFamilyMembers(), family.getMembers());
            for (Person member : family.getMembers())
                TestSupport.checkSet(member.getAllFamilyMembers(), family.getMembers());

            TestSupport.checkSet(family.getChildren(), buildSet(child1, child2, child3));

            TestSupport.checkSet(child1.getSiblings(), family.getChildren());
            TestSupport.checkSet(child2.getSiblings(), family.getChildren());
            TestSupport.checkSet(child3.getSiblings(), family.getChildren());

            final Car mcar = jtx.create(Car.class);
            final Car dcar = jtx.create(Car.class);

            mom.setVehicle(mcar);
            dad.setVehicle(dcar);

            final Bike bike1 = jtx.create(Bike.class);
            final Bike bike2 = jtx.create(Bike.class);
            final Bike bike3 = jtx.create(Bike.class);

            child1.setVehicle(bike1);
            child2.setVehicle(bike2);
            child3.setVehicle(bike2);

            TestSupport.checkSet(bike1.getAllOwners(), buildSet(child1));
            Assert.assertSame(bike1.getFirstOwner().orElse(null), child1);

            TestSupport.checkSet(bike2.getAllOwners(), buildSet(child2, child3));
            Assert.assertSame(bike2.getFirstOwner().orElse(null),
              child2.getObjId().compareTo(child3.getObjId()) < 0 ? child2 : child3);

            TestSupport.checkSet(bike3.getAllOwners(), buildSet());
            Assert.assertFalse(bike3.getFirstOwner().isPresent());

            Assert.assertSame(child1.getDadsVehicle().orElse(null), dcar);
            Assert.assertSame(child2.getDadsVehicle().orElse(null), dcar);
            Assert.assertSame(child3.getDadsVehicle().orElse(null), dcar);

            Assert.assertSame(child1.getDadsVehicle2().orElse(null), dcar);
            Assert.assertSame(child2.getDadsVehicle2().orElse(null), dcar);
            Assert.assertSame(child3.getDadsVehicle2().orElse(null), dcar);

            TestSupport.checkSet(child1.getSiblingBikes(), buildSet(bike1, bike2));
            TestSupport.checkSet(child2.getSiblingBikes(), buildSet(bike1, bike2));
            TestSupport.checkSet(child3.getSiblingBikes(), buildSet(bike1, bike2));

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test(dataProvider = "badClasses")
    public void testBadChild(Class<?> badClass) {
        try {
            BasicTest.getPermazen(Family.class, Car.class, Bike.class, Dad.class, Mom.class, badClass);
            assert false;
        } catch (IllegalArgumentException e) {
            this.log.info("got expected {}", e.toString());
        }
    }

    @DataProvider(name = "badClasses")
    public Object[][] badClasses() {
        return new Object[][] {
            { BadChild1.class },
            { BadParent1.class },
            { BadChild3.class },
            { BadChild4.class },
            { BadChild5.class },
        };
    }

// Model Classes

    public interface Vehicle extends JObject {

        @FollowPath("<-io.permazen.FollowPathTest$Person.vehicle")
        NavigableSet<Person> getAllOwners();

        @FollowPath("<-io.permazen.FollowPathTest$Person.vehicle")
        Optional<Person> getFirstOwner();
    }

    public interface HasVehicle {
        Vehicle getVehicle();
        void setVehicle(Vehicle x);
    }

    public interface Person extends JObject {
        Family getFamily();
        void setFamily(Family x);

        @FollowPath("->family<-io.permazen.FollowPathTest$Person.family")
        NavigableSet<Person> getAllFamilyMembers();
    }

    @PermazenType
    public abstract static class Family implements JObject {

        @FollowPath("<-io.permazen.FollowPathTest$Person.family")
        public abstract NavigableSet<Person> getMembers();

        @FollowPath("<-Mom.family")
        public abstract Optional<Mom> getMom();

        @FollowPath("<-Dad.family")
        public abstract Optional<Dad> getDad();

        @FollowPath("<-io.permazen.FollowPathTest$Child.family")
        public abstract NavigableSet<Child> getChildren();
    }

    @PermazenType
    public abstract static class Car implements Vehicle {
    }

    @PermazenType
    public abstract static class Bike implements Vehicle {
    }

    public interface Parent extends Person, HasVehicle {
    }

    @PermazenType
    public abstract static class Dad implements Parent {

        @FollowPath("->family<-Mom.family")
        public abstract Optional<Parent> getWife();
    }

    @PermazenType
    public abstract static class Mom implements Parent {

        @FollowPath("->family<-Dad.family")
        public abstract Optional<Parent> getHusband();
    }

    public abstract static class Child implements Person, HasVehicle {

        @FollowPath("->family<-io.permazen.FollowPathTest$Child.family->vehicle")
        public abstract NavigableSet<Vehicle> getSiblingBikes();

        @FollowPath("->family<-Dad.family")
        public abstract Optional<Dad> getDad();

        @FollowPath("->family<-Mom.family")
        public abstract Optional<Mom> getMom();

        @FollowPath("->family<-io.permazen.FollowPathTest$Child.family")
        public abstract NavigableSet<Child> getSiblings();

        @FollowPath("->family<-Dad.family->vehicle")
        public abstract Optional<Vehicle> getDadsVehicle();

        // Wider element type than necessary
        @FollowPath("->family<-Dad.family->vehicle")
        public abstract Optional<JObject> getDadsVehicle2();
    }

    @PermazenType
    public abstract static class GoodChild extends Child {
    }

// Bad @FollowPath classes

    // Wrong return type - should be NavigableSet<Vehicle>
    @PermazenType
    public abstract static class BadChild1 extends Child {
        @FollowPath("->family->vehicles")
        public abstract NavigableSet<Bike> getFamilyVehicles();
    }

    // Wrong return type - should be Optional not NavigableSet
    @PermazenType
    public abstract static class BadParent1 implements Parent {
        @FollowPath("->vehicle")
        public abstract NavigableSet<Vehicle> getMyVehicle();
    }

    // Wrong return type - should be Optional<Vehicle>
    @PermazenType
    public abstract static class BadChild3 extends Child {
        @FollowPath("->family<-Parent.family->vehicle")
        public abstract Optional<Bike> getFirstFamilyVehicle();
    }

    // Invalid path not ending on BadChild4
    @PermazenType
    public abstract static class BadChild4 extends Child {
        @FollowPath("<-Parent.vehicle")
        public abstract NavigableSet<Parent> getBogus();
    }

    // Invalid path
    @PermazenType
    public abstract static class BadChild5 extends Child {
        @FollowPath("foo.bar")
        public abstract NavigableSet<Object> getBogus();
    }
}
