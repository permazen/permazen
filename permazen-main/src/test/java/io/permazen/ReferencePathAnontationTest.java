
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenType;
import io.permazen.annotation.ReferencePath;
import io.permazen.test.TestSupport;

import java.util.NavigableSet;
import java.util.Optional;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReferencePathAnontationTest extends MainTestSupport {

    @Test
    public void testReferencePath() {
        final Permazen pdb = BasicTest.newPermazen(Family.class, Car.class, Bike.class, Dad.class, Mom.class, GoodChild.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final Family family = ptx.create(Family.class);

            final Mom mom = ptx.create(Mom.class);
            final Dad dad = ptx.create(Dad.class);

            mom.setFamily(family);
            dad.setFamily(family);

            Assert.assertSame(family.getMom().orElse(null), mom);
            Assert.assertSame(family.getDad().orElse(null), dad);

            Assert.assertSame(mom.getHusband().orElse(null), dad);
            Assert.assertSame(dad.getWife().orElse(null), mom);

            TestSupport.checkSet(family.getMembers(), buildSet(mom, dad));
            TestSupport.checkSet(mom.getAllFamilyMembers(), buildSet(mom, dad));
            TestSupport.checkSet(dad.getAllFamilyMembers(), buildSet(mom, dad));

            final Child child1 = ptx.create(GoodChild.class);
            final Child child2 = ptx.create(GoodChild.class);
            final Child child3 = ptx.create(GoodChild.class);

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

            final Car mcar = ptx.create(Car.class);
            final Car dcar = ptx.create(Car.class);

            mom.setVehicle(mcar);
            dad.setVehicle(dcar);

            final Bike bike1 = ptx.create(Bike.class);
            final Bike bike2 = ptx.create(Bike.class);
            final Bike bike3 = ptx.create(Bike.class);

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
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test(dataProvider = "badClasses")
    public void testBadChild(Class<?> badClass) {
        try {
            BasicTest.newPermazen(Family.class, Car.class, Bike.class, Dad.class, Mom.class, badClass).initialize();
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

    public interface Vehicle extends PermazenObject {

        @ReferencePath("<-io.permazen.ReferencePathAnontationTest$Person.vehicle")
        NavigableSet<Person> getAllOwners();

        @ReferencePath("<-io.permazen.ReferencePathAnontationTest$Person.vehicle")
        Optional<Person> getFirstOwner();
    }

    public interface HasVehicle {
        Vehicle getVehicle();
        void setVehicle(Vehicle x);
    }

    public interface Person extends PermazenObject {
        Family getFamily();
        void setFamily(Family x);

        @ReferencePath("->family<-io.permazen.ReferencePathAnontationTest$Person.family")
        NavigableSet<Person> getAllFamilyMembers();
    }

    @PermazenType
    public abstract static class Family implements PermazenObject {

        @ReferencePath("<-io.permazen.ReferencePathAnontationTest$Person.family")
        public abstract NavigableSet<Person> getMembers();

        @ReferencePath("<-Mom.family")
        public abstract Optional<Mom> getMom();

        @ReferencePath("<-Dad.family")
        public abstract Optional<Dad> getDad();

        @ReferencePath("<-io.permazen.ReferencePathAnontationTest$Child.family")
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

        @ReferencePath("->family<-Mom.family")
        public abstract Optional<Parent> getWife();
    }

    @PermazenType
    public abstract static class Mom implements Parent {

        @ReferencePath("->family<-Dad.family")
        public abstract Optional<Parent> getHusband();
    }

    public abstract static class Child implements Person, HasVehicle {

        @ReferencePath("->family<-io.permazen.ReferencePathAnontationTest$Child.family->vehicle")
        public abstract NavigableSet<Vehicle> getSiblingBikes();

        @ReferencePath("->family<-Dad.family")
        public abstract Optional<Dad> getDad();

        @ReferencePath("->family<-Mom.family")
        public abstract Optional<Mom> getMom();

        @ReferencePath("->family<-io.permazen.ReferencePathAnontationTest$Child.family")
        public abstract NavigableSet<Child> getSiblings();

        @ReferencePath("->family<-Dad.family->vehicle")
        public abstract Optional<Vehicle> getDadsVehicle();

        // Wider element type than necessary
        @ReferencePath("->family<-Dad.family->vehicle")
        public abstract Optional<PermazenObject> getDadsVehicle2();
    }

    @PermazenType
    public abstract static class GoodChild extends Child {
    }

// Bad @ReferencePath classes

    // Wrong return type - should be NavigableSet<Vehicle>
    @PermazenType
    public abstract static class BadChild1 extends Child {
        @ReferencePath("->family->vehicles")
        public abstract NavigableSet<Bike> getFamilyVehicles();
    }

    // Wrong return type - should be Optional not NavigableSet
    @PermazenType
    public abstract static class BadParent1 implements Parent {
        @ReferencePath("->vehicle")
        public abstract NavigableSet<Vehicle> getMyVehicle();
    }

    // Wrong return type - should be Optional<Vehicle>
    @PermazenType
    public abstract static class BadChild3 extends Child {
        @ReferencePath("->family<-Parent.family->vehicle")
        public abstract Optional<Bike> getFirstFamilyVehicle();
    }

    // Invalid path not ending on BadChild4
    @PermazenType
    public abstract static class BadChild4 extends Child {
        @ReferencePath("<-Parent.vehicle")
        public abstract NavigableSet<Parent> getBogus();
    }

    // Invalid path
    @PermazenType
    public abstract static class BadChild5 extends Child {
        @ReferencePath("foo.bar")
        public abstract NavigableSet<Object> getBogus();
    }
}
