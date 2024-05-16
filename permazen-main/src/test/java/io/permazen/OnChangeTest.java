
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnChange;
import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenListField;
import io.permazen.annotation.PermazenMapField;
import io.permazen.annotation.PermazenSetField;
import io.permazen.annotation.PermazenType;
import io.permazen.change.Change;
import io.permazen.change.FieldChange;
import io.permazen.change.ListFieldAdd;
import io.permazen.change.ListFieldChange;
import io.permazen.change.ListFieldClear;
import io.permazen.change.ListFieldRemove;
import io.permazen.change.ListFieldReplace;
import io.permazen.change.MapFieldAdd;
import io.permazen.change.MapFieldChange;
import io.permazen.change.MapFieldClear;
import io.permazen.change.MapFieldRemove;
import io.permazen.change.MapFieldReplace;
import io.permazen.change.SetFieldAdd;
import io.permazen.change.SetFieldChange;
import io.permazen.change.SetFieldClear;
import io.permazen.change.SetFieldRemove;
import io.permazen.change.SimpleFieldChange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class OnChangeTest extends MainTestSupport {

    private static final ThreadLocal<ArrayList<FieldChange<?>>> EVENTS = new ThreadLocal<ArrayList<FieldChange<?>>>() {
        @Override
        protected ArrayList<FieldChange<?>> initialValue() {
            return new ArrayList<>();
        }
    };

    @Test
    public void testSimpleFieldChange() {

        final Permazen pdb = BasicTest.newPermazen(Person.class, MeanPerson.class, NicePerson.class);
        final PermazenTransaction tx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(tx);
        try {

            final Person p1 = tx.create(Person.class);

            final NicePerson n1 = tx.create(NicePerson.class);
            final NicePerson n2 = tx.create(NicePerson.class);

            final MeanPerson m1 = tx.create(MeanPerson.class);

            this.verify();

            p1.setName("Person #1");
            this.verify(new SimpleFieldChange<>(p1, "name", null, "Person #1"));

            n1.setAge(10);      // no path to n1 yet
            this.verify();

            p1.getKnownPeople().add(n1);
            this.verify(new ListFieldAdd<Person, Person>(p1, "knownPeople", 0, n1));

            p1.getKnownPeople().add(n2);
            this.verify(new ListFieldAdd<Person, Person>(p1, "knownPeople", 1, n2));

            p1.getKnownPeople().add(0, m1);
            this.verify(new ListFieldAdd<Person, Person>(p1, "knownPeople", 0, m1));

            p1.getKnownPeople().add(n2);
            this.verify(new ListFieldAdd<Person, Person>(p1, "knownPeople", 3, n2));

            p1.getKnownPeople().set(3, n2);     // no change
            this.verify();

            p1.getKnownPeople().set(3, p1);
            this.verify(new ListFieldReplace<>(p1, "knownPeople", 3, n2, p1));

            p1.getKnownPeople().remove(3);
            this.verify(new ListFieldRemove<>(p1, "knownPeople", 3, p1));

            n1.setAge(10);      // no path to n1 yet
            this.verify();

            m1.getEnemies().put(n1, 0.5f);
            this.verify(new MapFieldAdd<>(m1, "enemies", n1, 0.5f));

            // Now there exists a path p1.knownPeople.element -> m1.enemies.key -> n1
            n1.setAge(20);
            this.verify(new SimpleFieldChange<Person, Integer>(n1, "age", 10, 20));

            m1.getEnemies().put(n1, 0.5f);      // no change
            this.verify();

            m1.getEnemies().put(n1, 2.5f);
            this.verify(new MapFieldReplace<>(m1, "enemies", n1, 0.5f, 2.5f));

            m1.getEnemies().remove(n2);
            this.verify();

            m1.getEnemies().remove(n1);
            this.verify(new MapFieldRemove<>(m1, "enemies", n1, 2.5f));

            m1.getEnemies().remove(n1);
            this.verify();

            m1.getEnemies().clear();        // map was already clear
            this.verify();

            m1.getEnemies().put(null, 2.5f);
            this.verify(new MapFieldAdd<MeanPerson, NicePerson, Float>(m1, "enemies", null, 2.5f));

            m1.getEnemies().clear();
            this.verify(new MapFieldClear<>(m1, "enemies"));

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testColorChange() {

        final Permazen pdb = BasicTest.newPermazen(ColorHolder.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final ColorHolder obj = ptx.create(ColorHolder.class);

            obj.setColor(Color.BLUE);
            Assert.assertSame(obj.getOldColor(), null);
            Assert.assertSame(obj.getNewColor(), Color.BLUE);

            obj.setColor(Color.GREEN);
            Assert.assertSame(obj.getOldColor(), Color.BLUE);
            Assert.assertSame(obj.getNewColor(), Color.GREEN);

            obj.setColor(null);
            Assert.assertSame(obj.getOldColor(), Color.GREEN);
            Assert.assertSame(obj.getNewColor(), null);

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testNoParamChange() {

        final Permazen pdb = BasicTest.newPermazen(Person2.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final Person2 p1 = ptx.create(Person2.class);
            final Person2 p2 = ptx.create(Person2.class);
            final Person2 p3 = ptx.create(Person2.class);

            p1.getFriends().add(p2);
            p1.getFriends().add(p3);
            p2.getFriends().add(p3);

            Assert.assertEquals(p1.getChangeNotifications(), 0);
            Assert.assertEquals(p2.getChangeNotifications(), 0);
            Assert.assertEquals(p3.getChangeNotifications(), 0);

            p1.setName("p1");

            Assert.assertEquals(p1.getChangeNotifications(), 0);
            Assert.assertEquals(p2.getChangeNotifications(), 0);
            Assert.assertEquals(p3.getChangeNotifications(), 0);

            p2.setName("p2");

            Assert.assertEquals(p1.getChangeNotifications(), 1);
            Assert.assertEquals(p2.getChangeNotifications(), 0);
            Assert.assertEquals(p3.getChangeNotifications(), 0);

            p3.setName("p3");

            Assert.assertEquals(p1.getChangeNotifications(), 2);
            Assert.assertEquals(p2.getChangeNotifications(), 1);
            Assert.assertEquals(p3.getChangeNotifications(), 0);

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testNonGenericParameter() {

        final Permazen pdb = BasicTest.newPermazen(NonGenericChange.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final NonGenericChange c = ptx.create(NonGenericChange.class);

            Assert.assertNull(c.getChange());

            c.setName("fred");

            Assert.assertEquals(c.getChange(), new SimpleFieldChange<NonGenericChange, String>(c, "name", null, "fred"));

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testInversePaths() {

        final Permazen pdb = BasicTest.newPermazen(InversePaths.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final InversePaths parent = ptx.create(InversePaths.class);
            final InversePaths child1 = ptx.create(InversePaths.class);
            final InversePaths child2 = ptx.create(InversePaths.class);
            final InversePaths child3 = ptx.create(InversePaths.class);

            child1.setParent(parent);
            child2.setParent(parent);
            child3.setParent(parent);

            Assert.assertNull(parent.getChange());
            Assert.assertNull(child1.getChange());
            Assert.assertNull(child2.getChange());
            Assert.assertNull(child3.getChange());

            child3.setName("pee-wee");

            final SimpleFieldChange<InversePaths, String> change = new SimpleFieldChange<>(child3, "name", null, "pee-wee");

            Assert.assertNull(parent.getChange());
            Assert.assertEquals(child1.getChange(), change);
            Assert.assertEquals(child2.getChange(), change);
            Assert.assertEquals(child3.getChange(), change);

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testInverseRestrictedTypes() {

        final Permazen pdb = BasicTest.newPermazen(A.class, B.class, C.class, D.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final A a = ptx.create(A.class);
            final B b = ptx.create(B.class);
            final C c = ptx.create(C.class);
            final D d = ptx.create(D.class);

            b.setA(a);
            c.setA(a);

            Assert.assertNull(a.getChange());

            d.setMiddleMan(c);                  // @OnChange path should not match because "c" is the middle object
            d.setFoo(123);

            Assert.assertNull(a.getChange());

            d.setMiddleMan(b);                  // @OnChange path should match now because "b" is the middle object
            d.setFoo(456);

            Assert.assertEquals(a.getChange(), new SimpleFieldChange<D, Integer>(d, "foo", 123, 456));

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    public void testCounter() {
        try {
            BasicTest.newPermazen(HasCounter.class).initialize();
            assert false;
        } catch (java.lang.IllegalArgumentException e) {
            // expected
        }
    }

    private void verify(FieldChange<?>... changes) {
        Assert.assertEquals(EVENTS.get(), Arrays.asList(changes), "\nACTUAL: " + EVENTS.get()
          + "\nEXPECTED: " + Arrays.asList(changes));
        EVENTS.get().clear();
    }

    @Test
    public void testChangeWithDelete() {

        final Permazen pdb = BasicTest.newPermazen(Node.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final Node parent = ptx.create(Node.class);
            final Node child = ptx.create(Node.class);

            child.setParent(parent);
            child.setColor(Color.RED);

            child.delete();

            final List<?> actual = parent.getChanges();
            final List<?> expected = Arrays.asList(new SimpleFieldChange<>(child, "color", null, Color.RED));
            Assert.assertEquals(actual, expected,
                "\n  ACTUAL: " + actual
              + "\nEXPECTED: " + expected);

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    private static void recordChange(FieldChange<?> change) {
        if (change.getPermazenObject().getPermazenTransaction() != PermazenTransaction.getCurrent()) // ignore detached changes
            return;
        EVENTS.get().add(change);
    }

// Model Classes

    @PermazenType(storageId = 100)
    public abstract static class Person implements PermazenObject {

        @PermazenField(storageId = 101)
        public abstract String getName();
        public abstract void setName(String name);

        @PermazenField(storageId = 102)
        public abstract int getAge();
        public abstract void setAge(int age);

        @PermazenListField(storageId = 103, element = @PermazenField(storageId = 104))
        public abstract List<Person> getKnownPeople();

        @OnChange(path = "->knownPeople->enemies.key", value = "age")
        private void knownEnemyAgeChange(SimpleFieldChange<NicePerson, Integer> change) {
            OnChangeTest.recordChange(change);
        }

    // name

        @OnChange("name")
        private void nameChange(FieldChange<? extends Person> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("name")
        private void nameChange(SimpleFieldChange<? extends Person, String> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("name")
        private static void personNameChange(SimpleFieldChange<? extends Person, String> change) {
            OnChangeTest.recordChange(change);
        }

    // knownPeople

        @OnChange("knownPeople")
        private void knownPeopleChange(FieldChange<? extends Person> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("knownPeople")
        private void knownPeopleChange(ListFieldChange<? extends Person> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("knownPeople")
        private void knownPeopleChange(ListFieldAdd<? extends Person, Person> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.recordChange(change);
        }

        @OnChange("knownPeople")
        private void knownPeopleChange(ListFieldRemove<? extends Person, Person> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.recordChange(change);
        }

        @OnChange("knownPeople")
        private void knownPeopleChange(ListFieldReplace<? extends Person, Person> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.recordChange(change);
        }

        @OnChange("knownPeople")
        private void knownPeopleChange(ListFieldClear<? extends Person> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.recordChange(change);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "@" + this.getObjId();
        }
    }

    @PermazenType(storageId = 200)
    public abstract static class MeanPerson extends Person {

        @PermazenMapField(storageId = 201,
          key = @PermazenField(storageId = 202),
          value = @PermazenField(storageId = 203, encoding = "float"))
        public abstract Map<NicePerson, Float> getEnemies();

    // enemies

        @OnChange("enemies")
        private void enemiesChange(FieldChange<MeanPerson> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("enemies")
        private void enemiesChange(MapFieldChange<MeanPerson> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("enemies")
        private void enemiesChange(MapFieldClear<MeanPerson> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.recordChange(change);
        }

        @OnChange("enemies")
        private void enemiesChange(MapFieldAdd<MeanPerson, NicePerson, Float> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.recordChange(change);
        }

        @OnChange("enemies")
        private void enemiesChange(MapFieldRemove<MeanPerson, NicePerson, Float> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.recordChange(change);
        }

        @OnChange("enemies")
        private void enemiesChange(MapFieldReplace<MeanPerson, NicePerson, Float> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.recordChange(change);
        }
    }

    @PermazenType(storageId = 300)
    public abstract static class NicePerson extends Person {

        @PermazenMapField(storageId = 301,
          key = @PermazenField(storageId = 302),
          value = @PermazenField(storageId = 303))
        public abstract Map<Person, Float> getRatings();

        @PermazenSetField(storageId = 304, element = @PermazenField(storageId = 305))
        public abstract Set<Person> getFriends();

    // ratings

        @OnChange("ratings")
        private void ratingsChange(FieldChange<NicePerson> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("ratings")
        private void ratingsChange(MapFieldChange<NicePerson> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("ratings")
        private void ratingsChange(MapFieldAdd<NicePerson, Person, Float> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.recordChange(change);
        }

        @OnChange("ratings")
        private void ratingsChange(MapFieldRemove<NicePerson, Person, Float> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.recordChange(change);
        }

        @OnChange("ratings")
        private void ratingsChange(MapFieldReplace<NicePerson, Person, Float> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.recordChange(change);
        }

        @OnChange("ratings")
        private void ratingsChange(MapFieldClear<NicePerson> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.recordChange(change);
        }

    // friends

        @OnChange("friends")
        private void friendsChange(FieldChange<NicePerson> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("friends")
        private void friendsChange(SetFieldChange<NicePerson> change) {
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("friends")
        private void friendsChange(SetFieldAdd<NicePerson, Person> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.recordChange(change);
        }

        @OnChange("friends")
        private void friendsChange(SetFieldRemove<NicePerson, Person> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.recordChange(change);
        }

        @OnChange("friends")
        private void friendsChange(SetFieldClear<NicePerson> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.recordChange(change);
        }
    }

    @PermazenType(storageId = 400)
    public abstract static class ColorHolder implements PermazenObject {

        private Color oldColor;
        private Color newColor;

        public Color getOldColor() {
            return this.oldColor;
        }
        public Color getNewColor() {
            return this.newColor;
        }

        @PermazenField(storageId = 401)
        public abstract Color getColor();
        public abstract void setColor(Color color);

        @OnChange("color")
        private void colorChange(SimpleFieldChange<ColorHolder, Color> change) {
            Assert.assertSame(change.getObject(), this);
            this.oldColor = change.getOldValue();
            this.newColor = change.getNewValue();
        }
    }

    public enum Color {
        RED,
        GREEN,
        BLUE;
    }

    @PermazenType
    public abstract static class Person2 implements PermazenObject {

        private int changeNotifications;

        public int getChangeNotifications() {
            return this.changeNotifications;
        }

        @PermazenField
        public abstract String getName();
        public abstract void setName(String name);

        @PermazenSetField
        public abstract Set<Person2> getFriends();

        @OnChange(path = "->friends.element", value = "name")
        private void onChange(Change<?> change) {
            this.changeNotifications++;
        }
    }

    @PermazenType
    public abstract static class NonGenericChange implements PermazenObject {

        private Object change;

        public Object getChange() {
            return this.change;
        }

        @PermazenField(storageId = 123)
        public abstract String getName();
        public abstract void setName(String name);

        @OnChange
        private void onChange(Object change) {
            this.change = change;
        }
    }

    @PermazenType
    public abstract static class InversePaths implements PermazenObject {

        private SimpleFieldChange<?, ?> change;

        public SimpleFieldChange<?, ?> getChange() {
            return this.change;
        }

        public abstract InversePaths getParent();
        public abstract void setParent(InversePaths parent);

        @PermazenField(storageId = 123)
        public abstract String getName();
        public abstract void setName(String name);

        @OnChange(path = "->parent<-InversePaths.parent", value = "name")   // i.e., change in any sibling's name (including myself)
        private void onChange(SimpleFieldChange<?, ?> change) {
            this.change = change;
        }
    }

// Inverse Restricted Test

    public interface HasA {
        A getA();
        void setA(A a);
    }

    @PermazenType
    public abstract static class A implements PermazenObject {

        private SimpleFieldChange<D, Integer> change;

        public SimpleFieldChange<D, Integer> getChange() {
            return this.change;
        }

        @OnChange(path = "<-B.a<-D.middleMan", value = "foo")
        private void onChange(SimpleFieldChange<D, Integer> change) {
            this.change = change;
        }
    }

    @PermazenType
    public abstract static class B implements PermazenObject, HasA {
    }

    @PermazenType
    public abstract static class C implements PermazenObject, HasA {
    }

    @PermazenType
    public abstract static class D implements PermazenObject {

        public abstract PermazenObject getMiddleMan();
        public abstract void setMiddleMan(PermazenObject x);

        @PermazenField(storageId = 10)
        public abstract int getFoo();
        public abstract void setFoo(int x);
    }

// Counter Test

    @PermazenType
    public abstract static class HasCounter implements PermazenObject {

        public abstract Counter getCounter();

        @OnChange("counter")
        private void onChange(Change<HasCounter> change) {
            throw new RuntimeException("unexpected notification");
        }
    }

// Delete Test

    @PermazenType
    public abstract static class Node implements PermazenObject {

        private final ArrayList<SimpleFieldChange<Node, Color>> changes = new ArrayList<>();

        public List<SimpleFieldChange<Node, Color>> getChanges() {
            return this.changes;
        }

        @PermazenField(storageId = 10)
        public abstract Node getParent();
        public abstract void setParent(Node x);

        @PermazenField(storageId = 20)
        public abstract Color getColor();
        public abstract void setColor(Color color);

        @OnChange(path = "<-Node.parent", value = "color")
        private void onChange(SimpleFieldChange<Node, Color> change) {
            this.changes.add(change);
        }
    }
}
