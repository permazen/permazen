
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.JListField;
import io.permazen.annotation.JMapField;
import io.permazen.annotation.JSetField;
import io.permazen.annotation.PermazenType;
import io.permazen.annotation.OnChange;
import io.permazen.change.Change;
import io.permazen.change.ChangeCopier;
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
import io.permazen.test.TestSupport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class OnChangeTest extends TestSupport {

    private static final ThreadLocal<ArrayList<FieldChange<?>>> EVENTS = new ThreadLocal<ArrayList<FieldChange<?>>>() {
        @Override
        protected ArrayList<FieldChange<?>> initialValue() {
            return new ArrayList<>();
        }
    };

    @Test
    public void testSimpleFieldChange() {

        final Permazen jdb = BasicTest.getPermazen(Person.class, MeanPerson.class, NicePerson.class);
        final JTransaction tx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(tx);
        try {

            final Person p1 = tx.create(Person.class);

            final NicePerson n1 = tx.create(NicePerson.class);
            final NicePerson n2 = tx.create(NicePerson.class);

            final MeanPerson m1 = tx.create(MeanPerson.class);

            this.verify();

            p1.setName("Person #1");
            this.verify(new SimpleFieldChange<>(p1, 101, "name", null, "Person #1"));

            n1.setAge(10);      // no path to n1 yet
            this.verify();

            p1.getKnownPeople().add(n1);
            this.verify(new ListFieldAdd<Person, Person>(p1, 103, "knownPeople", 0, n1));

            p1.getKnownPeople().add(n2);
            this.verify(new ListFieldAdd<Person, Person>(p1, 103, "knownPeople", 1, n2));

            p1.getKnownPeople().add(0, m1);
            this.verify(new ListFieldAdd<Person, Person>(p1, 103, "knownPeople", 0, m1));

            p1.getKnownPeople().add(n2);
            this.verify(new ListFieldAdd<Person, Person>(p1, 103, "knownPeople", 3, n2));

            p1.getKnownPeople().set(3, n2);     // no change
            this.verify();

            p1.getKnownPeople().set(3, p1);
            this.verify(new ListFieldReplace<>(p1, 103, "knownPeople", 3, n2, p1));

            p1.getKnownPeople().remove(3);
            this.verify(new ListFieldRemove<>(p1, 103, "knownPeople", 3, p1));

            n1.setAge(10);      // no path to n1 yet
            this.verify();

            m1.getEnemies().put(n1, 0.5f);
            this.verify(new MapFieldAdd<>(m1, 201, "enemies", n1, 0.5f));

            // Now there exists a path p1.knownPeople.element -> m1.enemies.key -> n1
            n1.setAge(20);
            this.verify(new SimpleFieldChange<Person, Integer>(n1, 102, "age", 10, 20));

            m1.getEnemies().put(n1, 0.5f);      // no change
            this.verify();

            m1.getEnemies().put(n1, 2.5f);
            this.verify(new MapFieldReplace<>(m1, 201, "enemies", n1, 0.5f, 2.5f));

            m1.getEnemies().remove(n2);
            this.verify();

            m1.getEnemies().remove(n1);
            this.verify(new MapFieldRemove<>(m1, 201, "enemies", n1, 2.5f));

            m1.getEnemies().remove(n1);
            this.verify();

            m1.getEnemies().clear();        // map was already clear
            this.verify();

            m1.getEnemies().put(null, 2.5f);
            this.verify(new MapFieldAdd<MeanPerson, NicePerson, Float>(m1, 201, "enemies", null, 2.5f));

            m1.getEnemies().clear();
            this.verify(new MapFieldClear<>(m1, 201, "enemies"));

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testColorChange() {

        final Permazen jdb = BasicTest.getPermazen(ColorHolder.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final ColorHolder obj = jtx.create(ColorHolder.class);

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
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testNoParamChange() {

        final Permazen jdb = BasicTest.getPermazen(Person2.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final Person2 p1 = jtx.create(Person2.class);
            final Person2 p2 = jtx.create(Person2.class);
            final Person2 p3 = jtx.create(Person2.class);

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
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testNonGenericParameter() {

        final Permazen jdb = BasicTest.getPermazen(NonGenericChange.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final NonGenericChange c = jtx.create(NonGenericChange.class);

            Assert.assertNull(c.getChange());

            c.setName("fred");

            Assert.assertEquals(c.getChange(),
              new SimpleFieldChange<NonGenericChange, String>(c, 123, "name", null, "fred"));

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testInversePaths() {

        final Permazen jdb = BasicTest.getPermazen(InversePaths.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final InversePaths parent = jtx.create(InversePaths.class);
            final InversePaths child1 = jtx.create(InversePaths.class);
            final InversePaths child2 = jtx.create(InversePaths.class);
            final InversePaths child3 = jtx.create(InversePaths.class);

            child1.setParent(parent);
            child2.setParent(parent);
            child3.setParent(parent);

            Assert.assertNull(parent.getChange());
            Assert.assertNull(child1.getChange());
            Assert.assertNull(child2.getChange());
            Assert.assertNull(child3.getChange());

            child3.setName("pee-wee");

            final SimpleFieldChange<InversePaths, String> change = new SimpleFieldChange<>(child3, 123, "name", null, "pee-wee");

            Assert.assertNull(parent.getChange());
            Assert.assertEquals(child1.getChange(), change);
            Assert.assertEquals(child2.getChange(), change);
            Assert.assertEquals(child3.getChange(), change);

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testInverseRestrictedTypes() {

        final Permazen jdb = BasicTest.getPermazen(A.class, B.class, C.class, D.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final A a = jtx.create(A.class);
            final B b = jtx.create(B.class);
            final C c = jtx.create(C.class);
            final D d = jtx.create(D.class);

            b.setA(a);
            c.setA(a);

            Assert.assertNull(a.getChange());

            d.setMiddleMan(c);                  // @OnChange path should not match because "c" is the middle object
            d.setFoo(123);

            Assert.assertNull(a.getChange());

            d.setMiddleMan(b);                  // @OnChange path should match now because "b" is the middle object
            d.setFoo(456);

            Assert.assertEquals(a.getChange(), new SimpleFieldChange<D, Integer>(d, 10, "foo", 123, 456));

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testCounter() {
        try {
            BasicTest.getPermazen(HasCounter.class);
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

    private static void recordChange(FieldChange<?> change) {
        if (change.getJObject().getTransaction() != JTransaction.getCurrent())      // ignore snapshot changes
            return;
        EVENTS.get().add(change);
    }

    private static void verifyCopy(Change<?> change) {
        if (change.getJObject().getTransaction() != JTransaction.getCurrent())      // ignore snapshot changes
            return;
        final Change<?> copy1 = change.visit(new ChangeCopier());
        final Change<?> copy2 = copy1.visit(new ChangeCopier(JTransaction.getCurrent()));
        Assert.assertEquals(copy2, change);
    }

// Model Classes

    @PermazenType(storageId = 100)
    public abstract static class Person implements JObject {

        @JField(storageId = 101)
        public abstract String getName();
        public abstract void setName(String name);

        @JField(storageId = 102)
        public abstract int getAge();
        public abstract void setAge(int age);

        @JListField(storageId = 103, element = @JField(storageId = 104))
        public abstract List<Person> getKnownPeople();

        @OnChange("knownPeople.element.enemies.key.age")
        private void knownEnemyAgeChange(SimpleFieldChange<NicePerson, Integer> change) {
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }

    // name

        @OnChange("name")
        private void nameChange(FieldChange<? extends Person> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
        }

        @OnChange("name")
        private void nameChange(SimpleFieldChange<? extends Person, String> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
        }

    // knownPeople

        @OnChange("knownPeople")
        private void knownPeopleChange(FieldChange<? extends Person> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
        }

        @OnChange("knownPeople")
        private void knownPeopleChange(ListFieldChange<? extends Person> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
        }

        @OnChange("knownPeople")
        private void knownPeopleChange(ListFieldAdd<? extends Person, Person> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }

        @OnChange("knownPeople")
        private void knownPeopleChange(ListFieldRemove<? extends Person, Person> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }

        @OnChange("knownPeople")
        private void knownPeopleChange(ListFieldReplace<? extends Person, Person> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }

        @OnChange("knownPeople")
        private void knownPeopleChange(ListFieldClear<? extends Person> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "@" + this.getObjId();
        }
    }

    @PermazenType(storageId = 200)
    public abstract static class MeanPerson extends Person {

        @JMapField(storageId = 201,
          key = @JField(storageId = 202),
          value = @JField(storageId = 203, type = "float"))
        public abstract Map<NicePerson, Float> getEnemies();

    // enemies

        @OnChange("enemies")
        private void enemiesChange(FieldChange<MeanPerson> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
        }

        @OnChange("enemies")
        private void enemiesChange(MapFieldChange<MeanPerson> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
        }

        @OnChange("enemies")
        private void enemiesChange(MapFieldClear<MeanPerson> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }

        @OnChange("enemies")
        private void enemiesChange(MapFieldAdd<MeanPerson, NicePerson, Float> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }

        @OnChange("enemies")
        private void enemiesChange(MapFieldRemove<MeanPerson, NicePerson, Float> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }

        @OnChange("enemies")
        private void enemiesChange(MapFieldReplace<MeanPerson, NicePerson, Float> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }

        @OnChange(startType = Person.class, value = "name")
        private static void personNameChange(SimpleFieldChange<? extends Person, String> change) {
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }
    }

    @PermazenType(storageId = 300)
    public abstract static class NicePerson extends Person {

        @JMapField(storageId = 301,
          key = @JField(storageId = 302),
          value = @JField(storageId = 303))
        public abstract Map<Person, Float> getRatings();

        @JSetField(storageId = 304, element = @JField(storageId = 305))
        public abstract Set<Person> getFriends();

    // ratings

        @OnChange("ratings")
        private void ratingsChange(FieldChange<NicePerson> change) {
            OnChangeTest.verifyCopy(change);
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("ratings")
        private void ratingsChange(MapFieldChange<NicePerson> change) {
            OnChangeTest.verifyCopy(change);
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("ratings")
        private void ratingsChange(MapFieldAdd<NicePerson, Person, Float> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }

        @OnChange("ratings")
        private void ratingsChange(MapFieldRemove<NicePerson, Person, Float> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }

        @OnChange("ratings")
        private void ratingsChange(MapFieldReplace<NicePerson, Person, Float> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }

        @OnChange("ratings")
        private void ratingsChange(MapFieldClear<NicePerson> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }

    // friends

        @OnChange("friends")
        private void friendsChange(FieldChange<NicePerson> change) {
            OnChangeTest.verifyCopy(change);
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("friends")
        private void friendsChange(SetFieldChange<NicePerson> change) {
            OnChangeTest.verifyCopy(change);
            Assert.assertSame(change.getObject(), this);
        }

        @OnChange("friends")
        private void friendsChange(SetFieldAdd<NicePerson, Person> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }

        @OnChange("friends")
        private void friendsChange(SetFieldRemove<NicePerson, Person> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }

        @OnChange("friends")
        private void friendsChange(SetFieldClear<NicePerson> change) {
            Assert.assertSame(change.getObject(), this);
            OnChangeTest.verifyCopy(change);
            OnChangeTest.recordChange(change);
        }
    }

    @PermazenType(storageId = 400)
    public abstract static class ColorHolder implements JObject {

        private Color oldColor;
        private Color newColor;

        public Color getOldColor() {
            return this.oldColor;
        }
        public Color getNewColor() {
            return this.newColor;
        }

        @JField(storageId = 401)
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
    public abstract static class Person2 implements JObject {

        private int changeNotifications;

        public int getChangeNotifications() {
            return this.changeNotifications;
        }

        @JField
        public abstract String getName();
        public abstract void setName(String name);

        @JSetField
        public abstract Set<Person2> getFriends();

        @OnChange("friends.element.name")
        private void onChange() {
            this.changeNotifications++;
        }
    }

    @PermazenType
    public abstract static class NonGenericChange implements JObject {

        private Object change;

        public Object getChange() {
            return this.change;
        }

        @JField(storageId = 123)
        public abstract String getName();
        public abstract void setName(String name);

        @OnChange
        private void onChange(Object change) {
            this.change = change;
        }
    }

    @PermazenType
    public abstract static class InversePaths implements JObject {

        private SimpleFieldChange<?, ?> change;

        public SimpleFieldChange<?, ?> getChange() {
            return this.change;
        }

        public abstract InversePaths getParent();
        public abstract void setParent(InversePaths parent);

        @JField(storageId = 123)
        public abstract String getName();
        public abstract void setName(String name);

        @OnChange("parent.^InversePaths:parent^.name")          // i.e., change in any sibling's name (including myself)
        private void onChange(SimpleFieldChange<?, ?> change) {
            this.change = change;
        }
    }

// Inverse Restricted Test

    public static interface HasA {
        A getA();
        void setA(A a);
    }

    @PermazenType
    public abstract static class A implements JObject {

        private SimpleFieldChange<D, Integer> change;

        public SimpleFieldChange<D, Integer> getChange() {
            return this.change;
        }

        @OnChange("^B:a^.^D:middleMan^.foo")
        private void onChange(SimpleFieldChange<D, Integer> change) {
            this.change = change;
        }
    }

    @PermazenType
    public abstract static class B implements JObject, HasA {
    }

    @PermazenType
    public abstract static class C implements JObject, HasA {
    }

    @PermazenType
    public abstract static class D implements JObject {

        public abstract JObject getMiddleMan();
        public abstract void setMiddleMan(JObject x);

        @JField(storageId = 10)
        public abstract int getFoo();
        public abstract void setFoo(int x);
    }

// Counter Test

    @PermazenType
    public abstract static class HasCounter implements JObject {

        public abstract Counter getCounter();

        @OnChange("counter")
        private void onChange(Change<HasCounter> change) {
            throw new RuntimeException("unexpected notification");
        }
    }
}
