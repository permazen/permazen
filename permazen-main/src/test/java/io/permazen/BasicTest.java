
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenListField;
import io.permazen.annotation.PermazenMapField;
import io.permazen.annotation.PermazenSetField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.core.DeletedObjectException;
import io.permazen.core.ReferencedObjectException;
import io.permazen.core.util.ObjIdMap;
import io.permazen.index.Index1;
import io.permazen.index.Index2;
import io.permazen.test.TestSupport;
import io.permazen.tuple.Tuple2;
import io.permazen.tuple.Tuple3;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BasicTest extends MainTestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testBasicStuff() throws Exception {

        final Permazen pdb = BasicTest.newPermazen();

        final PermazenTransaction tx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(tx);
        try {

            final MeanPerson t1 = tx.create(MeanPerson.class);
            final Person t2 = tx.create(Person.class);
            final Person t3 = tx.create(Person.class);
            this.check(t1, false, (byte)0, (short)0, (char)0, 0, 0.0f, 0L, 0.0, null, null, null, null, null);

            TestSupport.checkSet(tx.getAll(Object.class), buildSet(t1, t2, t3));

            t1.setZ(true);
            t1.setB((byte)123);
            t1.setS((short)-32763);
            t1.setC('!');
            t1.setI(-99);
            t1.setF(12.34e-13f);
            t1.setJ(Long.MAX_VALUE);
            t1.setD(Double.POSITIVE_INFINITY);

            t1.setString("hey there");
            t1.setMood(Mood.HAPPY);
            t1.setFriend(t1);

            final SortedSet<String> nicknames = t1.getNicknames();
            nicknames.add("cherry");
            nicknames.add("banana");
            nicknames.add("dinkle");
            nicknames.add("apple");
            nicknames.remove("cherry");

            final List<Person> enemies = t1.getEnemies();
            enemies.add(t3);

            t1.getRatings().put(t1, 100.0f);
            t1.getRatings().put(t2, -99.0f);
            t1.getRatings().put(null, -0.0f);
            t3.getRatings().put(t1, -3.0f);
            try {
                ((Map)t3.getRatings()).put(t1, -3);
                assert false;
            } catch (IllegalArgumentException e) {
                // expected
            }

            try {
                t1.getScores().add(null);
                assert false;
            } catch (IllegalArgumentException e) {
                // expected
            }
            t1.getScores().add(23);
            t1.getScores().add(21);
            t1.getScores().add(22);
            t1.getScores().add(21);
            t2.getScores().add(123);
            t2.getScores().add(456);
            t3.getScores().add(789);
            t3.getScores().add(456);
            t3.getScores().add(123);
            t3.getScores().add(456);

            t3.setMood(Mood.NORMAL);

            this.check(t1, true, (byte)123, (short)-32763, '!', -99, 12.34e-13f, Long.MAX_VALUE, Double.POSITIVE_INFINITY,
              "hey there", Mood.HAPPY, t1,
              Arrays.asList("apple", "banana", "dinkle"),
              Arrays.asList(23, 21, 22, 21),
              Arrays.asList(t3),
              t1, 100.0f, t2, -99.0f, null, -0.0f);

            this.check(t2, false, (byte)0, (short)0, (char)0, 0, 0.0f, 0L, 0.0, null, null, null, null, Arrays.asList(123, 456));
            this.check(t3, false, (byte)0, (short)0, (char)0, 0, 0.0f, 0L, 0.0, null, Mood.NORMAL, null, null,
              Arrays.asList(789, 456, 123, 456), t1, -3.0f);

            // Test copyTo() with different target
            final MeanPerson t1dup = tx.create(MeanPerson.class);
            t1.copyTo(tx, -1, new CopyState(new ObjIdMap<>(Collections.singletonMap(t1.getObjId(), t1dup.getObjId())), false));
            this.check(t1dup, true, (byte)123, (short)-32763, '!', -99, 12.34e-13f, Long.MAX_VALUE, Double.POSITIVE_INFINITY,
              "hey there", Mood.HAPPY, t1dup,
              Arrays.asList("apple", "banana", "dinkle"),
              Arrays.asList(23, 21, 22, 21),
              Arrays.asList(t3),
              t1dup, 100.0f, t2, -99.0f, null, -0.0f);
            t1dup.delete();

            t2.setBooleanArray(null);
            Assert.assertNull(t2.getBooleanArray());
            t2.setBooleanArray(new boolean[] { false, true, false });
            Assert.assertEquals(t2.getBooleanArray(), new boolean[] { false, true, false });

            // Check indexes
            TestSupport.checkMap(BasicTest.queryNicknames().asMap(), buildMap(
              "dinkle",     buildSet(t1),
              "banana",     buildSet(t1),
              "apple",      buildSet(t1)));
            TestSupport.checkMap(BasicTest.queryHaters().asMap(), buildMap(
              t3,           buildSet(t1)));
            TestSupport.checkMap(BasicTest.queryScores().asMap(), buildMap(
              21,           buildSet(t1),
              22,           buildSet(t1),
              23,           buildSet(t1),
              123,          buildSet(t2, t3),
              456,          buildSet(t2, t3),
              789,          buildSet(t3)));
            TestSupport.checkSet(BasicTest.queryScoreEntries().asSet(), buildSet(
              new Tuple3<Integer, Person, Integer>(21, t1, 1),
              new Tuple3<Integer, Person, Integer>(21, t1, 3),
              new Tuple3<Integer, Person, Integer>(22, t1, 2),
              new Tuple3<Integer, Person, Integer>(23, t1, 0),
              new Tuple3<>(123, t2, 0),
              new Tuple3<>(123, t3, 2),
              new Tuple3<>(456, t2, 1),
              new Tuple3<>(456, t3, 1),
              new Tuple3<>(456, t3, 3),
              new Tuple3<>(789, t3, 0)));
            TestSupport.checkMap(BasicTest.queryRatingKeys().asMap(), buildMap(
              t1,           buildSet(t1, t3),
              t2,           buildSet(t1),
              null,         buildSet(t1)));
            TestSupport.checkSet(BasicTest.queryRatingValueEntries().asSet(), buildSet(
              new Tuple3<Float, Person, Person>(100.0f, t1, t1),
              new Tuple3<Float, Person, Person>(-99.0f, t1, t2),
              new Tuple3<Float, Person, Person>(-0.0f, t1, null),
              new Tuple3<Float, Person, Person>(-3.0f, t3, t1)));
            TestSupport.checkMap(BasicTest.queryMoods().asMap(), buildMap(
              Mood.HAPPY,   buildSet(t1),
              Mood.NORMAL,  buildSet(t3),
              null,         buildSet(t2)));

            // Check restricted indexes
            TestSupport.checkMap(BasicTest.queryScoresMean().asMap(), buildMap(
              21,           buildSet(t1),
              22,           buildSet(t1),
              23,           buildSet(t1)));
            TestSupport.checkSet(BasicTest.queryScoreEntriesMean().asSet(), buildSet(
              new Tuple3<>(21, t1, 1),
              new Tuple3<>(21, t1, 3),
              new Tuple3<>(22, t1, 2),
              new Tuple3<>(23, t1, 0)));
            TestSupport.checkMap(BasicTest.queryScoreEntriesMean().asMap(), buildMap(
              new Tuple2<>(21, t1),  buildSet(1, 3),
              new Tuple2<>(22, t1),  buildSet(2),
              new Tuple2<>(23, t1),  buildSet(0)));
            TestSupport.checkSet(BasicTest.queryScoreEntriesMean().asIndex1().asSet(), buildSet(
              new Tuple2<>(21, t1),
              new Tuple2<>(22, t1),
              new Tuple2<>(23, t1)));
            TestSupport.checkMap(BasicTest.queryRatingKeysMean().asMap(), buildMap(
              t1,           buildSet(t1),
              t2,           buildSet(t1),
              null,         buildSet(t1)));
            TestSupport.checkSet(BasicTest.queryRatingValueEntriesMean().asSet(), buildSet(
              new Tuple3<Float, MeanPerson, Person>(100.0f, t1, t1),
              new Tuple3<>(-99.0f, t1, t2),
              new Tuple3<Float, MeanPerson, Person>(-0.0f, t1, null)));
            TestSupport.checkMap(BasicTest.queryMoodsMean().asMap(), buildMap(
              Mood.HAPPY,   buildSet(t1)));

            try {
                t1.delete();
                assert false;
            } catch (ReferencedObjectException e) {
                // expected
            }
            t3.getRatings().remove(t1);

            boolean deleted = t1.delete();
            Assert.assertTrue(deleted);
            TestSupport.checkMap(BasicTest.queryHaters().asMap(), buildMap());
            TestSupport.checkMap(BasicTest.queryMoods().asMap(), buildMap(
              Mood.NORMAL,  buildSet(t3),
              null,         buildSet(t2)));

            try {
                t1.getScores().clear();                     // we only detect object deletion on mutation
                assert false;
            } catch (DeletedObjectException e) {
                // expected
            }

            deleted = t1.delete();
            Assert.assertFalse(deleted);

            boolean recreated = t1.recreate();
            Assert.assertTrue(recreated);
            this.check(t1, false, (byte)0, (short)0, (char)0, 0, 0.0f, 0L, 0.0, null, null, null, null, null);

            recreated = t1.recreate();
            Assert.assertFalse(recreated);

            tx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    private void check(MeanPerson t, boolean z, byte b, short s, char c, int i, float f, long j, double d,
      String string, Mood mood, Person friend, Collection<String> nicknames, List<Integer> scores, List<Person> enemies,
      Object... ratingKeyValues) {
        this.check((Person)t, z, b, s, c, i, f, j, d, string, mood, friend, nicknames, scores, ratingKeyValues);
        if (enemies == null)
            enemies = Collections.<Person>emptyList();
        Assert.assertEquals(t.getEnemies(), enemies);
    }

    private void check(Person t, boolean z, byte b, short s, char c, int i, float f, long j, double d,
      String string, Mood mood, Person friend, Collection<String> nicknames, List<Integer> scores, Object... ratingKeyValues) {
        Assert.assertEquals(t.getZ(), z);
        Assert.assertEquals(t.getB(), b);
        Assert.assertEquals(t.getS(), s);
        Assert.assertEquals(t.getC(), c);
        Assert.assertEquals(t.getI(), i);
        Assert.assertEquals(t.getF(), f);
        Assert.assertEquals(t.getJ(), j);
        Assert.assertEquals(t.getD(), d);
        Assert.assertEquals(t.getString(), string);
        Assert.assertEquals(t.getMood(), mood);
        Assert.assertSame(t.getFriend(), friend);
        if (nicknames == null)
            nicknames = Collections.<String>emptySet();
        Assert.assertEquals(t.getNicknames().toArray(new String[0]), nicknames.toArray(new String[nicknames.size()]));
        Assert.assertEquals(t.getScores(), scores != null ? scores : Collections.emptyList());
        TestSupport.checkMap(t.getRatings(), buildMap(ratingKeyValues));
    }

    public static Permazen newPermazen() {
        return BasicTest.newPermazen(MeanPerson.class, Person.class);
    }

    public static Permazen newPermazen(Class<?>... classes) {
        return BasicTest.newPermazen(Arrays.<Class<?>>asList(classes));
    }

    public static Permazen newPermazen(Collection<Class<?>> classes) {
        final PermazenConfig config = PermazenConfig.builder().modelClasses(classes).build();
        return new Permazen(config);
    }

    public static Permazen newPermazen(Database db, Class<?>... classes) {
        return BasicTest.newPermazen(db, Arrays.<Class<?>>asList(classes));
    }

    public static Permazen newPermazen(Database db, Collection<Class<?>> classes) {
        return PermazenConfig.builder().database(db).modelClasses(classes).build().newPermazen();
    }

// Person queries

    public static Index1<String, Person> queryNicknames() {
        return PermazenTransaction.getCurrent().querySimpleIndex(Person.class, "nicknames.element", String.class);
    }

    public static Index1<Mood, Person> queryMoods() {
        return PermazenTransaction.getCurrent().querySimpleIndex(Person.class, "mood", Mood.class);
    }

    public static Index1<Integer, Person> queryScores() {
        return PermazenTransaction.getCurrent().querySimpleIndex(Person.class, "scores.element", Integer.class);
    }

    public static Index2<Integer, Person, Integer> queryScoreEntries() {
        return PermazenTransaction.getCurrent().queryListElementIndex(Person.class, "scores.element", Integer.class);
    }

    public static Index1<Person, Person> queryRatingKeys() {
        return PermazenTransaction.getCurrent().querySimpleIndex(Person.class, "ratings.key", Person.class);
    }

    public static Index2<Float, Person, Person> queryRatingValueEntries() {
        return PermazenTransaction.getCurrent().queryMapValueIndex(Person.class, "ratings.value", Float.class, Person.class);
    }

// MeanPerson queries

    public static Index1<Person, MeanPerson> queryHaters() {
        return PermazenTransaction.getCurrent().querySimpleIndex(MeanPerson.class, "enemies.element", Person.class);
    }

    public static Index1<Mood, MeanPerson> queryMoodsMean() {
        return PermazenTransaction.getCurrent().querySimpleIndex(MeanPerson.class, "mood", Mood.class);
    }

    public static Index1<Integer, MeanPerson> queryScoresMean() {
        return PermazenTransaction.getCurrent().querySimpleIndex(MeanPerson.class, "scores.element", Integer.class);
    }

    public static Index2<Integer, MeanPerson, Integer> queryScoreEntriesMean() {
        return PermazenTransaction.getCurrent().queryListElementIndex(MeanPerson.class, "scores.element", Integer.class);
    }

    public static Index1<Person, MeanPerson> queryRatingKeysMean() {
        return PermazenTransaction.getCurrent().querySimpleIndex(MeanPerson.class, "ratings.key", Person.class);
    }

    public static Index2<Float, MeanPerson, Person> queryRatingValueEntriesMean() {
        return PermazenTransaction.getCurrent().queryMapValueIndex(MeanPerson.class, "ratings.value", Float.class, Person.class);
    }

// Model Classes

    public enum Mood {
        MAD,
        NORMAL,
        HAPPY;
    }

    public interface HasFriend {

        @PermazenField(storageId = 110)
        Person getFriend();
        void setFriend(Person x);
    }

    @PermazenType(storageId = 100)
    public abstract static class Person implements HasFriend, PermazenObject {

        @PermazenField(storageId = 101)
        public abstract boolean getZ();
        public abstract void setZ(boolean value);

        @PermazenField(storageId = 102)
        public abstract byte getB();
        public abstract void setB(byte value);

        @PermazenField(storageId = 103)
        public abstract short getS();
        public abstract void setS(short value);

        @PermazenField(storageId = 104)
        public abstract char getC();
        public abstract void setC(char value);

        @PermazenField(storageId = 105)
        public abstract int getI();
        public abstract void setI(int value);

        @PermazenField(storageId = 106)
        public abstract float getF();
        public abstract void setF(float value);

        @PermazenField(storageId = 107)
        public abstract long getJ();
        public abstract void setJ(long value);

        @PermazenField(storageId = 108)
        public abstract double getD();
        public abstract void setD(double value);

        @PermazenField(storageId = 109)
        public abstract String getString();
        public abstract void setString(String value);

        @PermazenField(storageId = 111)
        public abstract boolean[] getBooleanArray();
        public abstract void setBooleanArray(boolean[] value);

        @PermazenField(storageId = 112, indexed = true)
        public abstract Mood getMood();
        public abstract void setMood(Mood mood);

        @PermazenSetField(storageId = 120, element = @PermazenField(storageId = 121, indexed = true))
        public abstract SortedSet<String> getNicknames();

        @PermazenListField(storageId = 130, element = @PermazenField(storageId = 131, encoding = "int", indexed = true))
        public abstract List<Integer> getScores();

        @PermazenMapField(storageId = 140,
          key = @PermazenField(storageId = 141, indexed = true),
          value = @PermazenField(storageId = 142, indexed = true))
        public abstract NavigableMap<Person, Float> getRatings();

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "@" + this.getObjId();
        }
    }

    @PermazenType(storageId = 200)
    public abstract static class MeanPerson extends Person {

        @PermazenListField(storageId = 150, element = @PermazenField(storageId = 151))
        public abstract List<Person> getEnemies();
    }
}
