
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedSet;

import org.jsimpledb.annotation.IndexQuery;
import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JListField;
import org.jsimpledb.annotation.JMapField;
import org.jsimpledb.annotation.JSetField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.DeletedObjectException;
import org.jsimpledb.core.ReferencedObjectException;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BasicTest extends TestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testBasicStuff() throws Exception {

        final JSimpleDB jdb = BasicTest.getJSimpleDB();

        final JTransaction tx = jdb.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(tx);
        try {

            final MeanPerson t1 = tx.create(MeanPerson.class);
            final Person t2 = tx.create(Person.class);
            final Person t3 = tx.create(Person.class);
            this.check(t1, false, (byte)0, (short)0, (char)0, 0, 0.0f, 0L, 0.0, null, null, null, null, null);

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
            t1.copyTo(tx, t1dup.getObjId());
            this.check(t1dup, true, (byte)123, (short)-32763, '!', -99, 12.34e-13f, Long.MAX_VALUE, Double.POSITIVE_INFINITY,
              "hey there", Mood.HAPPY, t1,
              Arrays.asList("apple", "banana", "dinkle"),
              Arrays.asList(23, 21, 22, 21),
              Arrays.asList(t3),
              t1, 100.0f, t2, -99.0f, null, -0.0f);
            t1dup.delete();

            t2.setBooleanArray(null);
            Assert.assertNull(t2.getBooleanArray());
            t2.setBooleanArray(new boolean[] { false, true, false });
            Assert.assertEquals(t2.getBooleanArray(), new boolean[] { false, true, false });

            // Check indexes
            final Indexer i = tx.create(Indexer.class);
            Assert.assertEquals(i.queryNicknames(), buildMap(
              "dinkle",     buildSet(t1),
              "banana",     buildSet(t1),
              "apple",      buildSet(t1)));
            Assert.assertEquals(i.queryHaters(), buildMap(
              t3,           buildSet(t1)));
            Assert.assertEquals(i.queryScores(), buildMap(
              21,           buildSet(t1),
              22,           buildSet(t1),
              23,           buildSet(t1),
              123,          buildSet(t2, t3),
              456,          buildSet(t2, t3),
              789,          buildSet(t3)), "ACTUAL = " + i.queryScores());
            Assert.assertEquals(i.queryScoreEntries(), buildMap(
              21,           buildSet(new ListIndexEntry<Person>(t1, 1), new ListIndexEntry<Person>(t1, 3)),
              22,           buildSet(new ListIndexEntry<Person>(t1, 2)),
              23,           buildSet(new ListIndexEntry<Person>(t1, 0)),
              123,          buildSet(new ListIndexEntry<Person>(t2, 0), new ListIndexEntry<Person>(t3, 2)),
              456,          buildSet(
                              new ListIndexEntry<Person>(t2, 1),
                              new ListIndexEntry<Person>(t3, 1),
                              new ListIndexEntry<Person>(t3, 3)),
              789,          buildSet(new ListIndexEntry<Person>(t3, 0))));
            Assert.assertEquals(i.queryRatingKeyEntries(), buildMap(
              t1,           buildSet(
                              new MapKeyIndexEntry<Person, Float>(t1, 100.0f),
                              new MapKeyIndexEntry<Person, Float>(t3, -3.0f)),
              t2,           buildSet(new MapKeyIndexEntry<Person, Float>(t1, -99.0f)),
              null,         buildSet(new MapKeyIndexEntry<Person, Float>(t1, -0.0f))));
            Assert.assertEquals(i.queryRatingValueEntries(), buildMap(
              100.0f,       buildSet(new MapValueIndexEntry<Person, Person>(t1, t1)),
              -99.0f,       buildSet(new MapValueIndexEntry<Person, Person>(t1, t2)),
              -0.0f,        buildSet(new MapValueIndexEntry<Person, Person>(t1, null)),
              -3.0f,        buildSet(new MapValueIndexEntry<Person, Person>(t3, t1))));
            Assert.assertEquals(i.queryMoods(), buildMap(
              Mood.HAPPY,   buildSet(t1),
              Mood.NORMAL,  buildSet(t3),
              null,         buildSet(t2)));

            // Check restricted indexes
            Assert.assertEquals(i.queryScoresMean(), buildMap(
              21,           buildSet(t1),
              22,           buildSet(t1),
              23,           buildSet(t1)), "ACTUAL = " + i.queryScores());
            Assert.assertEquals(i.queryScoreEntriesMean(), buildMap(
              21,           buildSet(new ListIndexEntry<Person>(t1, 1), new ListIndexEntry<Person>(t1, 3)),
              22,           buildSet(new ListIndexEntry<Person>(t1, 2)),
              23,           buildSet(new ListIndexEntry<Person>(t1, 0))));
            Assert.assertEquals(i.queryRatingKeyEntriesMean(), buildMap(
              t1,           buildSet(new MapKeyIndexEntry<Person, Float>(t1, 100.0f)),
              t2,           buildSet(new MapKeyIndexEntry<Person, Float>(t1, -99.0f)),
              null,         buildSet(new MapKeyIndexEntry<Person, Float>(t1, -0.0f))));
            Assert.assertEquals(i.queryRatingValueEntriesMean(), buildMap(
              100.0f,       buildSet(new MapValueIndexEntry<Person, Person>(t1, t1)),
              -99.0f,       buildSet(new MapValueIndexEntry<Person, Person>(t1, t2)),
              -0.0f,        buildSet(new MapValueIndexEntry<Person, Person>(t1, null))));
            Assert.assertEquals(i.queryMoodsMean(), buildMap(
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
            Assert.assertEquals(i.queryHaters(), buildMap());
            Assert.assertEquals(i.queryMoods(), buildMap(
              Mood.NORMAL,  buildSet(t3),
              null,         buildSet(t2)));

            try {
                t1.getScores();
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
            JTransaction.setCurrent(null);
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
        Assert.assertEquals(t.getRatings(), buildMap(ratingKeyValues));
    }

    public static JSimpleDB getJSimpleDB() {
        return BasicTest.getJSimpleDB(MeanPerson.class, Person.class, Indexer.class);
    }

    public static JSimpleDB getJSimpleDB(Class... classes) {
        return getJSimpleDB(Arrays.<Class<?>>asList(classes));
    }

    public static JSimpleDB getJSimpleDB(Iterable<Class<?>> classes) {
        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);
        return new JSimpleDB(db, 1, classes);
    }

// Model Classes

    public static enum Mood {
        MAD,
        NORMAL,
        HAPPY;
    }

    public interface HasFriend {

        @JField(storageId = 110)
        Person getFriend();
        void setFriend(Person x);
    }

    @JSimpleClass(storageId = 100)
    public abstract static class Person implements HasFriend, JObject {

        @JField(storageId = 101)
        public abstract boolean getZ();
        public abstract void setZ(boolean value);

        @JField(storageId = 102)
        public abstract byte getB();
        public abstract void setB(byte value);

        @JField(storageId = 103)
        public abstract short getS();
        public abstract void setS(short value);

        @JField(storageId = 104)
        public abstract char getC();
        public abstract void setC(char value);

        @JField(storageId = 105)
        public abstract int getI();
        public abstract void setI(int value);

        @JField(storageId = 106)
        public abstract float getF();
        public abstract void setF(float value);

        @JField(storageId = 107)
        public abstract long getJ();
        public abstract void setJ(long value);

        @JField(storageId = 108)
        public abstract double getD();
        public abstract void setD(double value);

        @JField(storageId = 109)
        public abstract String getString();
        public abstract void setString(String value);

        @JField(storageId = 111)
        public abstract boolean[] getBooleanArray();
        public abstract void setBooleanArray(boolean[] value);

        @JField(storageId = 112, indexed = true)
        public abstract Mood getMood();
        public abstract void setMood(Mood mood);

        @JSetField(storageId = 120, element = @JField(storageId = 121, indexed = true))
        public abstract SortedSet<String> getNicknames();

        @JListField(storageId = 130, element = @JField(storageId = 131, type = "int", indexed = true))
        public abstract List<Integer> getScores();

        @JMapField(storageId = 140,
          key = @JField(storageId = 141, indexed = true),
          value = @JField(storageId = 142, indexed = true))
        public abstract NavigableMap<Person, Float> getRatings();
    }

    @JSimpleClass(storageId = 200)
    public abstract static class MeanPerson extends Person {

        @JListField(storageId = 150, element = @JField(storageId = 151))
        public abstract List<Person> getEnemies();
    }

    @JSimpleClass(storageId = 300)
    public abstract static class Indexer {

        @IndexQuery(startType = Person.class, value = "nicknames.element")
        public abstract NavigableMap<String, NavigableSet<Person>> queryNicknames();

        @IndexQuery(startType = MeanPerson.class, value = "enemies.element")
        public abstract NavigableMap<Person, NavigableSet<MeanPerson>> queryHaters();

    // Person queries

        @IndexQuery(startType = Person.class, value = "mood")
        public abstract NavigableMap<Mood, NavigableSet<Person>> queryMoods();

        @IndexQuery(startType = Person.class, value = "scores.element")
        public abstract NavigableMap<Integer, NavigableSet<Person>> queryScores();

        @IndexQuery(startType = Person.class, value = "scores.element")
        public abstract NavigableMap<Integer, NavigableSet<ListIndexEntry<Person>>> queryScoreEntries();

        @IndexQuery(startType = Person.class, value = "ratings.key")
        public abstract NavigableMap<Person, NavigableSet<MapKeyIndexEntry<Person, Float>>> queryRatingKeyEntries();

        @IndexQuery(startType = Person.class, value = "ratings.value")
        public abstract NavigableMap<Float, NavigableSet<MapValueIndexEntry<Person, Person>>> queryRatingValueEntries();

    // MeanPerson queries

        @IndexQuery(startType = MeanPerson.class, value = "mood")
        public abstract NavigableMap<Mood, NavigableSet<MeanPerson>> queryMoodsMean();

        @IndexQuery(startType = MeanPerson.class, value = "scores.element")
        public abstract NavigableMap<Integer, NavigableSet<MeanPerson>> queryScoresMean();

        @IndexQuery(startType = MeanPerson.class, value = "scores.element")
        public abstract NavigableMap<Integer, NavigableSet<ListIndexEntry<MeanPerson>>> queryScoreEntriesMean();

        @IndexQuery(startType = MeanPerson.class, value = "ratings.key")
        public abstract NavigableMap<Person, NavigableSet<MapKeyIndexEntry<MeanPerson, Float>>> queryRatingKeyEntriesMean();

        @IndexQuery(startType = MeanPerson.class, value = "ratings.value")
        public abstract NavigableMap<Float, NavigableSet<MapValueIndexEntry<MeanPerson, Person>>> queryRatingValueEntriesMean();
    }
}

