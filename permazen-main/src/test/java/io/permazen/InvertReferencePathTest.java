
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenType;
import io.permazen.test.TestSupport;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.NavigableSet;
import java.util.stream.Stream;

import org.testng.annotations.Test;

public class InvertReferencePathTest extends TestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testInvertReferencePath() throws Exception {

        final Permazen jdb = BasicTest.getPermazen();

        final JTransaction tx = jdb.createTransaction(ValidationMode.MANUAL);
        JTransaction.setCurrent(tx);
        try {

            BasicTest.Person p1 = tx.create(BasicTest.Person.class);
            BasicTest.Person p2 = tx.create(BasicTest.Person.class);
            BasicTest.Person p3 = tx.create(BasicTest.Person.class);
            BasicTest.MeanPerson m1 = tx.create(BasicTest.MeanPerson.class);
            BasicTest.MeanPerson m2 = tx.create(BasicTest.MeanPerson.class);

            m1.getRatings().put(p1, 1.23f);
            m1.getRatings().put(m2, 4.56f);
            m2.getRatings().put(p3, 7.89f);
            m2.getEnemies().add(m1);
            m2.getEnemies().add(p1);
            m2.getEnemies().add(p2);
            p1.setFriend(p3);
            p2.setFriend(p1);
            p3.setFriend(p3);

            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "->ratings.key",
              Arrays.asList(p1)),            buildSet(m1));
            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "->ratings.key",
              Arrays.asList(p1, m2)),        buildSet(m1));
            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "->ratings.key",
              Arrays.asList(p1, m2, p3)),    buildSet(m1, m2));

            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "->ratings.key->enemies",
              Arrays.asList(m2, p3)),       buildSet());
            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "->ratings.key->enemies",
              Arrays.asList(m1, p1, p2)),   buildSet(m1));

            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "->ratings.key->enemies->friend",
              Arrays.asList(p1)),           buildSet(m1));
            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "->ratings.key->enemies->friend",
              Arrays.asList(p2)),           buildSet());
            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "->ratings.key->enemies->friend",
              Arrays.asList(p3)),           buildSet(m1));

            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "->ratings.key->enemies->friend->friend",
              Arrays.asList(p1)),           buildSet());
            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "->ratings.key->enemies->friend->friend",
              Arrays.asList(p2)),           buildSet());
            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "->ratings.key->enemies->friend->friend",
              Arrays.asList(p3)),           buildSet(m1));

            // Illegal paths
            for (String path : new String[] { "->ratings", "->string", "->ratings.key->string" }) {
                try {
                    this.invertRefPath(tx, BasicTest.MeanPerson.class, path, Collections.<JObject>emptySet());
                    assert false : "path \"" + path + "\" should be invalid";
                } catch (IllegalArgumentException e) {
                    // expected
                }
            }

            tx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInvertReferencePath2() throws Exception {

        final Permazen jdb = BasicTest.getPermazen(A.class, B.class, C.class);
        final JTransaction jtx = jdb.createTransaction(ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {

            A a = jtx.create(A.class);

            B b = jtx.create(B.class);
            C c = jtx.create(C.class);

            b.setA(a);
            c.setA(a);

            final ReferencePath inverseAny = jdb.parseReferencePath(Object.class, "<-" + HasA.class.getName() + ".a");
            final ReferencePath inverseB = jdb.parseReferencePath(A.class, "<-B.a");
            final ReferencePath inverseC = jdb.parseReferencePath(A.class, "<-C.a");

            TestSupport.checkSet(jtx.followReferencePath(inverseAny, Stream.of(a)), buildSet(b, c));
            TestSupport.checkSet(jtx.followReferencePath(inverseB, Stream.of(a)), buildSet(b));
            TestSupport.checkSet(jtx.followReferencePath(inverseC, Stream.of(a)), buildSet(c));

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    private NavigableSet<JObject> invertRefPath(JTransaction jtx,
      Class<?> startType, String path, Collection<? extends JObject> objs) {
        final ReferencePath refPath = jtx.getPermazen().parseReferencePath(startType, path);
        return jtx.invertReferencePath(refPath, objs.stream());
    }

// Model Classes

    public interface HasA {
        A getA();
        void setA(A a);
    }

    @PermazenType
    public abstract static class A implements JObject {
    }

    @PermazenType
    public abstract static class B implements JObject, HasA {
    }

    @PermazenType
    public abstract static class C implements JObject, HasA {
    }
}
