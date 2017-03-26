
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.util.Arrays;
import java.util.Collections;
import java.util.NavigableSet;

import org.jsimpledb.test.TestSupport;
import org.testng.annotations.Test;

public class InvertReferencePathTest extends TestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testInvertReferencePath() throws Exception {

        final JSimpleDB jdb = BasicTest.getJSimpleDB();

        final JTransaction tx = jdb.createTransaction(true, ValidationMode.MANUAL);
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

            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "ratings.key",
              Arrays.asList(p1)),            buildSet(m1));
            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "ratings.key",
              Arrays.asList(p1, m2)),        buildSet(m1));
            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "ratings.key",
              Arrays.asList(p1, m2, p3)),    buildSet(m1, m2));

            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "ratings.key.enemies.element",
              Arrays.asList(m2, p3)),       buildSet());
            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "ratings.key.enemies.element",
              Arrays.asList(m1, p1, p2)),   buildSet(m1));

            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "ratings.key.enemies.element.friend",
              Arrays.asList(p1)),           buildSet(m1));
            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "ratings.key.enemies.element.friend",
              Arrays.asList(p2)),           buildSet());
            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "ratings.key.enemies.element.friend",
              Arrays.asList(p3)),           buildSet(m1));

            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "ratings.key.enemies.element.friend.friend",
              Arrays.asList(p1)),           buildSet());
            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "ratings.key.enemies.element.friend.friend",
              Arrays.asList(p2)),           buildSet());
            TestSupport.checkSet(this.invertRefPath(tx, BasicTest.MeanPerson.class, "ratings.key.enemies.element.friend.friend",
              Arrays.asList(p3)),           buildSet(m1));

            // Illegal paths
            for (String path : new String[] { "ratings", "string", "ratings.key.string" }) {
                try {
                    this.invertRefPath(tx, BasicTest.MeanPerson.class, path, Collections.<JObject>emptySet());
                    assert false : "path `" + path + "' should be invalid";
                } catch (IllegalArgumentException e) {
                    // expected
                }
            }

            tx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    private NavigableSet<JObject> invertRefPath(JTransaction jtx,
      Class<?> startType, String path, Iterable<? extends JObject> objs) {
        final ReferencePath refPath = jtx.getJSimpleDB().parseReferencePath(startType, path, false);
        return jtx.invertReferencePath(refPath, objs);
    }
}

