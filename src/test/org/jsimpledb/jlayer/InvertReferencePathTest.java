
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer;

import java.util.Arrays;
import java.util.Collections;

import org.jsimpledb.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class InvertReferencePathTest extends TestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testInvertReferencePath() throws Exception {

        final JLayer jlayer = JLayerTest.getJLayer();

        final JTransaction tx = jlayer.createTransaction(true, ValidationMode.MANUAL);
        JTransaction.setCurrent(tx);
        try {

            JLayerTest.Person p1 = tx.create(JLayerTest.Person.class);
            JLayerTest.Person p2 = tx.create(JLayerTest.Person.class);
            JLayerTest.Person p3 = tx.create(JLayerTest.Person.class);
            JLayerTest.MeanPerson m1 = tx.create(JLayerTest.MeanPerson.class);
            JLayerTest.MeanPerson m2 = tx.create(JLayerTest.MeanPerson.class);

            m1.getRatings().put(p1, 1.23f);
            m1.getRatings().put(m2, 4.56f);
            m2.getRatings().put(p3, 7.89f);
            m2.getEnemies().add(m1);
            m2.getEnemies().add(p1);
            m2.getEnemies().add(p2);
            p1.setFriend(p3);
            p2.setFriend(p1);
            p3.setFriend(p3);

            Assert.assertEquals(tx.invertReferencePath(JLayerTest.MeanPerson.class, "ratings.key",
              Arrays.asList(p1)),            buildSet(m1));
            Assert.assertEquals(tx.invertReferencePath(JLayerTest.MeanPerson.class, "ratings.key",
              Arrays.asList(p1, m2)),        buildSet(m1));
            Assert.assertEquals(tx.invertReferencePath(JLayerTest.MeanPerson.class, "ratings.key",
              Arrays.asList(p1, m2, p3)),    buildSet(m1, m2));

            Assert.assertEquals(tx.invertReferencePath(JLayerTest.MeanPerson.class, "ratings.key.enemies.element",
              Arrays.asList(m2, p3)),       buildSet());
            Assert.assertEquals(tx.invertReferencePath(JLayerTest.MeanPerson.class, "ratings.key.enemies.element",
              Arrays.asList(m1, p1, p2)),   buildSet(m1));

            Assert.assertEquals(tx.invertReferencePath(JLayerTest.MeanPerson.class, "ratings.key.enemies.element.friend",
              Arrays.asList(p1)),           buildSet(m1));
            Assert.assertEquals(tx.invertReferencePath(JLayerTest.MeanPerson.class, "ratings.key.enemies.element.friend",
              Arrays.asList(p2)),           buildSet());
            Assert.assertEquals(tx.invertReferencePath(JLayerTest.MeanPerson.class, "ratings.key.enemies.element.friend",
              Arrays.asList(p3)),           buildSet(m1));

            Assert.assertEquals(tx.invertReferencePath(JLayerTest.MeanPerson.class, "ratings.key.enemies.element.friend.friend",
              Arrays.asList(p1)),           buildSet());
            Assert.assertEquals(tx.invertReferencePath(JLayerTest.MeanPerson.class, "ratings.key.enemies.element.friend.friend",
              Arrays.asList(p2)),           buildSet());
            Assert.assertEquals(tx.invertReferencePath(JLayerTest.MeanPerson.class, "ratings.key.enemies.element.friend.friend",
              Arrays.asList(p3)),           buildSet(m1));

            // Illegal paths
            for (String path : new String[] { "ratings", "string", "ratings.key.string" }) {
                try {
                    tx.invertReferencePath(JLayerTest.MeanPerson.class, path, Collections.<JObject>emptySet());
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
}

