
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.util;

import io.permazen.core.CoreAPITestSupport;
import io.permazen.core.ObjId;

import java.util.TreeSet;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ObjIdBiMultiMapTest extends CoreAPITestSupport {

    public static final int NUM_IDS = 200;
    public static final int NUM_ITERATIONS = 5000;
    public static final int NUM_CHOICES = 41;

    @Test
    public void testObjIdBiMultiMap() throws Exception {

        ObjIdBiMultiMap map = new ObjIdBiMultiMap();

        final ObjIdSet sources = new ObjIdSet();
        final ObjIdSet targets = new ObjIdSet();

        TreeSet<Edge> edges = new TreeSet<>();

        for (int i = 0; i < NUM_IDS; i++) {
            final ObjId source = new ObjId(0x0100000000000000L | this.random.nextInt(NUM_IDS));
            final ObjId target = new ObjId(0x0100000000000000L | this.random.nextInt(NUM_IDS));
            sources.add(source);
            targets.add(target);
            if (this.random.nextBoolean()) {
                edges.add(new Edge(source, target));
                map.add(source, target);
            }
        }

        this.checkBoth(map, edges, sources, targets);

        for (int i = 0; i < NUM_ITERATIONS; i++) {
            final ObjId source = new ObjId(0x0100000000000000L | this.random.nextInt(NUM_IDS));
            final ObjId target = new ObjId(0x0100000000000000L | this.random.nextInt(NUM_IDS));
            final int choice = this.random.nextInt(NUM_CHOICES);
            boolean actual = false;
            boolean expected = false;
            final String before = edges.toString();
            if (choice < 10) {
                actual = map.add(source, target);
                expected = edges.add(new Edge(source, target));
            } else if (choice < 20) {
                actual = map.remove(source, target);
                expected = edges.remove(new Edge(source, target));
            } else if (choice < 30) {
                ObjIdSet set = map.getSources(source);
                if (set == null) {
                    set = new ObjIdSet();
                    set.add(target);
                }
                actual = map.addAll(source, set);
                expected = edges.addAll(set.stream()
                  .map(t -> new Edge(source, t))
                  .collect(Collectors.toList()));
            } else if (choice < 40) {
                ObjIdSet set = map.getSources(source);
                if (set == null) {
                    set = new ObjIdSet();
                    set.add(target);
                }
                actual = map.removeAll(source, set);
                expected = edges.removeAll(set.stream()
                  .map(t -> new Edge(source, t))
                  .collect(Collectors.toList()));
            } else {
                map.clear();
                edges.clear();
            }
            Assert.assertEquals(map, this.buildMap(edges), "unequal after iteration " + i + " using choice "
              + choice + ", source=" + source + ", target=" + target + ", before=" + before);
            Assert.assertEquals(actual, expected, "wrong result " + actual + " when choice=" + choice
              + "\n*** MAP: " + map + "\n*** EDGES: " + edges + "\n*** EQUAL: " + map.equals(this.buildMap(edges)));
            if (this.random.nextInt(10) == 7) {
                map = map.inverse();
                edges = invert(edges);
            }
        }
    }

    private ObjIdBiMultiMap buildMap(Iterable<? extends Edge> edges) {
        final ObjIdBiMultiMap map = new ObjIdBiMultiMap();
        for (Edge edge : edges)
            map.add(edge.getSource(), edge.getTarget());
        return map;
    }

    private void checkBoth(ObjIdBiMultiMap map, TreeSet<Edge> edges, ObjIdSet sources, ObjIdSet targets) {
        this.check(map, edges, sources, targets);
        this.check(map.inverse(), this.invert(edges), targets, sources);
    }

    private void check(ObjIdBiMultiMap map, TreeSet<Edge> edges, ObjIdSet sources, ObjIdSet targets) {

        int numSources = 0;
        for (ObjId source : sources) {
            if (map.containsSource(source)) {
                numSources++;
                final ObjIdSet sourceTargets = map.getTargets(source);
                Assert.assertNotNull(sourceTargets);
                for (ObjId target : targets) {
                    final ObjIdSet targetSources = map.getSources(target);
                    if (sourceTargets.contains(target)) {
                        Assert.assertNotNull(targetSources);
                        Assert.assertTrue(targetSources.contains(source));
                    } else
                        Assert.assertTrue(targetSources == null || !targetSources.contains(source));
                }
            } else {
                Assert.assertNull(map.getTargets(source));
                for (ObjId target : targets) {
                    final ObjIdSet targetSources = map.getSources(target);
                    if (targetSources != null)
                        Assert.assertFalse(targetSources.contains(source));
                }
            }
        }
        Assert.assertEquals(numSources, map.getNumSources());

        final ObjIdSet seenSources = new ObjIdSet();
        final ObjIdSet seenTargets = new ObjIdSet();
        for (ObjId source : map.getSources()) {
            seenSources.add(source);
            for (ObjId target : map.getTargets(source)) {
                Assert.assertTrue(edges.contains(new Edge(source, target)));
                seenTargets.add(target);
            }
        }
        Assert.assertEquals(seenSources.size(), map.getNumSources());
        Assert.assertEquals(seenTargets.size(), map.getNumTargets());
    }

    private TreeSet<Edge> invert(TreeSet<Edge> edges) {
        final TreeSet<Edge> inverse = edges.stream()
          .map(edge -> new Edge(edge.getTarget(), edge.getSource()))
          .collect(Collectors.toCollection(TreeSet::new));
        return inverse;
    }

// Edge

    public static class Edge implements Comparable<Edge> {

        private final ObjId source;
        private final ObjId target;

        public Edge(ObjId source, ObjId target) {
            this.source = source;
            this.target = target;
        }

        public ObjId getSource() {
            return this.source;
        }

        public ObjId getTarget() {
            return this.target;
        }

        @Override
        public int hashCode() {
            return this.source.hashCode() * 31 + this.target.hashCode();
        }

        @Override
        public String toString() {
            return this.source + "->" + this.target;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final Edge that = (Edge)obj;
            return this.source.equals(that.source) && this.target.equals(that.target);
        }

        @Override
        public int compareTo(Edge that) {
            int diff = this.source.compareTo(that.source);
            if (diff != 0)
                return diff;
            return this.target.compareTo(that.target);
        }
    }
}

