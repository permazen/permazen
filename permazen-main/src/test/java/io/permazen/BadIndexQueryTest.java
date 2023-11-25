
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.PermazenType;
import io.permazen.test.TestSupport;
import io.permazen.util.NavigableSets;

import java.util.AbstractMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testng.annotations.Test;

public class BadIndexQueryTest extends TestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testWrongValueType() throws Exception {
        final Permazen jdb = BasicTest.getPermazen(DataFile.class, Analysis.class);
        final JTransaction jtx = jdb.createTransaction(ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {

            // Query with wrong value type
            try {
                jtx.queryIndex(DataFile.class, "state", Analysis.State.class);
                assert false : "expected exception here";
            } catch (IllegalArgumentException e) {
                this.log.debug("got expected {}", e.toString());
            }

            jtx.commit();
        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testQueryOnNonIndexedField() throws Exception {
        final Permazen jdb = BasicTest.getPermazen(DataFile.class, Analysis.class);
        final JTransaction jtx = jdb.createTransaction(ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {

            // Query on non-indexed field
            try {
                jtx.queryIndex(DataFile.class, "state", DataFile.State.class);
                assert false : "expected exception here";
            } catch (IllegalArgumentException e) {
                this.log.debug("got expected {}", e.toString());
            }

            jtx.commit();
        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWrongKeyType() throws Exception {
        final Permazen jdb = BasicTest.getPermazen(DataFile.class, Analysis.class);
        final JTransaction jtx = jdb.createTransaction(ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {

            final Analysis a = jtx.create(Analysis.class);
            a.setState(Analysis.State.AAA);

            // Wrong map key enum
            jtx.queryIndex(Analysis.class, "state", Analysis.State.class)
              .asMap().get(DataFile.State.XXX);

            // Wrong set enum
            jtx.queryIndex(Analysis.class, "state", Analysis.State.class)
              .asMap().keySet().contains(DataFile.State.XXX);

            // Wrong Map.Entry enum
            jtx.queryIndex(Analysis.class, "state", Analysis.State.class)
              .asMap().entrySet().contains(new AbstractMap.SimpleEntry<DataFile.State, Void>(DataFile.State.XXX, null));

            jtx.commit();
        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetInState() throws Exception {
        final Permazen jdb = BasicTest.getPermazen(DataFile.class, Analysis.class);
        final JTransaction jtx = jdb.createTransaction(ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {

            final Analysis a1 = jtx.create(Analysis.class);
            final Analysis a2 = jtx.create(Analysis.class);
            a1.setState(Analysis.State.AAA);
            a2.setState(Analysis.State.BBB);

            checkSet(Analysis.getInState(Analysis.State.AAA), buildSet(a1));
            checkSet(Analysis.getInState(Analysis.State.BBB), buildSet(a2));
            checkSet(Analysis.getInState(Analysis.State.AAA, Analysis.State.BBB), buildSet(a1, a2));

            jtx.commit();
        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public interface Analysis {

        @JField(indexed = true)
        State getState();
        void setState(State x);

        static NavigableSet<Analysis> getInState(State... states) {
            final NavigableMap<State, NavigableSet<Analysis>> indexMap
              = JTransaction.getCurrent().queryIndex(Analysis.class, "state", State.class).asMap();
            final List<NavigableSet<Analysis>> list = Stream.of(states)
              .map(indexMap::get)
              .map(set -> set != null ? set : NavigableSets.<Analysis>empty())
              .collect(Collectors.toList());
            return NavigableSets.union(list);
        }

        enum State {
            AAA,
            BBB;
        }
    }

    @PermazenType
    public interface DataFile {

        State getState();
        void setState(State x);

        enum State {
            XXX,
            YYY;
        }
    }
}
