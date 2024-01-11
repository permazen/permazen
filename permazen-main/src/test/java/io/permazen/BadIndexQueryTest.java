
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.util.NavigableSets;

import java.util.AbstractMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testng.annotations.Test;

public class BadIndexQueryTest extends MainTestSupport {

    @Test
    @SuppressWarnings("unchecked")
    public void testWrongValueType() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(DataFile.class, Analysis.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(ptx);
        try {

            // Query with wrong value type
            try {
                ptx.querySimpleIndex(DataFile.class, "state", Analysis.State.class);
                assert false : "expected exception here";
            } catch (IllegalArgumentException e) {
                this.log.debug("got expected {}", e.toString());
            }

            ptx.commit();
        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testQueryOnNonIndexedField() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(DataFile.class, Analysis.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(ptx);
        try {

            // Query on non-indexed field
            try {
                ptx.querySimpleIndex(DataFile.class, "state", DataFile.State.class);
                assert false : "expected exception here";
            } catch (IllegalArgumentException e) {
                this.log.debug("got expected {}", e.toString());
            }

            ptx.commit();
        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWrongKeyType() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(DataFile.class, Analysis.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(ptx);
        try {

            final Analysis a = ptx.create(Analysis.class);
            a.setState(Analysis.State.AAA);

            // Wrong map key enum
            ptx.querySimpleIndex(Analysis.class, "state", Analysis.State.class)
              .asMap().get(DataFile.State.XXX);

            // Wrong set enum
            ptx.querySimpleIndex(Analysis.class, "state", Analysis.State.class)
              .asMap().keySet().contains(DataFile.State.XXX);

            // Wrong Map.Entry enum
            ptx.querySimpleIndex(Analysis.class, "state", Analysis.State.class)
              .asMap().entrySet().contains(new AbstractMap.SimpleEntry<DataFile.State, Void>(DataFile.State.XXX, null));

            ptx.commit();
        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetInState() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(DataFile.class, Analysis.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(ptx);
        try {

            final Analysis a1 = ptx.create(Analysis.class);
            final Analysis a2 = ptx.create(Analysis.class);
            a1.setState(Analysis.State.AAA);
            a2.setState(Analysis.State.BBB);

            checkSet(Analysis.getInState(Analysis.State.AAA), buildSet(a1));
            checkSet(Analysis.getInState(Analysis.State.BBB), buildSet(a2));
            checkSet(Analysis.getInState(Analysis.State.AAA, Analysis.State.BBB), buildSet(a1, a2));

            ptx.commit();
        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public interface Analysis {

        @PermazenField(indexed = true)
        State getState();
        void setState(State x);

        static NavigableSet<Analysis> getInState(State... states) {
            final NavigableMap<State, NavigableSet<Analysis>> indexMap
              = PermazenTransaction.getCurrent().querySimpleIndex(Analysis.class, "state", State.class).asMap();
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
