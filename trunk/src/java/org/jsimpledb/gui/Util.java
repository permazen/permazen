
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import java.util.SortedMap;
import java.util.TreeMap;

import org.jsimpledb.JClass;
import org.jsimpledb.JField;

/**
 * Utility routines.
 */
final class Util {

    private Util() {
    }

    /**
     * Get the {@link JField}s that are common to all of the given types.
     *
     * @param jclasses types to inspect
     * @return map containing common {@link JField}s, or null if {@code jclasses} is empty
     */
    static SortedMap<Integer, JField> getCommonJFields(Iterable<? extends JClass<?>> jclasses) {
        if (jclasses == null)
            throw new IllegalArgumentException("null jclasses");
        TreeMap<Integer, JField> jfields = null;
        for (JClass<?> jclass : jclasses) {
            if (jfields == null)
                jfields = new TreeMap<Integer, JField>(jclass.getJFieldsByStorageId());
            else
                jfields.keySet().retainAll(jclass.getJFieldsByStorageId().keySet());
        }
        return jfields;
    }
}

