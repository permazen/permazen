
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.gui;

import com.google.common.base.Preconditions;

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
        Preconditions.checkArgument(jclasses != null, "null jclasses");
        TreeMap<Integer, JField> jfields = null;
        for (JClass<?> jclass : jclasses) {     // TODO: keep only fields with the same name; prefer indexed (sub-)fields
            if (jfields == null)
                jfields = new TreeMap<Integer, JField>(jclass.getJFieldsByStorageId());
            else
                jfields.keySet().retainAll(jclass.getJFieldsByStorageId().keySet());
        }
        return jfields;
    }
}

