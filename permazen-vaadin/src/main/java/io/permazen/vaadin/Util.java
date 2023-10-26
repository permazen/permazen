
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin;

import com.google.common.base.Preconditions;

import io.permazen.JClass;
import io.permazen.JField;

import java.util.SortedMap;
import java.util.TreeMap;

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
                jfields = new TreeMap<>(jclass.getJFieldsByStorageId());
            else
                jfields.keySet().retainAll(jclass.getJFieldsByStorageId().keySet());
        }
        return jfields;
    }
}
