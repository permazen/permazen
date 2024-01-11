
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin;

import com.google.common.base.Preconditions;

import io.permazen.PermazenClass;
import io.permazen.PermazenField;

import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Utility routines.
 */
final class Util {

    private Util() {
    }

    /**
     * Get the {@link PermazenField}s that are common to all of the given types.
     *
     * @param jclasses types to inspect
     * @return map containing common {@link PermazenField}s, or null if {@code jclasses} is empty
     */
    static SortedMap<Integer, PermazenField> getCommonJFields(Iterable<? extends PermazenClass<?>> jclasses) {
        Preconditions.checkArgument(jclasses != null, "null jclasses");
        TreeMap<Integer, PermazenField> jfields = null;
        for (PermazenClass<?> jclass : jclasses) {     // TODO: keep only fields with the same name; prefer indexed (sub-)fields
            if (jfields == null)
                jfields = new TreeMap<>(jclass.getFieldsByStorageId());
            else
                jfields.keySet().retainAll(jclass.getFieldsByStorageId().keySet());
        }
        return jfields;
    }
}
