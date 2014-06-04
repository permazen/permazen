
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.NavigableSet;

/**
 * Listener interface for notifications of a change in value of a {@link SimpleField}.
 *
 * @see Transaction#addSimpleFieldChangeListener Transaction.addSimpleFieldChangeListener()
 */
public interface SimpleFieldChangeListener {

    /**
     * Receive notification of a change in the value of a {@link SimpleField} in an object.
     *
     * <p>
     * Notifications are only delivered when the set of referring objects is non-empty.
     * </p>
     *
     * <p>
     * Notifications are delivered in the same thread that made the change, before the outermost mutation operation returns.
     * </p>
     *
     * @param tx associated transaction
     * @param id the ID of the affected object (i.e., the object containing the field that changed)
     * @param field the field that changed
     * @param path path of reference fields (represented by storage IDs) that lead to {@code id}
     * @param referrers all objects that (indirectly) refer to the affected object via the {@code path}
     * @param oldValue the field's previous value
     * @param newValue the field's new value
     * @param <V> Java type for {@code field}'s values
     */
    <V> void onSimpleFieldChange(Transaction tx, ObjId id, SimpleField<V> field,
      int[] path, NavigableSet<ObjId> referrers, V oldValue, V newValue);
}

