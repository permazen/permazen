
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.NavigableSet;

/**
 * Listener interface for notifications of a change in value of a {@link SetField}.
 *
 * @see Transaction#addSetFieldChangeListener Transaction.addSetFieldChangeListener()
 */
public interface SetFieldChangeListener {

    /**
     * Receive notification of the addition of a new element to a {@link SetField}.
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
     * @param referrers all objects that (indirectly) refer to the affected object via {@code path}
     * @param value the value added to the set
     * @param <E> Java type for {@code field}'s elements
     */
    <E> void onSetFieldAdd(Transaction tx, ObjId id, SetField<E> field, int[] path, NavigableSet<ObjId> referrers, E value);

    /**
     * Receive notification of the removal of an element from a {@link SetField}.
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
     * @param referrers all objects that (indirectly) refer to the affected object via {@code path}
     * @param value the value removed from the set
     * @param <E> Java type for {@code field}'s elements
     */
    <E> void onSetFieldRemove(Transaction tx, ObjId id, SetField<E> field, int[] path, NavigableSet<ObjId> referrers, E value);

    /**
     * Receive notification of the clearing of a {@link SetField}.
     *
     * <p>
     * This method is only used when the whole set is cleared; when a range restricted subset is cleared, individual
     * {@linkplain #onSetFieldRemove onSetFieldRemove()} invocations for each removed entry are made instead.
     * </p>
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
     * @param id the ID of the affected object (i.e., the object containing the field that was cleared)
     * @param field the field that changed
     * @param path path of reference fields (represented by storage IDs) that lead to {@code id}
     * @param referrers all objects that (indirectly) refer to the affected object via {@code path}
     */
    void onSetFieldClear(Transaction tx, ObjId id, SetField<?> field, int[] path, NavigableSet<ObjId> referrers);
}

