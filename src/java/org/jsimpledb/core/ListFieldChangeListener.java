
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import java.util.NavigableSet;

/**
 * Listener interface for notifications of a change in value of a {@link ListField}.
 *
 * @see Transaction#addListFieldChangeListener Transaction.addListFieldChangeListener()
 */
public interface ListFieldChangeListener {

    /**
     * Receive notification of the addition (or insertion) of an element to a {@link ListField}.
     *
     * <p>
     * Notifications are only delivered when {@code referrers} is non-empty.
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
     * @param index the position in the list at which {@code value} was added or inserted
     * @param value the value added to the list
     * @param <E> Java type for {@code field}'s elements
     */
    <E> void onListFieldAdd(Transaction tx, ObjId id, ListField<E> field,
      int[] path, NavigableSet<ObjId> referrers, int index, E value);

    /**
     * Receive notification of a removal of an element from a {@link ListField}.
     *
     * <p>
     * Notifications are only delivered when {@code referrers} is non-empty.
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
     * @param index the position in the list at which {@code value} was removed
     * @param value the value removed from the list
     * @param <E> Java type for {@code field}'s elements
     */
    <E> void onListFieldRemove(Transaction tx, ObjId id, ListField<E> field,
      int[] path, NavigableSet<ObjId> referrers, int index, E value);

    /**
     * Receive notification of the change in element at a specific position in a {@link ListField}.
     *
     * <p>
     * Notifications are only delivered when {@code referrers} is non-empty.
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
     * @param index the position in the list at which the change occurred
     * @param oldValue the old value at list position {@code index}
     * @param newValue the new value at list position {@code index}
     * @param <E> Java type for {@code field}'s elements
     */
    <E> void onListFieldReplace(Transaction tx, ObjId id, ListField<E> field,
      int[] path, NavigableSet<ObjId> referrers, int index, E oldValue, E newValue);

    /**
     * Receive notification of the clearing of a {@link ListField}.
     *
     * <p>
     * This method is only used when the whole list is cleared; when a sub-range of a list is cleared, individual
     * {@linkplain #onListFieldRemove onListFieldRemove()} invocations for each removed element are made instead.
     * </p>
     *
     * <p>
     * Notifications are only delivered when {@code referrers} is non-empty.
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
    void onListFieldClear(Transaction tx, ObjId id, ListField<?> field, int[] path, NavigableSet<ObjId> referrers);
}

