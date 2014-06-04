
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.NavigableSet;

/**
 * Listener interface for notifications of a change in value of a {@link MapField}.
 *
 * @see Transaction#addMapFieldChangeListener Transaction.addMapFieldChangeListener()
 */
public interface MapFieldChangeListener {

    /**
     * Receive notification of the addition of a new key/value pair into a {@link MapField}.
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
     * @param key the new map entry's key
     * @param value the new map entry's value
     * @param <K> Java type for {@code field}'s keys
     * @param <V> Java type for {@code field}'s values
     */
    <K, V> void onMapFieldAdd(Transaction tx, ObjId id, MapField<K, V> field,
      int[] path, NavigableSet<ObjId> referrers, K key, V value);

    /**
     * Receive notification of the removal of an entry from a {@link MapField}.
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
     * @param key the removed map entry's key
     * @param value the removed map entry's value
     * @param <K> Java type for {@code field}'s keys
     * @param <V> Java type for {@code field}'s values
     */
    <K, V> void onMapFieldRemove(Transaction tx, ObjId id, MapField<K, V> field,
      int[] path, NavigableSet<ObjId> referrers, K key, V value);

    /**
     * Receive notification of the change in value of an existing entry in a {@link MapField}.
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
     * @param key the map entry key
     * @param oldValue the previous value associated with {@code key}
     * @param newValue the new value associated with {@code key}
     * @param <K> Java type for {@code field}'s keys
     * @param <V> Java type for {@code field}'s values
     */
    <K, V> void onMapFieldReplace(Transaction tx, ObjId id, MapField<K, V> field,
      int[] path, NavigableSet<ObjId> referrers, K key, V oldValue, V newValue);

    /**
     * Receive notification of the clearing of a {@link MapField}.
     *
     * <p>
     * This method is only used when the whole map is cleared; when a range restricted submap is cleared, individual
     * {@linkplain #onMapFieldRemove onMapFieldRemove()} invocations for each removed entry are made instead.
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
    void onMapFieldClear(Transaction tx, ObjId id, MapField<?, ?> field, int[] path, NavigableSet<ObjId> referrers);
}

