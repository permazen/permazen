
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.schema.ReferenceSchemaField;

/**
 * Describes what action to take when an object is deleted and a reference to that object from some other object still exists.
 *
 * @see Transaction#delete Transaction.delete()
 * @see ReferenceSchemaField#getInverseDelete
 */
public enum DeleteAction {

    /**
     * Do nothing, thereby creating a dangling reference.
     *
     * <p>
     * The reference will still exist, but subsequent attempts to access any field in the deleted object
     * will result in a {@link DeletedObjectException}.
     */
    IGNORE,

    /**
     * Throw a {@link ReferencedObjectException}, thereby preventing the delete operation from happening.
     *
     * <p>
     * This is the default if not specified.
     *
     * <p>
     * Note: deleting an object that is only referred to by itself will not cause any exception to be thrown.
     */
    EXCEPTION,

    /**
     * Set the reference to null.
     *
     * <p>
     * When the reference field is a sub-field of a complex field, what that means depends on the type of collection:
     * <ul>
     *  <li>If the reference field is a set element field, the reference will be removed from the set and
     *      null will be added to the set if not already present.
     *  <li>If the reference field is a list element field, the corresponding list entry will be set to null.
     *      Note this can happen to multiple entries in the same list.
     *  <li>If the reference field is a map key, the entry will be removed from the, map and an entry with null key
     *      and the original entry's same value will be inserted into the map, replacing any previous null key entry.
     *  <li>If the reference field is a map value, the map entry remains but its value will be set to null.
     *      Note this can happen to multiple entries in the same map.
     * </ul>
     *
     * <p>
     * To always just remove the entry for complex sub-fields, use {@link #REMOVE} instead.
     */
    NULLIFY,

    /**
     * Remove the corresponding list element, set element, or map entry.
     *
     * <p>
     * This setting is only valid for complex sub-fields.
     */
    REMOVE,

    /**
     * Also delete the object containing the reference.
     *
     * <p>
     * This action will be repeated recursively, if necessary. Any reference loops are properly handled.
     */
    DELETE;
}
