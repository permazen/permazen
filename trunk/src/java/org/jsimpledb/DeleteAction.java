
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

/**
 * Describes what action to take when an object that is still referred to by
 * one or more other objects is deleted.
 */
public enum DeleteAction {

    /**
     * Do nothing. Subsequent attempts to access the deleted object will result in {@link DeletedObjectException}.
     */
    NOTHING,

    /**
     * Disallow deleting the object, instead throwing {@link ReferencedObjectException}.
     *
     * <p>
     * Note: deleting an object that is only referred to by itself will not cause any exception to be thrown.
     * </p>
     */
    EXCEPTION,

    /**
     * Remove the reference, either by setting it to null (in the case of a {@link SimpleField} in an object),
     * or by removing the corresponding collection element or key/value pair (in the case of a {@link ComplexField} sub-field).
     */
    UNREFERENCE,

    /**
     * Also delete the object containing the reference. This action will be repeated recursively, if necessary.
     */
    DELETE;
}

