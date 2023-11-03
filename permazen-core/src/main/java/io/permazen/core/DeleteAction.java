
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.schema.ReferenceSchemaField;

/**
 * Describes what action to take when an object that is still referred to by some other object is deleted.
 *
 * @see Transaction#delete Transaction.delete()
 * @see ReferenceSchemaField#getInverseDelete
 */
public enum DeleteAction {

    /**
     * Do nothing. The reference will still exist, but subsequent attempts to access the fields of the deleted object
     * will result in {@link DeletedObjectException}.
     */
    IGNORE,

    /**
     * Disallow deleting the object, instead throwing {@link ReferencedObjectException}. This is the default if not specified.
     *
     * <p>
     * Note: deleting an object that is only referred to by itself will not cause any exception to be thrown.
     */
    EXCEPTION,

    /**
     * Remove the reference, either by setting it to null (in the case of a {@link SimpleField} in an object),
     * or by removing the corresponding collection element or map entry (in the case of a {@link ComplexField} sub-field).
     */
    UNREFERENCE,

    /**
     * Also delete the object containing the reference. This action will be repeated recursively, if necessary.
     */
    DELETE;
}
