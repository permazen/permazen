
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.core.ObjId;
import io.permazen.core.ReferenceField;

/**
 * Information about an assigment of a deleted object to a reference field configured to disallow such assignments.
 */
class DeletedAssignment {

    private final ObjId id;                                 // referring object
    private final ReferenceField field;                     // field in referring object

    /**
     * Constructor.
     *
     * @param id the ID of the referring object
     * @param field the field which was assigned
     */
    DeletedAssignment(ObjId id, ReferenceField field) {
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(field != null, "null field");
        this.id = id;
        this.field = field;
    }

    /**
     * Get the ID of the referring object.
     *
     * @return the ID of the referring object
     */
    public ObjId getId() {
        return this.id;
    }

    /**
     * Get the reference field which was assigned.
     *
     * @return the reference field which was assigned.
     */
    public ReferenceField getField() {
        return this.field;
    }
}
