
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

/**
 * The {@link FieldType} for {@link ReferenceField}s.
 *
 * <p>
 * Null values are supported by this class.
 * </p>
 */
public class ReferenceFieldType extends NullSafeType<ObjId> {

    /**
     * Constructor.
     */
    public ReferenceFieldType() {
        super(FieldType.REFERENCE_TYPE_NAME, new ObjIdType());
    }
}

