
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import org.jsimpledb.core.ObjId;

/**
 * Thrown when attempting to get the {@link JObject} associated with an {@link ObjId} that
 * has an unrecognized type (i.e., storage ID).
 *
 * <p>
 * This exception should not occur during normal usage. It may occur when using both the higher-level {@link JSimpleDB} Java model
 * object API and the lower level core database API at the same time in an incompatible way.
 * </p>
 *
 * @see JSimpleDB#getJObject
 */
@SuppressWarnings("serial")
public class UnknownTypeException extends JSimpleDBException {

    private final ObjId id;
    private final int version;

    /**
     * Constructor.
     */
    public UnknownTypeException(ObjId id, int version) {
        this(id, version, "no model class with storage ID " + id.getStorageId() + " exists in database schema version " + version);
    }

    /**
     * Constructor.
     */
    public UnknownTypeException(ObjId id, int version, String message) {
        super(message);
        this.id = id;
        this.version = version;
    }

    /**
     * Get the {@link ObjId} that has the unrecognized type.
     */
    public ObjId getObjId() {
        return this.id;
    }

    /**
     * Get the {@link JSimpleDB}'s schema version, which did not contain the unrecognized type.
     */
    public int getSchemaVersion() {
        return this.version;
    }
}

