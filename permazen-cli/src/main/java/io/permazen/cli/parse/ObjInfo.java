
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.parse;

import io.permazen.cli.Session;
import io.permazen.core.DeletedObjectException;
import io.permazen.core.ObjId;
import io.permazen.core.ObjType;
import io.permazen.core.Schema;
import io.permazen.core.Transaction;

/**
 * Utility class holding meta-data about a database object.
 */
public class ObjInfo {

    private final ObjId id;
    private final ObjType type;
    private final Schema schema;

    /**
     * Constructor.
     *
     * @param tx database transaction
     * @param id the ID of the object to query
     * @throws DeletedObjectException if object does not exist
     */
    public ObjInfo(Transaction tx, ObjId id) {
        this.id = id;
        this.type = tx.getObjType(id);
        this.schema = this.type.getSchema();
    }

    public ObjId getObjId() {
        return this.id;
    }

    public ObjType getObjType() {
        return this.type;
    }

    public Schema getSchema() {
        return this.schema;
    }

    @Override
    public String toString() {
        return String.format("%s type %s#%d (schema \"%s\")",
          this.id, this.type.getName(), this.type.getStorageId(), this.schema.getSchemaId());
    }

    /**
     * Get object meta-data.
     *
     * @param session parse session
     * @param id the ID of the object to query
     * @return object info, or null if object doesn't exist
     */
    public static ObjInfo getObjInfo(Session session, ObjId id) {
        try {
            return new ObjInfo(session.getTransaction(), id);
        } catch (DeletedObjectException e) {
            return null;
        }
    }
}
