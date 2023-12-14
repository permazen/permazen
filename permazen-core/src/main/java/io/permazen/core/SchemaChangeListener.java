
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.schema.SchemaId;

import java.util.Map;

/**
 * Listener interface for notifications that an object's schema has been changed to match the current transaction.
 *
 * @see Transaction#addSchemaChangeListener Transaction.addSchemaChangeListener()
 */
@FunctionalInterface
public interface SchemaChangeListener {

    /**
     * Receive notification of an object schema change.
     *
     * <p>
     * Notifications are delivered in the same thread that first reads the object, before the operation
     * that triggered the schema change returns.
     *
     * @param tx associated transaction
     * @param id the ID of the updated object
     * @param oldVersion previous schema ID
     * @param newVersion new schema ID
     * @param oldFieldValues read-only mapping of the values of all fields in the old schema keyed by name
     */
    void onSchemaChange(Transaction tx, ObjId id, SchemaId oldVersion, SchemaId newVersion, Map<String, Object> oldFieldValues);
}
