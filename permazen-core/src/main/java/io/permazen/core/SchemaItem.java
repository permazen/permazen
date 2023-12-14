
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.schema.SchemaId;
import io.permazen.util.UnsignedIntEncoder;

/**
 * Superclass for the {@link ObjType}, {@link Field}, and {@link Index} classes which constitute a {@link Schema}.
 *
 * <p>
 * Instances have a {@link SchemaId} and a storage ID.
 * Instances also have a {@linkplain #getName name} which must be a valid Java identifier.
 * Instances are associated with a {@linkplain #getSchema specific} {@link Schema}.
 */
public abstract class SchemaItem {

    final Schema schema;
    final String name;
    final SchemaId schemaId;
    final int storageId;
    final int storageIdEncodedLength;

    SchemaItem(Schema schema, io.permazen.schema.SchemaItem item, String name) {
        this.schema = schema;
        this.name = name;
        this.schemaId = item.getSchemaId();
        this.storageId = this.schema.getSchemaBundle().getStorageId(this.schemaId);
        this.storageIdEncodedLength = UnsignedIntEncoder.encodeLength(this.storageId);

        // Notify schema bundle that we are a representative SchemaItem for our SchemaId.
        // We omit simple indexes because their associated fields represent them as well.
        if (!(this instanceof SimpleIndex))
            this.schema.getSchemaBundle().registerSchemaItemForSchemaId(this);
    }

    /**
     * Get the name associated with this instance.
     *
     * @return name of this object type or field, never null
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the storage ID associated with this instance.
     *
     * @return storage ID, always greater than zero
     */
    public int getStorageId() {
        return this.storageId;
    }

    /**
     * Get the schema ID associated with this instance.
     *
     * @return schema ID, never null
     */
    public SchemaId getSchemaId() {
        return this.schemaId;
    }

    /**
     * Get the schema with which this instance is associated.
     *
     * @return associated schema
     */
    public Schema getSchema() {
        return this.schema;
    }

    @Override
    public abstract String toString();
}
