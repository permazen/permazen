
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.UnsignedIntEncoder;

/**
 * Per-object meta data encoded in the object's KV value.
 */
class ObjInfo {

    private static final int META_DATA_VERSION = 1;

    // Stored meta-data
    final Transaction tx;
    final ObjId id;
    final int version;
    final boolean deleteNotified;

    // Additional derived meta-data
    Schema schema;
    ObjType objType;

    // Constructor that reads from key/value store
    ObjInfo(Transaction tx, ObjId id) {
        assert tx != null;
        assert id != null;
        this.tx = tx;
        this.id = id;
        final byte[] value = tx.kvt.get(this.id.getBytes());
        if (value == null)
            throw new DeletedObjectException(tx, this.id);
        final ByteReader reader = new ByteReader(value);
        final int metaDataVersion = UnsignedIntEncoder.read(reader);
        if (metaDataVersion != META_DATA_VERSION)
            throw new InconsistentDatabaseException("found unknown object meta-data version " + metaDataVersion + " in " + this.id);
        this.version = UnsignedIntEncoder.read(reader);
        if (this.version == 0)
            throw new InvalidObjectVersionException(this.id, this.version);
        this.deleteNotified = FieldTypeRegistry.BOOLEAN.read(reader);
    }

    // Constructor for explicitly given data
    ObjInfo(Transaction tx, ObjId id, int version, boolean deleteNotified, Schema schema, ObjType objType) {
        assert tx != null;
        assert id != null;
        this.tx = tx;
        this.id = id;
        this.version = version;
        this.deleteNotified = deleteNotified;
        this.schema = schema;
        this.objType = objType;
    }

    public ObjId getId() {
        return this.id;
    }

    public int getVersion() {
        return this.version;
    }

    public boolean isDeleteNotified() {
        return this.deleteNotified;
    }

    public Schema getSchema() {
        if (this.schema == null) {
            try {
                this.schema = this.tx.schemas.getVersion(this.version);
            } catch (IllegalArgumentException e) {
                throw new InvalidObjectVersionException(id, this.version);
            }
        }
        return this.schema;
    }

    public ObjType getObjType() {
        if (this.objType == null) {
            try {
                this.objType = this.getSchema().getObjType(this.id.getStorageId());
            } catch (IllegalArgumentException e) {
                throw new InconsistentDatabaseException("object " + this.id + " has invalid storage ID", e);
            }
        }
        return this.objType;
    }

    public static void write(Transaction tx, ObjId id, int version, boolean deleteNotified) {
        final ByteWriter writer = new ByteWriter();
        UnsignedIntEncoder.write(writer, META_DATA_VERSION);
        UnsignedIntEncoder.write(writer, version);
        FieldTypeRegistry.BOOLEAN.write(writer, deleteNotified);
        tx.kvt.put(id.getBytes(), writer.getBytes());
    }

    @Override
    public String toString() {
        return "ObjInfo"
          + "[id=" + this.id
          + ",version=" + this.version
          + (this.deleteNotified ? ",deleteNotified" : "")
          + ",schema=" + this.schema
          + ",objType=" + this.objType
          + "]";
    }
}

