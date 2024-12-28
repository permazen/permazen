
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.schema.SchemaId;
import io.permazen.util.ByteData;
import io.permazen.util.UnsignedIntEncoder;

import java.util.Optional;

/**
 * Per-object meta data encoded in the object's KV value.
 *
 * <p>
 * Instances are immutable. If an object's meta-data is updated (via {@link #write write()},
 * then a new instance should be created.
 */
class ObjInfo {

    // Stored meta-data
    final Transaction tx;
    final ObjId id;
    final int schemaIndex;

    // Additional derived meta-data
    Schema schema;
    ObjType objType;

    // Constructor that reads from key/value store
    ObjInfo(Transaction tx, ObjId id) {
        assert tx != null;
        assert id != null;
        this.tx = tx;
        this.id = id;
        final ByteData value = tx.kvt.get(this.id.getBytes());
        if (value == null)
            throw new DeletedObjectException(tx, this.id);
        final ByteData.Reader reader = value.newReader();
        this.schemaIndex = UnsignedIntEncoder.read(reader);
        if (this.schemaIndex == 0)
            throw new InvalidObjectVersionException(this.id, this.schemaIndex, null);
        final int flags = reader.remain() > 0 ? reader.readByte() : 0;
        if ((flags & ~Layout.OBJECT_FLAGS_VALID_BITS) != 0) {
            throw new InconsistentDatabaseException(String.format(
              "object meta-data in %s has invalid flags byte 0x%02x", this.id, flags));
        }
        if (reader.remain() > 0)
            throw new InconsistentDatabaseException(String.format("object meta-data in %s has trailing garbage", this.id));
    }

    // Constructor for explicitly given data
    ObjInfo(Transaction tx, ObjId id, int schemaIndex, Schema schema, ObjType objType) {
        assert tx != null;
        assert id != null;
        this.tx = tx;
        this.id = id;
        this.schemaIndex = schemaIndex;
        this.schema = schema;
        this.objType = objType;
    }

    public ObjId getId() {
        return this.id;
    }

    public int getSchemaIndex() {
        return this.schemaIndex;
    }

    public SchemaId getSchemaId() {
        return this.getSchema().getSchemaId();
    }

    public Schema getSchema() {
        if (this.schema == null) {
            try {
                this.schema = this.tx.getSchemaBundle().getSchema(this.schemaIndex);
            } catch (IllegalArgumentException e) {
                throw new InvalidObjectVersionException(id, this.schemaIndex, e);
            }
        }
        return this.schema;
    }

    public ObjType getObjType() {
        if (this.objType == null) {
            try {
                this.objType = this.getSchema().getObjType(this.id.getStorageId());
            } catch (IllegalArgumentException e) {
                throw new InconsistentDatabaseException(String.format("object %s has invalid storage ID", this.id), e);
            }
        }
        return this.objType;
    }

    public static void write(Transaction tx, ObjId id, int schemaIndex) {
        assert schemaIndex > 0;
        final ByteData.Writer writer = ByteData.newWriter(UnsignedIntEncoder.encodeLength(schemaIndex));
        Encodings.UNSIGNED_INT.write(writer, schemaIndex);
        //writer.write(flags);                              // we can omit a zero flags byte
        tx.kvt.put(id.getBytes(), writer.toByteData());
    }

    @Override
    public String toString() {
        return "ObjInfo"
          + "[id=" + this.id
          + ",schemaIndex=" + this.schemaIndex
          + ",schema=" + this.schema
          + ",objType=" + Optional.ofNullable(this.objType).map(ObjType::getName).orElse(null)
          + "]";
    }
}
