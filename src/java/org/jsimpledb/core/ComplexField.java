
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeToken;

import java.util.Iterator;
import java.util.List;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * A complex {@link Field}, such as a collection or map field.
 *
 * @param <T> Java type for the field's values
 */
public abstract class ComplexField<T> extends Field<T> {

    private final int storageIdLength;

    /**
     * Constructor.
     *
     * @param name the name of the field
     * @param storageId field content storage ID
     * @param version schema version
     * @param typeToken Java type for the field's values
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    ComplexField(String name, int storageId, SchemaVersion version, TypeToken<T> typeToken) {
        super(name, storageId, version, typeToken);
        this.storageIdLength = UnsignedIntEncoder.encodeLength(storageId);
    }

    /**
     * Get the sub-field(s) associated with this instance, ordered according to their meaning.
     *
     * @return unmodifiable list of simple fields
     */
    public abstract List<? extends SimpleField<?>> getSubFields();

    /**
     * Get the Java value of this field in the given object.
     *
     * @param tx transaction
     * @param id object id
     */
    public abstract T getValue(Transaction tx, ObjId id);

    abstract ComplexFieldStorageInfo toStorageInfo();

    /**
     * Determine whether an index on the given sub-field will have complex values or just plain object IDs.
     */
    abstract boolean hasComplexIndex(SimpleField<?> subField);

    /**
     * Check compatibility with another {@link ComplexField} across a schema change.
     * To be compatible, the two enties must be exactly the same in terms of data layouts and Java representations.
     * This is in effect an {@code #equals equals()} test with respect to those aspects. Note that compatibililty
     * does not necessarily imply the same sub-fields are indexed.
     *
     * <p>
     * The implementation in {@link ComplexField} checks that {@code that} has the same Java type and as this instance
     * and that all sub-fields are themselves {@link SimpleField#isSchemaChangeCompatible schema change compatible}.
     * </p>
     *
     * @param that field to check for compatibility
     * @throws NullPointerException if {@code that} is null
     */
    boolean isSchemaChangeCompatible(ComplexField<?> that) {
        if (that.getClass() != this.getClass() || that.storageId != this.storageId)
            return false;
        final List<? extends SimpleField<?>> thisSubFields = this.getSubFields();
        final List<? extends SimpleField<?>> thatSubFields = that.getSubFields();
        if (thisSubFields.size() != thatSubFields.size())
            return false;
        for (int i = 0; i < thisSubFields.size(); i++) {
            if (!thisSubFields.get(i).isSchemaChangeCompatible(thatSubFields.get(i)))
                return false;
        }
        return true;
    }

    @Override
    boolean isEquivalent(Field<?> field) {
        if (field.getClass() != this.getClass())
            return false;
        final ComplexField<?> that = (ComplexField<?>)field;
        final List<? extends SimpleField<?>> thisSubFields = this.getSubFields();
        final List<? extends SimpleField<?>> thatSubFields = that.getSubFields();
        if (thisSubFields.size() != thatSubFields.size())
            return false;
        for (int i = 0; i < thisSubFields.size(); i++) {
            if (!thisSubFields.get(i).isEquivalent(thatSubFields.get(i)))
                return false;
        }
        return true;
    }

    /**
     * Delete all content (but not index entries) for the given object.
     *
     * @param tx transaction
     * @param id object id
     */
    void deleteContent(Transaction tx, ObjId id) {
        final byte[] minKey = this.buildKey(id);
        final byte[] maxKey = ByteUtil.getKeyAfterPrefix(minKey);
        this.deleteContent(tx, minKey, maxKey);
    }

    /**
     * Delete all content (but not index entries) for the given object in the given key range
     *
     * @param tx transaction
     * @param id object id
     */
    void deleteContent(Transaction tx, byte[] minKey, byte[] maxKey) {
        tx.kvt.removeRange(minKey, maxKey);
    }

    /**
     * Add an index entry corresponding to the given sub-field and content key/value pair.
     *
     * @param tx transaction
     * @param id object id
     * @param subField indexed sub-field
     * @param contentKey the content key
     * @param contentValue the value associated with the content key, or null if not needed
     */
    void addIndexEntry(Transaction tx, ObjId id, SimpleField<?> subField, byte[] contentKey, byte[] contentValue) {
        tx.kvt.put(this.buildIndexEntry(id, subField, contentKey, contentValue), ByteUtil.EMPTY);
    }

    /**
     * Remove an index entry corresponding to the given sub-field and content key/value pair.
     *
     * @param tx transaction
     * @param id object id
     * @param subField indexed sub-field
     * @param contentKey the content key
     * @param contentValue the value associated with the content key, or null if not needed
     */
    void removeIndexEntry(Transaction tx, ObjId id, SimpleField<?> subField, byte[] contentKey, byte[] contentValue) {
        tx.kvt.remove(this.buildIndexEntry(id, subField, contentKey, contentValue));
    }

    private byte[] buildIndexEntry(ObjId id, SimpleField<?> subField, byte[] contentKey, byte[] contentValue) {
        final ByteReader contentKeyReader = new ByteReader(contentKey);
        contentKeyReader.skip(ObjId.NUM_BYTES + this.storageIdLength);                  // skip to content
        final ByteWriter writer = new ByteWriter();
        UnsignedIntEncoder.write(writer, subField.storageId);
        this.buildIndexEntry(id, subField, contentKeyReader, contentValue, writer);
        return writer.getBytes();
    }

    /**
     * Build an index key for the given object, sub-field, and content key/value pair.
     *
     * @param id object id
     * @param subField indexed sub-field
     * @param reader reader of content key, positioned just after the object ID and the storage ID for this field
     * @param value the value associated with the content key, or null if not needed
     * @param writer writer for the index entry, with the sub-field's storage ID already written
     */
    abstract void buildIndexEntry(ObjId id, SimpleField<?> subField, ByteReader reader, byte[] value, ByteWriter writer);

    /**
     * Add or remove index entries for the given object as appropriate after a schema version change
     * which changed only whether some or all sub-field(s) are indexed.
     *
     * @param kvt KV store
     * @param oldField compatible field in older schema
     * @param id object id
     */
    void updateSubFieldIndexes(Transaction tx, ComplexField<?> oldField, ObjId id) {
        final Iterator<? extends SimpleField<?>> oldSubFields = oldField.getSubFields().iterator();
        final Iterator<? extends SimpleField<?>> newSubFields = this.getSubFields().iterator();
        while (oldSubFields.hasNext() || newSubFields.hasNext()) {
            final SimpleField<?> oldSubField = oldSubFields.next();
            final SimpleField<?> newSubField = newSubFields.next();
            if (!oldSubField.indexed && newSubField.indexed)
                this.addIndexEntries(tx, id, newSubField);
            else if (oldSubField.indexed && !newSubField.indexed)
                oldField.removeIndexEntries(tx, id, oldSubField);
        }
    }

    /**
     * Add all index entries for the given object and sub-field.
     *
     * @param tx transaction
     * @param id object id
     * @param subField sub-field of this field
     */
    void addIndexEntries(Transaction tx, ObjId id, SimpleField<?> subField) {
        if (!subField.indexed)
            throw new IllegalArgumentException(this + " is not indexed");
        final byte[] prefix = this.buildKey(id);
        final byte[] prefixEnd = ByteUtil.getKeyAfterPrefix(prefix);
        for (Iterator<KVPair> i = tx.kvt.getRange(prefix, prefixEnd, false); i.hasNext(); ) {
            final KVPair pair = i.next();
            this.addIndexEntry(tx, id, subField, pair.getKey(), pair.getValue());
        }
    }

    /**
     * Remove all index entries for the given object and sub-field.
     *
     * @param tx transaction
     * @param id object id
     * @param subField sub-field of this field
     */
    void removeIndexEntries(Transaction tx, ObjId id, SimpleField<?> subField) {
        final byte[] prefix = this.buildKey(id);
        this.removeIndexEntries(tx, id, subField, prefix, ByteUtil.getKeyAfterPrefix(prefix));
    }

    /**
     * Remove index entries for the given object and sub-field, restricted to the given key range.
     *
     * @param tx transaction
     * @param id object id
     * @param subField sub-field of this field
     */
    void removeIndexEntries(Transaction tx, ObjId id, SimpleField<?> subField, byte[] minKey, byte[] maxKey) {
        if (!subField.indexed)
            throw new IllegalArgumentException(this + " is not indexed");
        for (Iterator<KVPair> i = tx.kvt.getRange(minKey, maxKey, false); i.hasNext(); ) {
            final KVPair pair = i.next();
            this.removeIndexEntry(tx, id, subField, pair.getKey(), pair.getValue());
        }
    }
}

