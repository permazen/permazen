
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.kv.KVPair;
import io.permazen.kv.KeyRange;
import io.permazen.schema.ComplexSchemaField;
import io.permazen.schema.SimpleSchemaField;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.UnsignedIntEncoder;

import java.util.List;
import java.util.Set;

/**
 * A complex {@link Field}, such as a collection or map field.
 *
 * @param <T> Java type for the field's values
 */
public abstract class ComplexField<T> extends Field<T> {

    private final int storageIdLength;

    ComplexField(ObjType objType, ComplexSchemaField schemaField, TypeToken<T> typeToken) {
        super(objType, schemaField, typeToken);
        this.storageIdLength = UnsignedIntEncoder.encodeLength(storageId);
    }

// Public methods

    /**
     * Get the sub-field(s) associated with this instance, ordered according to their meaning.
     *
     * @return unmodifiable list of simple fields
     */
    public abstract List<? extends SimpleField<?>> getSubFields();

// Non-public methods

    /**
     * Create a sub-field index.
     */
    abstract ComplexSubFieldIndex<T, ?> createSubFieldIndex(Schema schema,
      SimpleSchemaField schemaField, ObjType objType, SimpleField<?> field);

    /**
     * Get the Java collection object representing the value of this instance in the given object.
     * This method does not need to do any validity checking of its parameters.
     */
    abstract T getValueInternal(Transaction tx, ObjId id);

    /**
     * Copy the Java collection object representing the value of this instance into memory and return a read-only view.
     * This method does not need to do any validity checking of its parameters.
     */
    abstract T getValueReadOnlyCopy(Transaction tx, ObjId id);

    /**
     * Iterate all values in the specified subfield.
     *
     * @throws IllegalArgumentException if {@code subField} is not a sub-field of this instance
     */
    abstract <F> Iterable<F> iterateSubField(Transaction tx, ObjId id, SimpleField<F> subField);

    /**
     * Delete all content (but not index entries) for the given object.
     *
     * @param tx transaction
     * @param id object id
     */
    void deleteContent(Transaction tx, ObjId id) {
        final ByteData minKey = this.buildKey(id);
        final ByteData maxKey = ByteUtil.getKeyAfterPrefix(minKey);
        this.deleteContent(tx, minKey, maxKey);
    }

    /**
     * Delete all content (but not index entries) for the given object in the given key range.
     *
     * @param tx transaction
     * @param minKey minimum key
     * @param maxKey maximum key
     * @see #removeIndexEntries(Transaction, ObjId)
     */
    void deleteContent(Transaction tx, ByteData minKey, ByteData maxKey) {
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
    void addIndexEntry(Transaction tx, ObjId id, SimpleField<?> subField, ByteData contentKey, ByteData contentValue) {
        Preconditions.checkArgument(subField.indexed, "not indexed");
        tx.kvt.put(this.buildIndexEntry(id, subField, contentKey, contentValue), ByteData.empty());
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
    void removeIndexEntry(Transaction tx, ObjId id, SimpleField<?> subField, ByteData contentKey, ByteData contentValue) {
        Preconditions.checkArgument(subField.indexed, "not indexed");
        tx.kvt.remove(this.buildIndexEntry(id, subField, contentKey, contentValue));
    }

    private ByteData buildIndexEntry(ObjId id, SimpleField<?> subField, ByteData contentKey, ByteData contentValue) {
        final ByteData.Writer writer = ByteData.newWriter();
        UnsignedIntEncoder.write(writer, subField.storageId);
        final ByteData content = contentKey.substring(ObjId.NUM_BYTES + this.storageIdLength);
        this.buildIndexEntry(id, subField, content, contentValue, writer);
        return writer.toByteData();
    }

    /**
     * Build an index key for the given object, sub-field, and content key/value pair.
     *
     * @param id object id
     * @param subField indexed sub-field
     * @param content content key data, which lives just after the object ID and the storage ID for this field
     * @param value the value associated with the content key, or null if not needed
     * @param writer writer for the index entry, with the sub-field's storage ID already written
     */
    abstract void buildIndexEntry(ObjId id, SimpleField<?> subField,
      ByteData content, ByteData value, ByteData.Writer writer);

    /**
     * Add all index entries for the given object and sub-field.
     *
     * @param tx transaction
     * @param id object id
     * @param subField sub-field of this field
     */
    void addIndexEntries(Transaction tx, ObjId id, SimpleField<?> subField) {
        Preconditions.checkArgument(subField.indexed, "not indexed");
        final ByteData prefix = this.buildKey(id);
        final ByteData prefixEnd = ByteUtil.getKeyAfterPrefix(prefix);
        try (CloseableIterator<KVPair> i = tx.kvt.getRange(prefix, prefixEnd)) {
            while (i.hasNext()) {
                final KVPair pair = i.next();
                assert KeyRange.forPrefix(prefix).contains(pair.getKey());
                this.addIndexEntry(tx, id, subField, pair.getKey(), pair.getValue());
            }
        }
    }

    /**
     * Remove all index entries for the given object.
     *
     * @param tx transaction
     * @param id object id
     */
    void removeIndexEntries(Transaction tx, ObjId id) {
        for (SimpleField<?> subField : this.getSubFields()) {
            if (subField.indexed)
                this.removeIndexEntries(tx, id, subField);
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
        final ByteData prefix = this.buildKey(id);
        this.removeIndexEntries(tx, id, subField, prefix, ByteUtil.getKeyAfterPrefix(prefix));
    }

    /**
     * Remove index entries for the given object and sub-field, restricted to the given key range.
     *
     * @param tx transaction
     * @param id object id
     * @param subField sub-field of this field
     */
    void removeIndexEntries(Transaction tx, ObjId id, SimpleField<?> subField, ByteData minKey, ByteData maxKey) {
        Preconditions.checkArgument(subField.indexed, "not indexed");
        try (CloseableIterator<KVPair> i = tx.kvt.getRange(minKey, maxKey)) {
            while (i.hasNext()) {
                final KVPair pair = i.next();
                assert new KeyRange(minKey, maxKey).contains(pair.getKey());
                this.removeIndexEntry(tx, id, subField, pair.getKey(), pair.getValue());
            }
        }
    }

    /**
     * Remove all field entries in which the specified reference sub-field refers to an object
     * type that is in the specified set of newly disallowed object types.
     */
    abstract void unreferenceRemovedTypes(Transaction tx, ObjId id, ReferenceField subField, Set<Integer> removedStorageIds);
}
