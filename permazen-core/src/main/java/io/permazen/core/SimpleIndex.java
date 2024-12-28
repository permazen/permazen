
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.encoding.Encoding;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVTransaction;
import io.permazen.schema.SimpleSchemaField;
import io.permazen.util.ByteData;

import java.util.Collections;
import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * An index on a simple field, either a regular simple field or a sub-field of a complex field.
 *
 * @param <T> field's value type
 */
public abstract class SimpleIndex<T> extends Index {

// Constructor

    SimpleIndex(Schema schema, SimpleSchemaField schemaField, String name, ObjType objType, SimpleField<T> field) {
        super(schema, schemaField, name, objType, Collections.singleton(field));
    }

// Public methods

    /**
     * Get the indexed field.
     *
     * @return the indexed field
     */
    @SuppressWarnings("unchecked")
    public SimpleField<T> getField() {
        return (SimpleField<T>)this.fields.get(0);
    }

    /**
     * Get the indexed field's encoding.
     *
     * @return the indexed field's encoding
     */
    @SuppressWarnings("unchecked")
    public Encoding<T> getEncoding() {
        return (Encoding<T>)this.getEncodings().get(0);
    }

    @Override
    public abstract CoreIndex1<T, ObjId> getIndex(Transaction tx);

    /**
     * Get the key in the underlying key/value store corresponding to the given value in this index.
     *
     * <p>
     * The returned key will be the prefix of all index entries with the given value over all objects.
     *
     * @param value indexed value
     * @return the corresponding {@link KVDatabase} key
     * @see KVTransaction#watchKey KVTransaction.watchKey()
     */
    public ByteData getKey(T value) {
        return this.getKey(new Object[] { value });
    }

    /**
     * Get the key in the underlying key/value store corresponding to the given value and target object
     * in this index.
     *
     * @param id target object ID
     * @param value indexed value
     * @return the corresponding {@link KVDatabase} key
     * @see KVTransaction#watchKey KVTransaction.watchKey()
     */
    public ByteData getKey(ObjId id, T value) {
        return this.getKey(id, new Object[] { value });
    }

// Package methods

    /**
     * Nullify or remove all references from objects in the specified referrers set to the specified target through
     * the reference field associated with this instance.
     *
     * <p>
     * Used to implement {@link DeleteAction#NULLIFY} and {@link DeleteAction#REMOVE}.
     *
     * <p>
     * This method may assume that this instance's {@link Encoding} is reference.
     *
     * @param tx transaction
     * @param remove true to remove entries in complex sub-fields, false to just nullify references
     * @param target referenced object being deleted
     * @param referrers objects that refer to {@code target} via this reference field
     */
    abstract void unreferenceAll(Transaction tx, boolean remove, ObjId target, NavigableSet<ObjId> referrers);

    /**
     * Read this field from the given object and add non-null value(s) to the given set.
     *
     * @param tx transaction
     * @param id object being accessed
     * @param values read values
     * @param filter optional filter to apply
     */
    abstract void readAllNonNull(Transaction tx, ObjId id, Set<T> values, Predicate<? super T> filter);
}
