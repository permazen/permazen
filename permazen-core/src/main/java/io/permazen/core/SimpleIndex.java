
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.encoding.Encoding;
import io.permazen.schema.SimpleSchemaField;

import java.util.Collections;
import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Represents an index on a single simple field, either a regular simple field or a sub-field of a complex field.
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
    public Encoding<T> getEncoding() {
        return this.getField().getEncoding();
    }

    @Override
    public abstract CoreIndex<T, ObjId> getIndex(Transaction tx);

// Package methods

    /**
     * Remove all references from objects in the specified referrers set to the specified target through
     * the reference field associated with this instance. Used to implement {@link DeleteAction#UNREFERENCE}.
     *
     * <p>
     * This method may assume that this instance's {@link Encoding} is reference.
     *
     * @param tx transaction
     * @param target referenced object being deleted
     * @param referrers objects that refer to {@code target} via this reference field
     */
    abstract void unreferenceAll(Transaction tx, ObjId target, NavigableSet<ObjId> referrers);

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
