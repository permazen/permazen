
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.core.util.ObjIdMap;
import io.permazen.encoding.Encoding;
import io.permazen.schema.SimpleSchemaField;
import io.permazen.util.ByteData;

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

/**
 * A simple {@link Field}.
 *
 * <p>
 * {@link SimpleField}s have these requirements and properties:
 * <ul>
 *  <li>They have an associated {@link Encoding} representing the domain of possible values</li>
 *  <li>Theys can serve as the element sub-field for {@link SetField}s and {@link ListField}s,
 *      and the key and value sub-fields for {@link MapField}s.</li>
 *  <li>They can be indexed.</li>
 * </ul>
 *
 * @param <T> Java type for the field's values
 */
public class SimpleField<T> extends Field<T> {

    final Encoding<T> encoding;
    final boolean indexed;

    // Parent field if sub-field
    ComplexField<?> parent;

    // The simple index on this field, if any
    SimpleIndex<T> index;

    // Maps composite index to this field's offset in index field list, or null if none
    HashMap<CompositeIndex, Integer> compositeIndexMap;

    SimpleField(ObjType objType, SimpleSchemaField field, Encoding<T> encoding, boolean indexed) {
        super(objType, field, encoding.getTypeToken());
        this.encoding = encoding;
        this.indexed = indexed;
    }

// Public methods

    /**
     * Get the {@link Encoding} associated with this field.
     *
     * @return this field's type
     */
    public Encoding<T> getEncoding() {
        return this.encoding;
    }

    /**
     * Determine whether this field is indexed.
     *
     * @return true if this field is indexed
     */
    public boolean isIndexed() {
        return this.indexed;
    }

    /**
     * Get the parent field if this field is a sub-field of a complex field.
     *
     * @return parent field, or null if this is not a sub-field
     */
    public ComplexField<?> getParentField() {
        return this.parent;
    }

    @Override
    public String getFullName() {
        return this.parent != null ? this.parent.name + "." + this.name : this.name;
    }

    /**
     * Get the {@link SimpleIndex} on this field.
     *
     * @return the index on this field
     * @throws UnknownIndexException if there is no simple index on this field
     */
    public SimpleIndex<T> getIndex() {
        if (this.index != null)
            return this.index;
        throw new UnknownIndexException(name, String.format("there is no index on field \"%s\"", this.getName()));
    }

    /**
     * Get the {@link CompositeIndex}s that contain this field, if any.
     *
     * @return zero or more {@link CompositeIndex} that contain this field
     */
    public Set<CompositeIndex> getCompositeIndexes() {
        return this.compositeIndexMap != null ?
          Collections.unmodifiableSet(this.compositeIndexMap.keySet()) : Collections.emptySet();
    }

    /**
     * Set the value of this field in the given object.
     * Does not alter the schema of the object.
     *
     * @param tx transaction
     * @param id object id
     * @param value new value
     * @throws DeletedObjectException if no object with ID equal to {@code id} is found
     * @throws StaleTransactionException if this transaction is no longer usable
     * @throws IllegalArgumentException if {@code tx} or {@code id} is null
     * @throws IllegalArgumentException if this field is a sub-field of a {@link ComplexField}
     */
    public void setValue(Transaction tx, ObjId id, T value) {
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(this.parent == null, "field is a sub-field of " + this.parent);
        tx.writeSimpleField(id, this.name, value, false);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T getValue(Transaction tx, ObjId id) {
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(this.parent == null, "field is a sub-field of " + this.parent);
        return (T)tx.readSimpleField(id, this.name, false);
    }

    @Override
    public boolean hasDefaultValue(Transaction tx, ObjId id) {
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(this.parent == null, "field is a sub-field of " + this.parent);
        return tx.hasDefaultValue(id, this);
    }

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.caseSimpleField(this);
    }

    @Override
    public String toString() {
        return String.format(
          "%sfield \"%s\" of type %s", this.indexed ? "indexed " : "", this.getFullName(), this.encoding.getTypeToken());
    }

// Non-public methods

    @Override
    @SuppressWarnings("unchecked")
    void copy(ObjId srcId, ObjId dstId, Transaction srcTx, Transaction dstTx, ObjIdMap<ObjId> objectIdMap) {
        T value = (T)srcTx.readSimpleField(srcId, this.name, false);
        if (objectIdMap != null)
            value = this.remapObjectId(objectIdMap, value);
        dstTx.writeSimpleField(dstId, this.name, value, false);
    }

    protected boolean remapsObjectId() {
        return false;
    }

    protected T remapObjectId(ObjIdMap<ObjId> objectIdMap, T value) {
        return value;
    }

    /**
     * Encode the given value.
     *
     * @param obj value to encode
     * @return encoded value, or null if value is the default value
     * @throws IllegalArgumentException if {@code obj} cannot be encoded
     */
    ByteData encode(Object obj) {
        final ByteData.Writer writer = ByteData.newWriter();
        try {
            this.encoding.validateAndWrite(writer, obj);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("can't set %s to value %s: %s", this, obj, e.getMessage()), e);
        }
        final ByteData result = writer.toByteData();
        return result.equals(this.encoding.getDefaultValueBytes()) ? null : result;
    }
}
