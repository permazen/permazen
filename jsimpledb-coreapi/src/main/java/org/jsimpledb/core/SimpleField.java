
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Map;

import org.jsimpledb.util.ByteWriter;

/**
 * A simple {@link Field}.
 *
 * <p>
 * {@link SimpleField}s have these requirements and properties:
 * <ul>
 *  <li>They have an associated {@link FieldType} representing the domain of possible values</li>
 *  <li>Theys can serve as the element sub-field for {@link SetField}s and {@link ListField}s,
 *      and the key and value sub-fields for {@link MapField}s.</li>
 *  <li>They can be indexed.</li>
 * </ul>
 *
 * @param <T> Java type for the field's values
 */
public class SimpleField<T> extends Field<T> {

    final FieldType<T> fieldType;
    final boolean indexed;

    ComplexField<?> parent;
    Map<CompositeIndex, Integer> compositeIndexMap;         // maps index to this field's offset in field list

    /**
     * Constructor.
     *
     * @param name the name of the field
     * @param storageId field storage ID
     * @param schema schema version
     * @param fieldType field type
     * @param indexed whether this field is indexed
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     * @throws IllegalArgumentException if {@code storageId} is zero or less
     */
    SimpleField(String name, int storageId, Schema schema, FieldType<T> fieldType, boolean indexed) {
        super(name, storageId, schema, fieldType.getTypeToken());
        this.fieldType = fieldType;
        this.indexed = indexed;
    }

// Public methods

    /**
     * Get the {@link FieldType} associated with this field.
     *
     * @return this field's type
     */
    public FieldType<T> getFieldType() {
        return this.fieldType;
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
     * Set the value of this field in the given object.
     * Does not alter the schema version of the object.
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
        tx.writeSimpleField(id, this.storageId, value, false);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T getValue(Transaction tx, ObjId id) {
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(this.parent == null, "field is a sub-field of " + this.parent);
        return (T)tx.readSimpleField(id, this.storageId, false);
    }

    @Override
    public boolean hasDefaultValue(Transaction tx, ObjId id) {
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(this.parent == null, "field is a sub-field of " + this.parent);
        return tx.hasDefaultValue(id, this);
    }

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        return target.caseSimpleField(this);
    }

    @Override
    public String toString() {
        return (this.indexed ? "indexed " : "") + "field `" + this.name + "' of type " + this.fieldType.typeToken;
    }

// Non-public methods

    @Override
    SimpleFieldStorageInfo<T> toStorageInfo() {
        return new SimpleFieldStorageInfo<>(this, this.parent != null ? this.parent.storageId : 0);
    }

    @Override
    void copy(ObjId srcId, ObjId dstId, Transaction srcTx, Transaction dstTx) {
        dstTx.writeSimpleField(dstId, this.storageId, srcTx.readSimpleField(srcId, this.storageId, false), false);
    }

    /**
     * Encode the given value.
     *
     * @param obj value to encode
     * @return encoded value, or null if value is the default value
     * @throws IllegalArgumentException if {@code obj} cannot be encoded
     */
    byte[] encode(Object obj) {
        T value;
        try {
            value = this.fieldType.validate(obj);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("can't set " + this + " to value " + obj + ": " + e.getMessage(), e);
        }
        final ByteWriter writer = new ByteWriter();
        this.fieldType.write(writer, value);
        final byte[] result = writer.getBytes();
        return Arrays.equals(result, this.fieldType.getDefaultValue()) ? null : result;
    }
}

