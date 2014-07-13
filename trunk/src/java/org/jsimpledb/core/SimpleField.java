
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.Arrays;

import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * A simple {@link Field}.
 *
 * <p>
 * {@link SimpleField}s have these requirements and properties:
 *  <ul>
 *  <li>They have an associated {@link FieldType} representing the domain of possible values</li>
 *  <li>Theys can serve as the element sub-field for {@link SetField}s and {@link ListField}s,
 *      and the key and value sub-fields for {@link MapField}s.</li>
 *  <li>They can be indexed.</li>
 * </p>
 *
 * @param <T> Java type for the field's values
 */
public class SimpleField<T> extends Field<T> {

    final FieldType<T> fieldType;
    final boolean indexed;

    ComplexField<?> parent;

    /**
     * Constructor.
     *
     * @param name the name of the field
     * @param storageId field storage ID
     * @param version schema version
     * @param fieldType field type
     * @param indexed whether this field is indexed
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     * @throws IllegalArgumentException if {@code storageId} is zero or less
     */
    SimpleField(String name, int storageId, SchemaVersion version, FieldType<T> fieldType, boolean indexed) {
        super(name, storageId, version, fieldType.getTypeToken());
        this.fieldType = fieldType;
        this.indexed = indexed;
    }

// Public methods

    /**
     * Get the {@link FieldType} associated with this field.
     */
    public FieldType<T> getFieldType() {
        return this.fieldType;
    }

    /**
     * Determine whether this field is indexed.
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
     */
    public void setValue(Transaction tx, ObjId id, T value) {
        if (tx == null)
            throw new IllegalArgumentException("null tx");
        tx.writeSimpleField(id, this.storageId, value, false);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T getValue(Transaction tx, ObjId id) {
        if (tx == null)
            throw new IllegalArgumentException("null tx");
        return (T)tx.readSimpleField(id, this.storageId, false);
    }

    @Override
    public boolean hasDefaultValue(Transaction tx, ObjId id) {
        if (tx == null)
            throw new IllegalArgumentException("null tx");
        return tx.hasDefaultValue(id, this);
    }

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        return target.caseSimpleField(this);
    }

    @Override
    public String toString() {
        return "field `" + this.name + "' of type " + this.fieldType.typeToken;
    }

// Non-public methods

    /**
     * Check compatibility with another {@link SimpleField} across a schema change.
     * To be compatible, the two fields must be exactly the same in terms of binary encoding, Java representation,
     * and default value.
     * This is in effect an {@code #equals equals()} test with respect to those aspects. Note that compatibililty
     * does not necessarily imply the fields are both indexed or both not indexed.
     *
     * <p>
     * The implementation in {@link SimpleField} checks that the two fields have equivalent {@link FieldType}s.
     * </p>
     *
     * @param that field to check for compatibility
     * @throws NullPointerException if {@code that} is null
     */
    boolean isSchemaChangeCompatible(SimpleField<?> that) {
        return this.fieldType.equals(that.fieldType);
    }

    @Override
    SimpleFieldStorageInfo toStorageInfo() {
        return new SimpleFieldStorageInfo(this,
          this.parent != null ? this.parent.storageId : 0, this.parent != null && this.parent.hasComplexIndex(this));
    }

    @Override
    void copy(ObjId srcId, ObjId dstId, Transaction srcTx, Transaction dstTx) {
        dstTx.writeSimpleField(dstId, this.storageId, srcTx.readSimpleField(srcId, this.storageId, false), false);
    }

    @Override
    boolean isEquivalent(Field<?> field) {
        if (field.getClass() != this.getClass())
            return false;
        final SimpleField<?> that = (SimpleField<?>)field;
        return this.fieldType.equals(that.fieldType) && this.indexed == that.indexed;
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

    /**
     * Bulid the index entry for a given object having the given value in this field.
     * The encoded field value is read from the given {@link ByteReader}.
     *
     * @param id object containing the field
     * @param value encoded field value, or null for default value
     * @return index key
     */
    byte[] buildIndexKey(ObjId id, byte[] value) {

        // Sanity check
        if (!this.indexed)
            throw new IllegalArgumentException(this + " is not indexed");

        // Get default value if necessary
        if (value == null)
            value = this.fieldType.getDefaultValue();

        // Build index entry
        final ByteWriter writer = new ByteWriter();
        UnsignedIntEncoder.write(writer, this.storageId);
        writer.write(value);
        id.writeTo(writer);

        // Return index entry
        return writer.getBytes();
    }
}

