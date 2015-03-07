
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

/**
 * List field.
 *
 * <p>
 * JSimpleDB list fields have performance characteristics similar to {@link ArrayList}.
 * </p>
 *
 * @param <E> Java type for the list elements
 */
public class ListField<E> extends CollectionField<List<E>, E> {

    /**
     * Constructor.
     *
     * @param name the name of the field
     * @param storageId field content storage ID
     * @param schema schema version
     * @param elementField this field's element sub-field
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    @SuppressWarnings("serial")
    ListField(String name, int storageId, Schema schema, SimpleField<E> elementField) {
        super(name, storageId, schema, new TypeToken<List<E>>() { }
          .where(new TypeParameter<E>() { }, elementField.typeToken.wrap()), elementField);
    }

// Public methods

    @Override
    @SuppressWarnings("unchecked")
    public List<E> getValue(Transaction tx, ObjId id) {
        if (tx == null)
            throw new IllegalArgumentException("null tx");
        return (List<E>)tx.readListField(id, this.storageId, false);
    }

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        return target.caseListField(this);
    }

    @Override
    public String toString() {
        return "list field `" + this.name + "' containing " + this.elementField;
    }

// Non-public methods

    @Override
    List<E> getValueInternal(Transaction tx, ObjId id) {
        return new JSList<E>(tx, this, id);
    }

    @Override
    List<E> getValueReadOnlyCopy(Transaction tx, ObjId id) {
        return Collections.unmodifiableList(new ArrayList<E>(this.getValueInternal(tx, id)));
    }

    @Override
    ListFieldStorageInfo<E> toStorageInfo() {
        return new ListFieldStorageInfo<E>(this);
    }

    @Override
    void copy(ObjId srcId, ObjId dstId, Transaction srcTx, Transaction dstTx) {
        final List<E> srcList = this.getValue(srcTx, srcId);
        final List<E> dstList = this.getValue(dstTx, dstId);
        final int ssize = srcList.size();
        final int dsize = dstList.size();
        final int min = Math.min(ssize, dsize);
        for (int i = 0; i < min; i++)
            dstList.set(i, srcList.get(i));
        if (ssize < dsize)
            dstList.subList(ssize, dsize).clear();
        else if (dsize < ssize)
            dstList.addAll(srcList.subList(dsize, ssize));
    }

    @Override
    void buildIndexEntry(ObjId id, SimpleField<?> subField, ByteReader reader, byte[] value, ByteWriter writer) {
        assert subField == this.elementField;
        writer.write(value);
        id.writeTo(writer);
        writer.write(reader);
    }
}

