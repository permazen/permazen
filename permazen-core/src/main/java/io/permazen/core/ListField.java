
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.core.util.ObjIdMap;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVTransaction;
import io.permazen.schema.ListSchemaField;
import io.permazen.schema.SimpleSchemaField;
import io.permazen.util.ByteData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * List field.
 *
 * <p>
 * Permazen list fields have performance characteristics similar to {@link ArrayList}.
 *
 * @param <E> Java type for the list elements
 */
public class ListField<E> extends CollectionField<List<E>, E> {

    @SuppressWarnings("serial")
    ListField(ObjType objType, ListSchemaField field, SimpleField<E> elementField) {
        super(objType, field, new TypeToken<List<E>>() { }
          .where(new TypeParameter<E>() { }, elementField.typeToken.wrap()), elementField);
    }

// Public methods

    @Override
    public ListElementIndex<E> getElementIndex() {
        return (ListElementIndex<E>)super.getElementIndex();
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<E> getValue(Transaction tx, ObjId id) {
        Preconditions.checkArgument(tx != null, "null tx");
        return (List<E>)tx.readListField(id, this.name, false);
    }

    /**
     * Get the key in the underlying key/value store corresponding to this field in the specified object
     * and the specified list index.
     *
     * @param id object ID
     * @param index list index
     * @return the corresponding {@link KVDatabase} key
     * @throws IllegalArgumentException if {@code id} is null or has the wrong object type
     * @throws IllegalArgumentException if {@code index} is negative
     * @see KVTransaction#watchKey KVTransaction.watchKey()
     */
    public ByteData getKey(ObjId id, int index) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(index >= 0, "negative index");

        // Build key
        final ByteData.Writer writer = ByteData.newWriter();
        writer.write(super.getKey(id));
        Encodings.UNSIGNED_INT.write(writer, index);
        return writer.toByteData();
    }

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.caseListField(this);
    }

    @Override
    public String toString() {
        return "list field \"" + this.name + "\" containing " + this.elementField;
    }

// Package Methods

    @Override
    ListElementIndex<E> createElementSubFieldIndex(Schema schema, SimpleSchemaField schemaField, ObjType objType) {
        return new ListElementIndex<>(schema, schemaField, objType, this);
    }

    @Override
    List<E> getValueInternal(Transaction tx, ObjId id) {
        return new JSList<>(tx, this, id);
    }

    @Override
    List<E> getValueReadOnlyCopy(Transaction tx, ObjId id) {
        return Collections.unmodifiableList(new ArrayList<>(this.getValueInternal(tx, id)));
    }

    @Override
    void copy(ObjId srcId, ObjId dstId, Transaction srcTx, Transaction dstTx, ObjIdMap<ObjId> objectIdMap) {
        final List<E> srcList = this.getValue(srcTx, srcId);
        final List<E> dstList = this.getValue(dstTx, dstId);
        final int ssize = srcList.size();
        final int dsize = dstList.size();
        final int min = Math.min(ssize, dsize);
        for (int i = 0; i < min; i++)
            dstList.set(i, this.elementField.remapObjectId(objectIdMap, srcList.get(i)));
        if (ssize < dsize)
            dstList.subList(ssize, dsize).clear();
        else if (dsize < ssize) {
            for (E elem : srcList.subList(dsize, ssize))
                dstList.add(this.elementField.remapObjectId(objectIdMap, elem));
        }
    }

    @Override
    void buildIndexEntry(ObjId id, SimpleField<?> subField, ByteData content, ByteData value, ByteData.Writer writer) {
        assert subField == this.elementField;
        writer.write(value);
        id.writeTo(writer);
        writer.write(content);
    }
}
