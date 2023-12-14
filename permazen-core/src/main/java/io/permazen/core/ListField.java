
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.core.util.ObjIdMap;
import io.permazen.schema.ListSchemaField;
import io.permazen.schema.SimpleSchemaField;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;

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
    ListField(Schema schema, ListSchemaField field, SimpleField<E> elementField) {
        super(schema, field, new TypeToken<List<E>>() { }
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
    void buildIndexEntry(ObjId id, SimpleField<?> subField, ByteReader reader, byte[] value, ByteWriter writer) {
        assert subField == this.elementField;
        writer.write(value);
        id.writeTo(writer);
        writer.write(reader);
    }
}
