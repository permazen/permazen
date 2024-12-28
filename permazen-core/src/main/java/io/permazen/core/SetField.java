
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.core.util.ObjIdMap;
import io.permazen.encoding.Encoding;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVTransaction;
import io.permazen.schema.SetSchemaField;
import io.permazen.schema.SimpleSchemaField;
import io.permazen.util.ByteData;
import io.permazen.util.CloseableIterator;
import io.permazen.util.ImmutableNavigableSet;

import java.util.NavigableSet;

/**
 * Set field.
 *
 * @param <E> Java type for the set elements
 */
public class SetField<E> extends CollectionField<NavigableSet<E>, E> {

    @SuppressWarnings("serial")
    SetField(ObjType objType, SetSchemaField field, SimpleField<E> elementField) {
        super(objType, field, new TypeToken<NavigableSet<E>>() { }
          .where(new TypeParameter<E>() { }, elementField.typeToken.wrap()), elementField);
    }

// Public methods

    @Override
    public SetElementIndex<E> getElementIndex() {
        return (SetElementIndex<E>)super.getElementIndex();
    }

    @Override
    @SuppressWarnings("unchecked")
    public NavigableSet<E> getValue(Transaction tx, ObjId id) {
        Preconditions.checkArgument(tx != null, "null tx");
        return (NavigableSet<E>)tx.readSetField(id, this.name, false);
    }

    /**
     * Get the key in the underlying key/value store corresponding to this field in the specified object
     * and the specified element.
     *
     * @param id object ID
     * @param element set element
     * @return the corresponding {@link KVDatabase} key
     * @throws IllegalArgumentException if {@code id} is null or has the wrong object type
     * @see KVTransaction#watchKey KVTransaction.watchKey()
     */
    public ByteData getKey(ObjId id, E element) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");

        // Build key
        final ByteData.Writer writer = ByteData.newWriter();
        writer.write(super.getKey(id));
        this.elementField.encoding.write(writer, element);
        return writer.toByteData();
    }

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.caseSetField(this);
    }

    @Override
    public String toString() {
        return "set field \"" + this.name + "\"";
    }

// Package Methods

    @Override
    SetElementIndex<E> createElementSubFieldIndex(Schema schema, SimpleSchemaField schemaField, ObjType objType) {
        return new SetElementIndex<E>(schema, schemaField, objType, this);
    }

    @Override
    NavigableSet<E> getValueInternal(Transaction tx, ObjId id) {
        return new JSSet<>(tx, this, id);
    }

    @Override
    NavigableSet<E> getValueReadOnlyCopy(Transaction tx, ObjId id) {
        return new ImmutableNavigableSet<>(this.getValueInternal(tx, id));
    }

    @Override
    void copy(ObjId srcId, ObjId dstId, Transaction srcTx, Transaction dstTx, ObjIdMap<ObjId> objectIdMap) {
        final Encoding<E> encoding = this.elementField.encoding;
        final NavigableSet<E> src = this.getValue(srcTx, srcId);
        final NavigableSet<E> dst = this.getValue(dstTx, dstId);
        try (CloseableIterator<E> si = CloseableIterator.wrap(src.iterator());
             CloseableIterator<E> di = CloseableIterator.wrap(dst.iterator())) {

            // Check for empty
            if (!si.hasNext()) {
                dst.clear();
                return;
            }

            // If we're not remapping anything, walk forward through both sets and synchronize dst to src
            if (objectIdMap == null || objectIdMap.isEmpty() || !this.elementField.remapsObjectId()) {
                if (!di.hasNext()) {
                    dst.addAll(src);
                    return;
                }
                E s = si.next();
                E d = di.next();
                while (true) {
                    final int diff = encoding.compare(s, d);
                    boolean sadvance = true;
                    boolean dadvance = true;
                    if (diff < 0) {
                        dst.add(s);
                        dadvance = false;
                    } else if (diff > 0) {
                        di.remove();
                        sadvance = false;
                    }
                    if (sadvance) {
                        if (!si.hasNext()) {
                            dst.tailSet(s, false).clear();
                            return;
                        }
                        s = si.next();
                    }
                    if (dadvance) {
                        if (!di.hasNext()) {
                            dst.addAll(src.tailSet(s, true));
                            return;
                        }
                        d = di.next();
                    }
                }
            } else {
                dst.clear();
                while (si.hasNext())
                    dst.add(this.elementField.remapObjectId(objectIdMap, si.next()));
            }
        }
    }

    @Override
    void buildIndexEntry(ObjId id, SimpleField<?> subField, ByteData content, ByteData value, ByteData.Writer writer) {
        assert subField == this.elementField;
        writer.write(content);
        id.writeTo(writer);
    }
}
