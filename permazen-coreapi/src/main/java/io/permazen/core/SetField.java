
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.core.util.ObjIdMap;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.CloseableIterator;

import java.util.Collections;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * Set field.
 *
 * @param <E> Java type for the set elements
 */
public class SetField<E> extends CollectionField<NavigableSet<E>, E> {

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
    SetField(String name, int storageId, Schema schema, SimpleField<E> elementField) {
        super(name, storageId, schema, new TypeToken<NavigableSet<E>>() { }
          .where(new TypeParameter<E>() { }, elementField.typeToken.wrap()), elementField);
    }

// Public methods

    @Override
    @SuppressWarnings("unchecked")
    public NavigableSet<E> getValue(Transaction tx, ObjId id) {
        Preconditions.checkArgument(tx != null, "null tx");
        return (NavigableSet<E>)tx.readSetField(id, this.storageId, false);
    }

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        return target.caseSetField(this);
    }

    @Override
    public String toString() {
        return "set field \"" + this.name + "\" containing " + this.elementField;
    }

// Non-public methods

    @Override
    NavigableSet<E> getValueInternal(Transaction tx, ObjId id) {
        return new JSSet<>(tx, this, id);
    }

    @Override
    NavigableSet<E> getValueReadOnlyCopy(Transaction tx, ObjId id) {
        return Collections.unmodifiableNavigableSet(new TreeSet<E>(this.getValueInternal(tx, id)));
    }

    @Override
    SetElementStorageInfo<E> toStorageInfo(SimpleField<?> subField) {
        assert subField == this.elementField;
        return new SetElementStorageInfo<>(this);
    }

    @Override
    void copy(ObjId srcId, ObjId dstId, Transaction srcTx, Transaction dstTx, ObjIdMap<ObjId> objectIdMap) {
        final FieldType<E> fieldType = this.elementField.fieldType;
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
                    final int diff = fieldType.compare(s, d);
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
    void buildIndexEntry(ObjId id, SimpleField<?> subField, ByteReader reader, byte[] value, ByteWriter writer) {
        assert subField == this.elementField;
        writer.write(reader);
        id.writeTo(writer);
    }
}

