
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;

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
        return "set field `" + this.name + "' containing " + this.elementField;
    }

// Non-public methods

    @Override
    NavigableSet<E> getValueInternal(Transaction tx, ObjId id) {
        return new JSSet<E>(tx, this, id);
    }

    @Override
    NavigableSet<E> getValueReadOnlyCopy(Transaction tx, ObjId id) {
        return Sets.unmodifiableNavigableSet(new TreeSet<E>(this.getValueInternal(tx, id)));
    }

    @Override
    SetFieldStorageInfo<E> toStorageInfo() {
        return new SetFieldStorageInfo<E>(this);
    }

    @Override
    void copy(ObjId srcId, ObjId dstId, Transaction srcTx, Transaction dstTx) {
        final FieldType<E> fieldType = this.elementField.fieldType;
        final NavigableSet<E> src = this.getValue(srcTx, srcId);
        final NavigableSet<E> dst = this.getValue(dstTx, dstId);
        final Iterator<E> si = src.iterator();
        final Iterator<E> di = dst.iterator();
        if (!si.hasNext()) {
            dst.clear();
            return;
        }
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
    }

    @Override
    void buildIndexEntry(ObjId id, SimpleField<?> subField, ByteReader reader, byte[] value, ByteWriter writer) {
        assert subField == this.elementField;
        writer.write(reader);
        id.writeTo(writer);
    }
}

