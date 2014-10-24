
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.Deque;
import java.util.List;

import org.jsimpledb.core.ObjId;

abstract class JComplexFieldInfo extends JFieldInfo {

    private List<JSimpleFieldInfo> subFieldInfos;

    JComplexFieldInfo(JComplexField jfield) {
        super(jfield);
    }

    public List<JSimpleFieldInfo> getSubFieldInfos() {
        return this.subFieldInfos;
    }
    public void setSubFieldInfos(List<JSimpleFieldInfo> subFieldInfos) {
        this.subFieldInfos = subFieldInfos;
    }

    public abstract String getSubFieldInfoName(JSimpleFieldInfo subFieldInfo);

    public JSimpleFieldInfo getSubFieldInfo(String name) {
        for (JSimpleFieldInfo subFieldInfo : this.getSubFieldInfos()) {
            if (this.getSubFieldInfoName(subFieldInfo).equals(name))
                return subFieldInfo;
        }
        throw new IllegalArgumentException("no sub-field named `" + name + "' exists in " + this);
    }

    /**
     * Add any valid index entry return types for @IndexQuery-annotated methods that query the given indexed sub-field.
     */
    abstract <T, V> void addIndexEntryReturnTypes(List<TypeToken<?>> types,
      TypeToken<T> targetType, JSimpleFieldInfo subFieldInfo, TypeToken<V> valueType);

    /**
     * Determine the type of index query from the method return type, i.e., normal object query or some kind of index entry query.
     *
     * @return zero for a normal query or some field-specific value otherwise
     */
    abstract int getIndexEntryQueryType(TypeToken<?> queryObjectType);

    /**
     * Get the {@link JTransaction} method to invoke from generated classes for the given index entry query type.
     */
    abstract Method getIndexEntryQueryMethod(int queryType);

    /**
     * Recurse for copying between transactions. Copies all objects referred to by any reference in the given
     * subfield of the given object from {@code srcTx} to {@code dstTx}.
     *
     * @param seen IDs of objects already copied
     * @param srcTx source transaction
     * @param dstTx destination transaction
     * @param id ID of the object containing this complex field in {@code srcTx}
     * @param storageId storage ID of the sub-field of this field containing the references to copy
     * @param nextFields remaining fields to follow in the reference path
     */
    public abstract void copyRecurse(ObjIdSet seen, JTransaction srcTx, JTransaction dstTx,
      ObjId id, int storageId, Deque<Integer> nextFields);

    // Recurse on the iteration of references
    protected void copyRecurse(ObjIdSet seen, JTransaction srcTx, JTransaction dstTx, Iterable<?> it, Deque<Integer> nextFields) {
        for (Object obj : it) {
            if (obj != null) {
                final ObjId id = (ObjId)obj;
                srcTx.copyTo(seen, dstTx, id, id, false, nextFields);
            }
        }
    }
}

