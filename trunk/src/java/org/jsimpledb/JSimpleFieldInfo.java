
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.change.SimpleFieldChange;
import org.jsimpledb.core.Transaction;

class JSimpleFieldInfo extends JFieldInfo {

    private final HashSet<TypeToken<?>> typeTokens = new HashSet<>();
    private final int parentStorageId;

    private TypeToken<?> ancestorType;
    private boolean indexed;

    JSimpleFieldInfo(JSimpleField jfield, int parentStorageId) {
        super(jfield);
        this.parentStorageId = parentStorageId;
    }

    public int getParentStorageId() {
        return this.parentStorageId;
    }

    @Override
    void witness(JField jfield) {
        super.witness(jfield);
        final JSimpleField jsimpleField = (JSimpleField)jfield;
        if (this.typeTokens.add(jsimpleField.typeToken))
            this.ancestorType = null;
        this.indexed |= jsimpleField.indexed;
    }

    @Override
    public TypeToken<?> getTypeToken() {
        if (this.ancestorType == null)
            this.ancestorType = Util.findLowestCommonAncestor(this.typeTokens);
        return this.ancestorType;
    }

    /**
     * Determine whether any of the individual associated {@link JSimpleField}s are indexed.
     */
    public boolean isIndexed() {
        return this.indexed;
    }

    @Override
    void registerChangeListener(Transaction tx, int[] path, Iterable<Integer> types, AllChangesListener listener) {
        tx.addSimpleFieldChangeListener(this.storageId, path, types, listener);
    }

    @Override
    <T> void addChangeParameterTypes(List<TypeToken<?>> types, TypeToken<T> targetType) {
        this.addChangeParameterTypes(types, targetType, this.getTypeToken());
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private <T, V> void addChangeParameterTypes(List<TypeToken<?>> types, TypeToken<T> targetType, TypeToken<V> fieldType) {
        types.add(new TypeToken<SimpleFieldChange<T, V>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<V>() { }, fieldType.wrap()));
    }

    /**
     * Add valid return types for @IndexQuery-annotated methods that query this indexed field.
     */
    @SuppressWarnings("serial")
    <T, V> void addIndexReturnTypes(List<TypeToken<?>> types, TypeToken<T> targetType, TypeToken<V> valueType) {
        types.add(new TypeToken<NavigableMap<V, NavigableSet<T>>>() { }
          .where(new TypeParameter<V>() { }, valueType.wrap())
          .where(new TypeParameter<T>() { }, targetType));
    }

// Object

    @Override
    public String toString() {
        String string = super.toString();
        if (this.parentStorageId != 0)
            string = string.replaceAll(" field", " sub-field");
        return string;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final JSimpleFieldInfo that = (JSimpleFieldInfo)obj;
        return this.parentStorageId == that.parentStorageId;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.parentStorageId;
    }
}

