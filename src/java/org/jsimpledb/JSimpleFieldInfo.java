
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.util.HashSet;
import java.util.List;

import org.jsimpledb.change.SimpleFieldChange;
import org.jsimpledb.core.Transaction;

class JSimpleFieldInfo extends JFieldInfo {

    private final int parentStorageId;

    private boolean indexed;

    JSimpleFieldInfo(JSimpleField jfield, int parentStorageId) {
        super(jfield);
        this.parentStorageId = parentStorageId;
    }

    /**
     * Get parent complex field storage info, if any.
     *
     * @return parent complex field storage info, or zero if this instance does not represent a sub-field
     */
    public int getParentStorageId() {
        return this.parentStorageId;
    }

    @Override
    public TypeToken<?> getTypeToken(Class<?> context) {
        final HashSet<TypeToken<?>> contextFieldTypes = new HashSet<>();
        for (JClass<?> jclass : this.jdb.jclasses.values()) {

            // Check if jclass is under consideration
            if (!context.isAssignableFrom(jclass.type))
                continue;

            // Find this field in jclass, if it exists
            final JSimpleField jfield;
            if (this.parentStorageId != 0) {
                final JComplexField parentField = (JComplexField)jclass.jfields.get(this.parentStorageId);
                if (parentField == null)
                    continue;
                jfield = parentField.getSubField(this.storageId);
            } else if ((jfield = (JSimpleField)jclass.jfields.get(this.storageId)) == null)
                continue;

            // Add field's type in jclass
            contextFieldTypes.add(jfield.typeToken);
        }
        if (contextFieldTypes.isEmpty())
            throw new IllegalArgumentException("no sub-type of " + context + " contains " + this);
        return Util.findLowestCommonAncestor(contextFieldTypes);
    }

    @Override
    void witness(JField jfield) {
        super.witness(jfield);
        final JSimpleField jsimpleField = (JSimpleField)jfield;
        this.indexed |= jsimpleField.indexed;
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
    <T> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType) {
        this.addChangeParameterTypes(types, targetType, this.getTypeToken(targetType));
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private <T, V> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType, TypeToken<V> fieldType) {
        types.add(new TypeToken<SimpleFieldChange<T, V>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<V>() { }, fieldType.wrap()));
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

