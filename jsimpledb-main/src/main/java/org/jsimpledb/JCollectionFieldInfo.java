
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jsimpledb.core.CollectionField;

abstract class JCollectionFieldInfo extends JComplexFieldInfo {

    JCollectionFieldInfo(JCollectionField jfield) {
        super(jfield);
    }

    /**
     * Get the element sub-field info.
     */
    public JSimpleFieldInfo getElementFieldInfo() {
        return this.getSubFieldInfos().get(0);
    }

    @Override
    public String getSubFieldInfoName(JSimpleFieldInfo subFieldInfo) {
        if (subFieldInfo.storageId == this.getElementFieldInfo().getStorageId())
            return CollectionField.ELEMENT_FIELD_NAME;
        throw new RuntimeException("internal error");
    }

    @Override
    public Set<TypeToken<?>> getTypeTokens(Class<?> context) {
        final Set<TypeToken<?>> elementTypeTokens = this.getElementFieldInfo().getTypeTokens(context);
        final HashSet<TypeToken<?>> typeTokens = new HashSet<>(elementTypeTokens.size());
        for (TypeToken<?> elementTypeToken : elementTypeTokens)
            typeTokens.add(this.buildTypeToken(elementTypeToken.wrap()));
        return typeTokens;
    }

    @Override
    <T> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType) {
        for (TypeToken<?> elementTypeToken : this.getElementFieldInfo().getTypeTokens(targetType))
            this.addChangeParameterTypes(types, targetType, elementTypeToken);
    }

    abstract <T, E> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType, TypeToken<E> elementType);

    abstract <E> TypeToken<? extends Collection<E>> buildTypeToken(TypeToken<E> elementType);

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final JCollectionFieldInfo that = (JCollectionFieldInfo)obj;
        return this.getElementFieldInfo().equals(that.getElementFieldInfo());
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.getElementFieldInfo().hashCode();
    }
}

