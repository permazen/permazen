
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;

import org.jsimpledb.core.EnumValue;

class JEnumFieldInfo extends JSimpleFieldInfo {

    final Class<? extends Enum<?>> enumType;
    final EnumConverter<?> converter;

    @SuppressWarnings("unchecked")
    JEnumFieldInfo(JEnumField jfield, int parentStorageId) {
        super(jfield, parentStorageId);
        this.enumType = (Class<? extends Enum<?>>)jfield.getTypeToken().getRawType();
        this.converter = jfield.converter;
    }

    public Class<? extends Enum<?>> getEnumType() {
        return this.enumType;
    }

    @Override
    public Converter<EnumValue, ? extends Enum<?>> getConverter(JTransaction jtx) {
        return this.converter.reverse();
    }

// Object

    @Override
    public String toString() {
        return super.toString() + " and type " + this.enumType;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final JEnumFieldInfo that = (JEnumFieldInfo)obj;
        return this.enumType == that.enumType;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.enumType.hashCode();
    }
}

