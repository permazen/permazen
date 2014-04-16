
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.reflect.TypeToken;

import java.util.HashMap;

import org.dellroad.stuff.java.EnumUtil;
import org.jsimpledb.core.EnumValue;

class EnumConverter<T extends Enum<T>> extends Converter<EnumValue, T> {

    private final TypeToken<T> typeToken;

    private final HashMap<Integer, T> ordinalMap = new HashMap<>();
    private final HashMap<String, T> nameMap = new HashMap<>();

    EnumConverter(TypeToken<T> typeToken) {
        if (typeToken == null)
            throw new IllegalArgumentException("null typeToken");
        typeToken.getRawType().asSubclass(Enum.class);                  // verify it's really an Enum
        this.typeToken = typeToken;
        for (T value : EnumUtil.getValues(this.getType())) {
            this.ordinalMap.put(value.ordinal(), value);
            this.nameMap.put(value.name(), value);
        }
    }

    @Override
    protected T doForward(EnumValue enumValue) {
        if (enumValue == null)
            return null;
        final T nameMatch = this.nameMap.get(enumValue.getName());
        final T ordinalMatch = this.ordinalMap.get(enumValue.getOrdinal());
        final T value = nameMatch != null ? nameMatch : ordinalMatch;
        if (value == null)
            throw new UnmatchedEnumException(this.getType(), enumValue);
        return value;
    }

    @Override
    protected EnumValue doBackward(T value) {
        if (value == null)
            return null;
        return new EnumValue(value);
    }

    @SuppressWarnings("unchecked")
    protected Class<T> getType() {
        return (Class<T>)this.typeToken.getRawType();
    }
}

