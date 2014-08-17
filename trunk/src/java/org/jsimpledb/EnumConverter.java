
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;

import java.util.HashMap;

import org.dellroad.stuff.java.EnumUtil;
import org.jsimpledb.core.EnumValue;

/**
 * Converts between core database {@link EnumValue} objects and the corresponding Java {@link Enum} model values.
 *
 * <p>
 * When converting in the forward direction from {@link EnumValue} to {@link Enum}, the corresponding {@link Enum} value
 * is chosen by first attempting to match by {@linkplain EnumValue#getName name}, then by
 * {@linkplain EnumValue#getOrdinal ordinal value}. If neither match succeeds, an {@link UnmatchedEnumException} is thrown.
 * </p>
 */
public class EnumConverter<T extends Enum<T>> extends Converter<EnumValue, T> {

    private final Class<T> enumType;
    private final HashMap<Integer, T> ordinalMap = new HashMap<>();
    private final HashMap<String, T> nameMap = new HashMap<>();

    /**
     * Constructor.
     *
     * @param enumType {@link Enum} type
     * @throws IllegalArgumentException if {@code enumType} is null
     */
    public EnumConverter(Class<T> enumType) {
        if (enumType == null)
            throw new IllegalArgumentException("null enumType");
        enumType.asSubclass(Enum.class);                            // verify it's really an Enum
        this.enumType = enumType;
        for (T value : EnumUtil.getValues(this.enumType)) {
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
            throw new UnmatchedEnumException(this.enumType, enumValue);
        return value;
    }

    @Override
    protected EnumValue doBackward(T value) {
        if (value == null)
            return null;
        return new EnumValue(value);
    }

    /**
     * Get the {@link Enum} type associated with this instance.
     */
    public Class<T> getEnumType() {
        return this.enumType;
    }
}

