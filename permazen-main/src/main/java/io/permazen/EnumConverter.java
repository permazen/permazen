
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.collect.EnumHashBiMap;
import com.google.common.collect.Maps;

import io.permazen.core.EnumValue;

import java.lang.reflect.Array;
import java.util.EnumSet;

/**
 * Converts between core database {@link EnumValue} objects and the corresponding Java {@link Enum} model values.
 */
public class EnumConverter<T extends Enum<T>> extends Converter<T, EnumValue> {

    private final Class<T> enumType;
    private final EnumHashBiMap<T, EnumValue> valueMap;

    /**
     * Constructor.
     *
     * @param enumType {@link Enum} type
     * @throws IllegalArgumentException if {@code enumType} is null
     */
    public EnumConverter(Class<T> enumType) {
        Preconditions.checkArgument(enumType != null, "null enumType");
        enumType.asSubclass(Enum.class);                            // verify it's really an Enum
        this.enumType = enumType;
        this.valueMap = EnumHashBiMap.<T, EnumValue>create(Maps.asMap(EnumSet.allOf(this.enumType), EnumValue::new));
    }

    @Override
    protected EnumValue doForward(T value) {
        if (value == null)
            return null;
        final EnumValue enumValue = this.valueMap.get(value);
        if (enumValue == null)
            throw new IllegalArgumentException("invalid enum value " + value + " not an instance of " + this.enumType);
        return enumValue;
    }

    @Override
    protected T doBackward(EnumValue enumValue) {
        if (enumValue == null)
            return null;
        final T value = this.valueMap.inverse().get(enumValue);
        if (value == null)
            throw new IllegalArgumentException("invalid value " + enumValue + " not found in " + this.enumType);
        return value;
    }

    /**
     * Get the {@link Enum} type associated with this instance.
     *
     * @return associated {@link Enum} type
     */
    public Class<T> getEnumType() {
        return this.enumType;
    }

    /**
     * Convenience "constructor" allowing wildcard caller {@link Enum} types.
     *
     * @param enumType type for the created converter
     * @return new converter
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static EnumConverter<?> createEnumConverter(Class<? extends Enum<?>> enumType) {
        return new EnumConverter(enumType);
    }

    /**
     * Create a {@link Converter} for arrays based on converting the base elements with an {@link EnumConverter}.
     *
     * @param enumType base enum type
     * @param dimensions number of array dimensions
     * @return new converter
     * @throws IllegalArgumentException if {@code enumType} is null
     * @throws IllegalArgumentException if {@code dimemsions} is not between 1 and 255 (inclusive)
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Converter<?, ?> createEnumArrayConverter(Class<? extends Enum<?>> enumType, int dimensions) {
        Preconditions.checkArgument(dimensions >= 1 && dimensions <= 255, "invalid dimensions");
        Class<?> aType = enumType;
        Class<?> bType = EnumValue.class;
        Converter<?, ?> converter = EnumConverter.createEnumConverter(enumType);
        for (int i = 0; i < dimensions; i++) {
            converter = new ArrayConverter(aType, bType, converter);
            aType = Array.newInstance(aType, 0).getClass();
            bType = Array.newInstance(bType, 0).getClass();
        }
        return converter;
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final EnumConverter<?> that = (EnumConverter<?>)obj;
        return this.enumType == that.enumType;
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode() ^ this.enumType.hashCode();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[type=" + this.enumType.getName() + "]";
    }
}
