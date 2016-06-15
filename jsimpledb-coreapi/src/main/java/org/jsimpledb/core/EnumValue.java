
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Preconditions;

/**
 * Holds a non-null {@link Enum} value without actually referencing any Java {@link Enum} type.
 * Instead, instances hold a name and ordinal value.
 *
 * <p>
 * Instances are immutable.
 *
 * @see EnumFieldType
 * @see org.jsimpledb.EnumConverter
 */
public class EnumValue {

    private final String name;
    private final int ordinal;

    /**
     * Constructor taking name and ordinal value.
     *
     * @param name enum name
     * @param ordinal enum ordinal value
     * @throws IllegalArgumentException if {@code ordinal} is negative
     * @throws IllegalArgumentException if {@code name} is null
     */
    public EnumValue(String name, int ordinal) {
        Preconditions.checkArgument(name != null, "null name");
        Preconditions.checkArgument(ordinal >= 0, "invalid negative ordinal");
        this.name = name;
        this.ordinal = ordinal;
    }

    /**
     * Constructor taking an {@link Enum} value.
     *
     * @param value enum value
     * @throws IllegalArgumentException if {@code value} is null
     */
    public EnumValue(Enum<?> value) {
        Preconditions.checkArgument(value != null, "null value");
        this.name = value.name();
        this.ordinal = value.ordinal();
    }

    /**
     * Get the enum name.
     *
     * @return enum value name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the enum ordinal value.
     *
     * @return enum value ordinal
     */
    public int getOrdinal() {
        return this.ordinal;
    }

    /**
     * Find the instance in the given {@link Enum} type that matches this instance in both name and ordinal value, if any.
     *
     * <p>
     * Note: to match only by name, just use {@link Enum#valueOf Enum.valueOf()}.
     *
     * @param type {@link Enum} type
     * @param <T> enum type
     * @return matching instance of type {@code type}, or null if none exists
     * @throws IllegalArgumentException if {@code type} is null
     */
    public <T extends Enum<T>> T find(Class<T> type) {
        Preconditions.checkArgument(type != null, "null type");
        final T value;
        try {
            value = Enum.valueOf(type, this.name);
        } catch (IllegalArgumentException e) {
            return null;
        }
        return value.ordinal() == this.ordinal ? value : null;
    }

// Object

    @Override
    public String toString() {
        return this.name + "#" + this.ordinal;
    }

    @Override
    public int hashCode() {
        return this.name.hashCode() ^ this.ordinal;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final EnumValue that = (EnumValue)obj;
        return this.name.equals(that.name) && this.ordinal == that.ordinal;
    }
}

