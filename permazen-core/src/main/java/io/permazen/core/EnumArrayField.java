
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.core.encoding.EnumValueEncoding;

/**
 * A field that contains an array (having one or more dimensions) of values chosen from
 * an ordered list of unique {@link String} identifiers.
 *
 * <p>
 * Two instances of this class are considered compatible only when their ordered lists of identifiers and dimensions are identical.
 */
public class EnumArrayField extends SimpleField<Object> {

    private final EnumValueEncoding baseType;
    private final int dimensions;

    /**
     * Constructor.
     *
     * @param name the name of the field
     * @param storageId field storage ID
     * @param schema schema version
     * @param indexed whether this field is indexed
     * @param baseType base component type
     * @param encoding encoding
     * @param dimensions number of dimensions
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     * @throws IllegalArgumentException if {@code storageId} is invalid
     * @throws IllegalArgumentException if {@code dimensions} is invalid
     */
    @SuppressWarnings("unchecked")
    EnumArrayField(String name, int storageId, Schema schema,
      boolean indexed, EnumValueEncoding baseType, Encoding<?> encoding, int dimensions) {
        super(name, storageId, schema, (Encoding<Object>)encoding, indexed);
        Preconditions.checkArgument(dimensions >= 1 && dimensions <= Encoding.MAX_ARRAY_DIMENSIONS);
        this.baseType = baseType;
        this.dimensions = dimensions;
    }

    /**
     * Get the base encoding.
     *
     * @return array base encoding
     */
    public EnumValueEncoding getBaseType() {
        return this.baseType;
    }

    /**
     * Get the number of enum array dimensions.
     *
     * @return number of dimensions, a value from 1 to 255
     */
    public int getDimensions() {
        return this.dimensions;
    }

// Public methods

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        return target.caseEnumArrayField(this);
    }

    @Override
    public String toString() {
        return "enum array field \"" + this.name + "\"";
    }
}
