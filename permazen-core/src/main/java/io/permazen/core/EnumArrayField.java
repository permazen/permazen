
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.encoding.Encoding;
import io.permazen.schema.EnumArraySchemaField;

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

    @SuppressWarnings("unchecked")
    EnumArrayField(Schema schema, EnumArraySchemaField field, EnumValueEncoding baseType, Encoding<?> encoding, boolean indexed) {
        super(schema, field, (Encoding<Object>)encoding, indexed);
        this.baseType = baseType;
        this.dimensions = field.getDimensions();
        Preconditions.checkArgument(dimensions >= 1 && dimensions <= Encoding.MAX_ARRAY_DIMENSIONS);
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
        Preconditions.checkArgument(target != null, "null target");
        return target.caseEnumArrayField(this);
    }

    @Override
    public String toString() {
        return "enum array field \"" + this.getFullName() + "\"";
    }
}
