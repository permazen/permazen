
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.schema.EnumSchemaField;

/**
 * A field that contains a value chosen from in an ordered list of unique {@link String} identifiers.
 *
 * <p>
 * Two instances of this class are considered compatible only when their ordered lists of identifiers are identical.
 */
public class EnumField extends SimpleField<EnumValue> {

    EnumField(ObjType objType, EnumSchemaField field, boolean indexed) {
        super(objType, field, new EnumValueEncoding(field.getIdentifiers()), indexed);
    }

// Public methods

    /**
     * Get the {@link EnumValueEncoding} associated with this instance.
     */
    @Override
    public EnumValueEncoding getEncoding() {
        return (EnumValueEncoding)super.getEncoding();
    }

    @Override
    public <R> R visit(FieldSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.caseEnumField(this);
    }

    @Override
    public String toString() {
        return "enum field \"" + this.getFullName() + "\"";
    }
}
