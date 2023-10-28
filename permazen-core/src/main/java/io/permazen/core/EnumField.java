
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.core.encoding.EnumValueEncoding;

import java.util.List;

/**
 * A field that contains a value chosen from in an ordered list of unique {@link String} identifiers.
 *
 * <p>
 * Two instances of this class are considered compatible only when their ordered lists of identifiers are identical.
 */
public class EnumField extends SimpleField<EnumValue> {

    /**
     * Constructor.
     *
     * @param name the name of the field
     * @param storageId field storage ID
     * @param schema schema version
     * @param idents the unique enum identifiers
     * @param indexed whether this field is indexed
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     * @throws IllegalArgumentException if {@code storageId} is invalid
     * @throws IllegalArgumentException if any identifier in {@code idents} is null, duplicate, or not a valid Java identifier
     */
    EnumField(String name, int storageId, Schema schema, boolean indexed, List<String> idents) {
        super(name, storageId, schema, new EnumValueEncoding(idents), indexed);
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
        return target.caseEnumField(this);
    }

    @Override
    public String toString() {
        return "enum field \"" + this.name + "\"";
    }
}
