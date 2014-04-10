
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * A complex field in one version of a {@link SchemaObject}.
 */
public abstract class ComplexSchemaField extends SchemaField {

    // Overridde for @NotNull annotation
    @NotNull(message = "complex fields must have a name")
    @Override
    public String getName() {
        return super.getName();
    }

    public abstract Map<String, SimpleSchemaField> getSubFields();

// Cloneable

    @Override
    public ComplexSchemaField clone() {
        return (ComplexSchemaField)super.clone();
    }
}

