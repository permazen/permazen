
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

/**
 * A field in a {@link SchemaObject}.
 */
public abstract class SchemaField extends AbstractSchemaItem {

// Object

    @Override
    public String toString() {
        return "field " + super.toString();
    }

// Cloneable

    @Override
    public SchemaField clone() {
        return (SchemaField)super.clone();
    }
}

