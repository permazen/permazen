
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

/**
 * A list field in one version of a {@link SchemaObject}.
 */
public class ListSchemaField extends CollectionSchemaField {

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseListSchemaField(this);
    }

// Object

    @Override
    public String toString() {
        return "list " + super.toString();
    }

// Cloneable

    @Override
    public ListSchemaField clone() {
        return (ListSchemaField)super.clone();
    }
}

