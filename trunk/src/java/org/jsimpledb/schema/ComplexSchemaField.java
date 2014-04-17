
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import java.util.Map;

import org.jsimpledb.core.InvalidSchemaException;

/**
 * A complex field in one version of a {@link SchemaObject}.
 */
public abstract class ComplexSchemaField extends SchemaField {

    @Override
    public void validate() {
        super.validate();
        if (this.getName() == null || this.getName().length() == 0)
            throw new InvalidSchemaException(this + " must specify a name");
        for (Map.Entry<String, SimpleSchemaField> entry : this.getSubFields().entrySet()) {
            final String subFieldName = entry.getKey();
            final SimpleSchemaField subField = entry.getValue();
            if (subField == null)
                throw new InvalidSchemaException("invalid " + this + ": missing sub-field `" + subFieldName + "'");
            if (subField.getName() != null)
                throw new InvalidSchemaException("sub-" + subField + " of " + this + " must not specify a name");
            subField.validate();
        }
    }

    public abstract Map<String, SimpleSchemaField> getSubFields();

// Cloneable

    @Override
    public ComplexSchemaField clone() {
        return (ComplexSchemaField)super.clone();
    }
}

