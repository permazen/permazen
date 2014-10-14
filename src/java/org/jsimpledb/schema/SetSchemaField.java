
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import javax.xml.namespace.QName;

/**
 * A set field in one version of a {@link SchemaObjectType}.
 */
public class SetSchemaField extends CollectionSchemaField {

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseSetSchemaField(this);
    }

    @Override
    QName getXMLTag() {
        return SET_FIELD_TAG;
    }

// Object

    @Override
    public String toString() {
        return "set " + super.toString();
    }

// Cloneable

    @Override
    public SetSchemaField clone() {
        return (SetSchemaField)super.clone();
    }
}

