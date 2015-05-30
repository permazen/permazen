
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.schema;

import javax.xml.namespace.QName;

import org.jsimpledb.util.DiffGenerating;
import org.jsimpledb.util.Diffs;

/**
 * A set field in one version of a {@link SchemaObjectType}.
 */
public class SetSchemaField extends CollectionSchemaField implements DiffGenerating<SetSchemaField> {

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseSetSchemaField(this);
    }

    @Override
    QName getXMLTag() {
        return SET_FIELD_TAG;
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(SetSchemaField that) {
        return super.differencesFrom(that);
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

