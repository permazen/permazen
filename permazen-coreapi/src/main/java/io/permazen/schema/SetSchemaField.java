
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import io.permazen.util.DiffGenerating;
import io.permazen.util.Diffs;

import javax.xml.namespace.QName;

/**
 * A set field in one version of a {@link SchemaObjectType}.
 */
public class SetSchemaField extends CollectionSchemaField implements DiffGenerating<SetSchemaField> {

// SchemaFieldSwitch

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseSetSchemaField(this);
    }

// XML Writing

    @Override
    QName getXMLTag() {
        return XMLConstants.SET_FIELD_TAG;
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

