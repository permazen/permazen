
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import javax.xml.namespace.QName;

import org.jsimpledb.util.DiffGenerating;
import org.jsimpledb.util.Diffs;

/**
 * A list field in one version of a {@link SchemaObjectType}.
 */
public class ListSchemaField extends CollectionSchemaField implements DiffGenerating<ListSchemaField> {

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseListSchemaField(this);
    }

    @Override
    QName getXMLTag() {
        return LIST_FIELD_TAG;
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(ListSchemaField that) {
        return super.differencesFrom(that);
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

