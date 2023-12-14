
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import io.permazen.util.DiffGenerating;
import io.permazen.util.Diffs;

import javax.xml.namespace.QName;

/**
 * A list field in one version of a {@link SchemaObjectType}.
 */
public class ListSchemaField extends CollectionSchemaField implements DiffGenerating<ListSchemaField> {

    /**
     * The {@link ItemType} that this class represents.
     */
    public static final ItemType ITEM_TYPE = ItemType.LIST_FIELD;

// SchemaFieldSwitch

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseListSchemaField(this);
    }

// XML Writing

    @Override
    QName getXMLTag() {
        return XMLConstants.LIST_FIELD_TAG;
    }

// Schema ID

    @Override
    public final ItemType getItemType() {
        return ITEM_TYPE;
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
