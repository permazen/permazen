
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

import javax.xml.namespace.QName;

/**
 * XML tag and element names.
 */
public interface XMLConstants {

    QName COUNTER_FIELD_TAG = new QName("CounterField");
    QName LIST_FIELD_TAG = new QName("ListField");
    QName MAP_FIELD_TAG = new QName("MapField");
    QName OBJECT_TAG = new QName("Object");
    QName REFERENCE_FIELD_TAG = new QName("ReferenceField");
    QName ENUM_FIELD_TAG = new QName("EnumField");
    QName IDENTIFIER_TAG = new QName("Identifier");
    QName SCHEMA_MODEL_TAG = new QName("Schema");
    QName SET_FIELD_TAG = new QName("SetField");
    QName SIMPLE_FIELD_TAG = new QName("SimpleField");

    QName INDEXED_ATTRIBUTE = new QName("indexed");
    QName NAME_ATTRIBUTE = new QName("name");
    QName ON_DELETE_ATTRIBUTE = new QName("onDelete");
    QName STORAGE_ID_ATTRIBUTE = new QName("storageId");
    QName TYPE_ATTRIBUTE = new QName("type");
}

