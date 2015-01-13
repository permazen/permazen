
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

    // Elements
    QName COMPOSITE_INDEX_TAG = new QName("CompositeIndex");
    QName COUNTER_FIELD_TAG = new QName("CounterField");
    QName ENUM_FIELD_TAG = new QName("EnumField");
    QName IDENTIFIER_TAG = new QName("Identifier");
    QName INDEXED_FIELD_TAG = new QName("IndexedField");
    QName LIST_FIELD_TAG = new QName("ListField");
    QName MAP_FIELD_TAG = new QName("MapField");
    QName OBJECT_TYPES_TAG = new QName("ObjectTypes");
    QName OBJECT_TYPE_TAG = new QName("ObjectType");
    QName REFERENCE_FIELD_TAG = new QName("ReferenceField");
    QName SCHEMA_MODEL_TAG = new QName("Schema");
    QName SET_FIELD_TAG = new QName("SetField");
    QName SIMPLE_FIELD_TAG = new QName("SimpleField");

    // Attributes
    QName ENCODING_SIGNATURE_ATTRIBUTE = new QName("encodingSignature");
    QName FORMAT_VERSION_ATTRIBUTE = new QName("formatVersion");
    QName INDEXED_ATTRIBUTE = new QName("indexed");
    QName NAME_ATTRIBUTE = new QName("name");
    QName ON_DELETE_ATTRIBUTE = new QName("onDelete");
    QName STORAGE_ID_ATTRIBUTE = new QName("storageId");
    QName TYPE_ATTRIBUTE = new QName("type");
}

