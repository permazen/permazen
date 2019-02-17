
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import javax.xml.namespace.QName;

/**
 * XML tag and element names.
 */
public final class XMLConstants {

    // Elements
    public static final QName COMPOSITE_INDEX_TAG = new QName("CompositeIndex");
    public static final QName COUNTER_FIELD_TAG = new QName("CounterField");
    public static final QName ENUM_FIELD_TAG = new QName("EnumField");
    public static final QName ENUM_ARRAY_FIELD_TAG = new QName("EnumArrayField");
    public static final QName IDENTIFIER_TAG = new QName("Identifier");
    public static final QName INDEXED_FIELD_TAG = new QName("IndexedField");
    public static final QName LIST_FIELD_TAG = new QName("ListField");
    public static final QName MAP_FIELD_TAG = new QName("MapField");
    public static final QName OBJECT_TYPES_TAG = new QName("ObjectTypes");
    public static final QName OBJECT_TYPE_TAG = new QName("ObjectType");
    public static final QName REFERENCE_FIELD_TAG = new QName("ReferenceField");
    public static final QName SCHEMA_MODEL_TAG = new QName("Schema");
    public static final QName SET_FIELD_TAG = new QName("SetField");
    public static final QName SIMPLE_FIELD_TAG = new QName("SimpleField");

    // Attributes
    public static final QName ALLOW_DELETED_ATTRIBUTE = new QName("allowDeleted");
    public static final QName ALLOW_DELETED_SNAPSHOT_ATTRIBUTE = new QName("allowDeletedSnapshot");
    public static final QName CASCADE_DELETE_ATTRIBUTE = new QName("cascadeDelete");
    public static final QName DIMENSIONS_ATTRIBUTE = new QName("dimensions");
    public static final QName ENCODING_SIGNATURE_ATTRIBUTE = new QName("encodingSignature");
    public static final QName FORMAT_VERSION_ATTRIBUTE = new QName("formatVersion");
    public static final QName INDEXED_ATTRIBUTE = new QName("indexed");
    public static final QName NAME_ATTRIBUTE = new QName("name");
    public static final QName ON_DELETE_ATTRIBUTE = new QName("onDelete");
    public static final QName STORAGE_ID_ATTRIBUTE = new QName("storageId");
    public static final QName TYPE_ATTRIBUTE = new QName("type");

    private XMLConstants() {
    }
}

