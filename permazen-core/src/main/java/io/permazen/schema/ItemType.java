
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import javax.xml.namespace.QName;

/**
 * The various types of items that constitute a {@link SchemaModel}.
 */
public enum ItemType {
    SCHEMA_MODEL("Schema", XMLConstants.SCHEMA_MODEL_TAG, false),
    OBJECT_TYPE("ObjectType", XMLConstants.OBJECT_TYPE_TAG, false),
    SIMPLE_FIELD("SimpleField", XMLConstants.SIMPLE_FIELD_TAG, true),
    REFERENCE_FIELD("ReferenceField", XMLConstants.REFERENCE_FIELD_TAG, true),
    ENUM_FIELD("EnumField", XMLConstants.ENUM_FIELD_TAG, true),
    ENUM_ARRAY_FIELD("EnumArrayField", XMLConstants.ENUM_ARRAY_FIELD_TAG, true),
    COUNTER_FIELD("CounterField", XMLConstants.COUNTER_FIELD_TAG, true),
    SET_FIELD("SetField", XMLConstants.SET_FIELD_TAG, true),
    LIST_FIELD("ListField", XMLConstants.LIST_FIELD_TAG, true),
    MAP_FIELD("MapField", XMLConstants.MAP_FIELD_TAG, true),
    COMPOSITE_INDEX("CompositeIndex", XMLConstants.COMPOSITE_INDEX_TAG, false);

    private final String typeCode;
    private final QName elementName;
    private final boolean fieldItemType;

    ItemType(String typeCode, QName elementName, boolean fieldItemType) {
        this.typeCode = typeCode;
        this.elementName = elementName;
        this.fieldItemType = fieldItemType;
    }

    /**
     * Get the type code, which prefixes generated {@link SchemaId}s.
     *
     * @return type code for this schema item type
     */
    public String getTypeCode() {
        return this.typeCode;
    }

    /**
     * Get the XML element used for this item type.
     *
     * @return XML element name
     */
    public QName getElementName() {
        return this.elementName;
    }

    /**
     * Determine if this instance represents an object field item type.
     *
     * @return true for field item types, otherwise false
     */
    public boolean isFieldItemType() {
        return this.fieldItemType;
    }

    /**
     * Get instance that corresponds to the given type code.
     *
     * @return item type
     * @throws IllegalArgumentException if {@code typeCode} is null or invalid
     */
    public static ItemType forTypeCode(String typeCode) {
        for (ItemType itemType : ItemType.values()) {
            if (itemType.typeCode.equals(typeCode))
                return itemType;
        }
        throw new IllegalArgumentException(String.format("unknown item type code \"%s\"", typeCode));
    }
}
