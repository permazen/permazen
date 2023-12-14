
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.NavigableMap;
import java.util.function.Consumer;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

/**
 * A complex field in one version of a {@link SchemaObjectType}.
 */
public abstract class ComplexSchemaField extends SchemaField {

    /**
     * Get the sub-fields of this field.
     *
     * @return list of sub-fields, which will be unmodifiable if this instance is locked down
     */
    public abstract NavigableMap<String, SimpleSchemaField> getSubFields();

// Recursion

    @Override
    public void visitSchemaItems(Consumer<? super SchemaItem> visitor) {
        super.visitSchemaItems(visitor);
        this.getSubFields().values().forEach(subField -> subField.visitSchemaItems(visitor));
    }

// Lockdown

    @Override
    public void lockDown() {
        super.lockDown();
        this.getSubFields().values().forEach(field -> field.setParent(this));
        this.lockDownMap(this.getSubFields());
    }

// Validation

    @Override
    void validate() {
        super.validate();

        // Verify mapped field names
        this.verifyMappedNames("sub-field", this.getSubFields());

        // Verify parent back references
        this.verifyBackReferences("parent field", this.getSubFields(), SimpleSchemaField::getParent);

        // Validate fields
        this.getSubFields().values().forEach(SimpleSchemaField::validate);
    }

// Schema ID

    @Override
    void writeSchemaIdHashData(DataOutputStream output, boolean forSchemaModel) throws IOException {
        super.writeSchemaIdHashData(output, forSchemaModel);
        for (SimpleSchemaField subField : this.getSubFields().values())
            subField.writeSchemaIdHashData(output, forSchemaModel);
    }

// XML Reading

    SimpleSchemaField readSubField(XMLStreamReader reader, int formatVersion, String name) throws XMLStreamException {
        final SimpleSchemaField field = this.readMappedType(reader, false, SchemaModel.SIMPLE_FIELD_TAG_MAP);
        field.readXML(reader, formatVersion, false);
        if (field.getName() == null)
            field.setName(name);
        field.setParent(this);
        return field;
    }

// XML Writing

    @Override
    void writeXML(XMLStreamWriter writer, boolean prettyPrint) throws XMLStreamException {
        final QName tag = this.getXMLTag();
        this.writeStartElement(writer, tag);
        this.writeAttributes(writer);
        if (prettyPrint)
            this.writeSchemaIdComment(writer);
        for (SimpleSchemaField subField : this.getSubFields().values())
            subField.writeXML(writer, prettyPrint, false);                      // omit (redundant) names for sub-fields
        writer.writeEndElement();
    }

    abstract QName getXMLTag();

// Cloneable

    @Override
    public ComplexSchemaField clone() {
        return (ComplexSchemaField)super.clone();
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final ComplexSchemaField that = (ComplexSchemaField)obj;
        return this.getSubFields().equals(that.getSubFields());
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.getSubFields().hashCode();
    }
}
