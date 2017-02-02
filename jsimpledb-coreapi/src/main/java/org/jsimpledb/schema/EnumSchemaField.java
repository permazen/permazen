
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.schema;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.string.StringEncoder;
import org.jsimpledb.core.EnumFieldType;
import org.jsimpledb.core.InvalidSchemaException;
import org.jsimpledb.util.Diffs;

/**
 * An enum field in a {@link SchemaObjectType}.
 */
public class EnumSchemaField extends SimpleSchemaField {

    private /*final*/ List<String> idents = new ArrayList<>();

    /**
     * Get the ordered list of identifiers.
     *
     * @return enum identifier list
     */
    public List<String> getIdentifiers() {
        return this.idents;
    }

// Lockdown

    @Override
    void lockDownRecurse() {
        super.lockDownRecurse();
        this.idents = Collections.unmodifiableList(this.idents);
    }

// SchemaFieldSwitch

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseEnumSchemaField(this);
    }

// Validation

    @Override
    void validate() {
        super.validate();
        if (this.getEncodingSignature() != 0)
            throw new IllegalArgumentException("invalid " + this + ": encoding signature must be zero");
        try {
            EnumFieldType.validateIdentifiers(this.idents);
        } catch (IllegalArgumentException e) {
            throw new InvalidSchemaException("invalid " + this + ": " + e.getMessage(), e);
        }
    }

    @Override
    void validateType() {
        // we ignore the type
    }

// Compatibility

    // For enum types, we don't care if the type names are different; this allows enum types
    // to change their Java class or packge names without creating an incompatible schema.
    @Override
    boolean isCompatibleType(SimpleSchemaField field) {
        final EnumSchemaField that = (EnumSchemaField)field;
        return this.idents.equals(that.idents);
    }

    @Override
    void writeFieldTypeCompatibilityHashData(DataOutputStream output) throws IOException {
        output.writeInt(this.idents.size());
        for (String ident : this.idents)
            output.writeUTF(ident);
    }

// XML Reading

    @Override
    void readSubElements(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        while (this.expect(reader, true, XMLConstants.IDENTIFIER_TAG))
            this.idents.add(reader.getElementText());
    }

// XML Writing

    @Override
    void writeXML(XMLStreamWriter writer, boolean includeName) throws XMLStreamException {
        writer.writeStartElement(XMLConstants.ENUM_FIELD_TAG.getNamespaceURI(), XMLConstants.ENUM_FIELD_TAG.getLocalPart());
        this.writeAttributes(writer, includeName);
        for (String ident : this.idents) {
            writer.writeStartElement(XMLConstants.IDENTIFIER_TAG.getNamespaceURI(), XMLConstants.IDENTIFIER_TAG.getLocalPart());
            writer.writeCharacters(StringEncoder.encode(ident, false));
            writer.writeEndElement();
        }
        writer.writeEndElement();
    }

    @Override
    void writeTypeAttribute(XMLStreamWriter writer) throws XMLStreamException {
        // we ignore the type
    }

// DiffGenerating

    @Override
    public Diffs differencesFrom(SimpleSchemaField other) {
        final Diffs diffs = new Diffs(super.differencesFrom(other));
        if (!(other instanceof EnumSchemaField)) {
            diffs.add("change type from " + other.getClass().getSimpleName() + " to " + this.getClass().getSimpleName());
            return diffs;
        }
        final EnumSchemaField that = (EnumSchemaField)other;
        if (!this.idents.equals(that.idents)) {
            final Diffs enumDiffs = new Diffs();
            final TreeMap<String, Integer> thisOrdinals = new TreeMap<>();
            final TreeMap<String, Integer> thatOrdinals = new TreeMap<>();
            for (int i = 0; i < this.idents.size(); i++)
                thisOrdinals.put(this.idents.get(i), i);
            for (int i = 0; i < that.idents.size(); i++)
                thatOrdinals.put(that.idents.get(i), i);
            final PeekingIterator<String> thisIterator = Iterators.peekingIterator(thisOrdinals.keySet().iterator());
            final PeekingIterator<String> thatIterator = Iterators.peekingIterator(thatOrdinals.keySet().iterator());
            while (thisIterator.hasNext() || thatIterator.hasNext()) {
                final String thisName = thisIterator.hasNext() ? thisIterator.peek() : null;
                final String thatName = thatIterator.hasNext() ? thatIterator.peek() : null;
                assert thisName != null || thatName != null;
                final int diff = thisName == null ? 1 : thatName == null ? -1 : thisName.compareTo(thatName);
                if (diff < 0)
                    enumDiffs.add("added `" + thisName + "' (ordinal " + thisOrdinals.get(thisName) + ")");
                else
                    thatIterator.next();
                if (diff > 0)
                    enumDiffs.add("removed `" + thatName + "' (ordinal " + thatOrdinals.get(thatName) + ")");
                else
                    thisIterator.next();
            }
            diffs.add("changed enum identifier list", enumDiffs);
        }
        return diffs;
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final EnumSchemaField that = (EnumSchemaField)obj;
        return this.idents.equals(that.idents);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.idents.hashCode();
    }

// Cloneable

    @Override
    public EnumSchemaField clone() {
        final EnumSchemaField clone = (EnumSchemaField)super.clone();
        clone.idents = new ArrayList<>(this.idents);
        return clone;
    }
}

