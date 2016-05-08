
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.schema;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.ArrayList;
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

    @Override
    void validate() {
        super.validate();
        try {
            EnumFieldType.validateIdentifiers(this.idents);
        } catch (IllegalArgumentException e) {
            throw new InvalidSchemaException("invalid " + this + ": " + e.getMessage(), e);
        }
    }

    @Override
    public <R> R visit(SchemaFieldSwitch<R> target) {
        return target.caseEnumSchemaField(this);
    }

    // For enum types, we don't care if the type names are different; we only care if the identifier sets are different.
    // This allows enum types to change Java packages without creating an incompatible schema.
    @Override
    boolean isCompatibleWithInternal(AbstractSchemaItem that0) {
        final EnumSchemaField that = (EnumSchemaField)that0;
        if (this.isIndexed() != that.isIndexed())
            return false;
        if (!this.idents.equals(that.idents))
            return false;
        return true;
    }

    @Override
    void readSubElements(XMLStreamReader reader, int formatVersion) throws XMLStreamException {
        while (this.expect(reader, true, IDENTIFIER_TAG))
            this.idents.add(reader.getElementText());
    }

    @Override
    void writeXML(XMLStreamWriter writer, boolean includeName) throws XMLStreamException {
        writer.writeStartElement(ENUM_FIELD_TAG.getNamespaceURI(), ENUM_FIELD_TAG.getLocalPart());
        this.writeAttributes(writer, includeName);
        for (String ident : this.idents) {
            writer.writeStartElement(IDENTIFIER_TAG.getNamespaceURI(), IDENTIFIER_TAG.getLocalPart());
            writer.writeCharacters(StringEncoder.encode(ident, false));
            writer.writeEndElement();
        }
        writer.writeEndElement();
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

