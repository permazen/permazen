
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.xml.AnnotatedXMLStreamWriter;

/**
 * {@link AnnotatedXMLStreamWriter} that inserts the schema update list into the document
 * using an {@link PersistentObjectSchemaUpdater#UPDATES_ELEMENT_NAME} annotation element.
 * Used by {@link PersistentObjectSchemaUpdater}.
 */
public class UpdatesXMLStreamWriter extends AnnotatedXMLStreamWriter {

    private final List<String> updates;

    /**
     * Constructor.
     *
     * @param inner nested output
     * @param updates list of updates to add
     * @throws IllegalArgumentException if {@code updates} is null
     */
    public UpdatesXMLStreamWriter(XMLStreamWriter inner, List<String> updates) {
        super(inner);
        if (updates == null)
            throw new IllegalArgumentException("null updates");
        this.updates = updates;
    }

    @Override
    protected void addAnnotationElement(XMLStreamWriter writer) throws XMLStreamException {
        final QName updatesTag = PersistentObjectSchemaUpdater.UPDATES_ELEMENT_NAME;
        final QName updateTag = PersistentObjectSchemaUpdater.UPDATE_ELEMENT_NAME;
        if (this.updates.isEmpty()) {
            writer.writeEmptyElement(updatesTag.getPrefix(), updatesTag.getLocalPart(), updatesTag.getNamespaceURI());
            writer.writeNamespace(updatesTag.getPrefix(), updatesTag.getNamespaceURI());
        } else {
            writer.writeStartElement(updatesTag.getPrefix(), updatesTag.getLocalPart(), updatesTag.getNamespaceURI());
            writer.writeNamespace(updatesTag.getPrefix(), updatesTag.getNamespaceURI());
            final String space = this.getTrailingSpace();
            final String space2 = space.length() > 0 ? space + "    " : "";
            for (String updateName : this.updates) {
                writer.writeCharacters(space2);
                writer.writeStartElement(updateTag.getPrefix(), updateTag.getLocalPart(), updateTag.getNamespaceURI());
                writer.writeCharacters(updateName);
                writer.writeEndElement();
            }
            writer.writeCharacters(space);
            writer.writeEndElement();
        }
    }
}

