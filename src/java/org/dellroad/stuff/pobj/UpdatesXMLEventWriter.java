
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.util.List;

import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLStreamException;

import org.dellroad.stuff.xml.AnnotatedXMLEventWriter;

/**
 * {@link AnnotatedXMLEventWriter} that inserts the schema update list into the document
 * using an {@link PersistentObjectSchemaUpdater#UPDATES_ELEMENT_NAME} annotation element.
 * Used by {@link PersistentObjectSchemaUpdater}.
 */
public class UpdatesXMLEventWriter extends AnnotatedXMLEventWriter {

    private final List<String> updates;

    /**
     * Constructor.
     *
     * @param inner nested output
     * @param updates list of updates to add
     * @throws IllegalArgumentException if {@code updates} is null
     */
    public UpdatesXMLEventWriter(XMLEventWriter inner, List<String> updates) {
        super(inner);
        if (updates == null)
            throw new IllegalArgumentException("null updates");
        this.updates = updates;
    }

    @Override
    protected void addAnnotationElement(XMLEventWriter writer) throws XMLStreamException {
        writer.add(this.xmlEventFactory.createStartElement(PersistentObjectSchemaUpdater.UPDATES_ELEMENT_NAME, null, null));
        writer.add(this.xmlEventFactory.createAttribute(PersistentObjectSchemaUpdater.XMLNS_ATTRIBUTE_NAME,
          PersistentObjectSchemaUpdater.NAMESPACE_URI));
        final String space = this.getTrailingSpace();
        final String space2 = space.length() > 0 ? space + "    " : "";
        if (!this.updates.isEmpty()) {
            for (String updateName : this.updates) {
                writer.add(this.xmlEventFactory.createCharacters(space2));
                writer.add(this.xmlEventFactory.createStartElement(PersistentObjectSchemaUpdater.UPDATE_ELEMENT_NAME, null, null));
                writer.add(this.xmlEventFactory.createCharacters(updateName));
                writer.add(this.xmlEventFactory.createEndElement(PersistentObjectSchemaUpdater.UPDATE_ELEMENT_NAME, null));
            }
            writer.add(this.xmlEventFactory.createCharacters(space));
        }
        writer.add(this.xmlEventFactory.createEndElement(PersistentObjectSchemaUpdater.UPDATES_ELEMENT_NAME, null));
    }
}

