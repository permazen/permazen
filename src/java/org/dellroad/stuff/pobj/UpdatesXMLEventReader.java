
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

import org.dellroad.stuff.xml.AnnotatedXMLEventReader;

/**
 * {@link AnnotatedXMLEventReader} that reads the nested schema update list. Used by {@link PersistentObjectSchemaUpdater}.
 */
public class UpdatesXMLEventReader extends AnnotatedXMLEventReader {

    private ArrayList<String> updates;

    public UpdatesXMLEventReader(XMLEventReader inner) {
        super(inner);
    }

    /**
     * Get the updates gleaned from the scan.
     *
     * @return list of updates, or null if the document didn't contain an updates element
     */
    public List<String> getUpdates() {
        return this.updates;
    }

    @Override
    protected boolean readAnnotationElement(XMLEventReader reader) throws XMLStreamException {

        // Check element name
        XMLEvent event = reader.peek();
        if (!event.isStartElement() || !event.asStartElement().getName().equals(PersistentObjectSchemaUpdater.UPDATES_ELEMENT_NAME))
            return false;
        reader.nextEvent();

        // Read updates
        this.updates = new ArrayList<String>();
        while (true) {

            // Skip whitespace and comments
            AnnotatedXMLEventReader.skipWhiteSpace(reader, true);

            // Done with updates?
            if ((event = reader.nextEvent()).isEndElement())
                return true;

            // Read <update>
            if (!event.isStartElement()
              || !event.asStartElement().getName().equals(PersistentObjectSchemaUpdater.UPDATE_ELEMENT_NAME)) {
                throw new XMLStreamException("XML parse error: expected " + PersistentObjectSchemaUpdater.UPDATE_ELEMENT_NAME
                  + " but got " + this.describe(event), event.getLocation());
            }

            // Read update name
            final StringBuilder buf = new StringBuilder();
            for (event = reader.nextEvent(); event.isCharacters(); event = reader.nextEvent())
                buf.append(event.asCharacters().getData());
            this.updates.add(buf.toString());

            // Read </update>
            if (!event.isEndElement()) {
                throw new XMLStreamException("XML parse error: expected " + PersistentObjectSchemaUpdater.UPDATE_ELEMENT_NAME
                  + " closing tag but got " + this.describe(event), event.getLocation());
            }
        }
    }

    private String describe(XMLEvent event) {
        return event.getClass().getSimpleName() + "[" + event + "]";
    }
}

