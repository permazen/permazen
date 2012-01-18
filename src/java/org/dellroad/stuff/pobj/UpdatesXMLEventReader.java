
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
import javax.xml.stream.events.StartElement;
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
    protected boolean isAnnotationElement(StartElement event) {
        return event.getName().equals(XMLConstants.UPDATES_ELEMENT_NAME);
    }

    @Override
    protected void readAnnotationElement(StartElement startElement) throws XMLStreamException {
        this.updates = new ArrayList<String>();
        while (true) {

            // Skip to next event
            XMLEvent event = this.advance();

            // Skip whitespace and comments
            event = this.skipWhiteSpace(event, true);

            // Done with updates?
            if (event.isEndElement()) {
                this.advance();
                return;
            }

            // Get <update> start element event
            if (!event.isStartElement() || !event.asStartElement().getName().equals(XMLConstants.UPDATE_ELEMENT_NAME)) {
                throw new XMLStreamException("XML parse error: expected " + XMLConstants.UPDATE_ELEMENT_NAME
                  + " but got " + this.describe(event), event.getLocation());
            }

            // Get update name
            StringBuilder buf = new StringBuilder();
            for (event = this.advance(); event.isCharacters(); event = this.advance())
                buf.append(event.asCharacters().getData());
            this.updates.add(buf.toString());

            // Get <update> end element event
            if (!event.isEndElement()) {
                throw new XMLStreamException("XML parse error: expected " + XMLConstants.UPDATE_ELEMENT_NAME
                  + " closing tag but got " + this.describe(event), event.getLocation());
            }
        }
    }

    private String describe(XMLEvent event) {
        return event.getClass().getSimpleName() + "[" + event + "]";
    }
}

