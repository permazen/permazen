
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.dellroad.stuff.xml.AnnotatedXMLStreamReader;

/**
 * {@link AnnotatedXMLStreamReader} that reads the nested schema update list. Used by {@link PersistentObjectSchemaUpdater}.
 */
public class UpdatesXMLStreamReader extends AnnotatedXMLStreamReader {

    private ArrayList<String> updates;

    public UpdatesXMLStreamReader(XMLStreamReader inner) {
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
    protected boolean readAnnotationElement(XMLStreamReader reader) throws XMLStreamException {

        // Check element name
        if (reader.getEventType() != START_ELEMENT || !reader.getName().equals(PersistentObjectSchemaUpdater.UPDATES_ELEMENT_NAME))
            return false;

        // Read updates
        this.updates = new ArrayList<String>();
        while (true) {

            // Ignore leading whitespace, comments, and PI's
            int eventType = reader.next();
            while (reader.isWhiteSpace() || eventType == COMMENT || eventType == PROCESSING_INSTRUCTION)
                eventType = reader.next();

            // Done with updates?
            if (reader.isEndElement())
                return true;

            // Read <update>text</update>
            reader.require(START_ELEMENT, PersistentObjectSchemaUpdater.UPDATE_ELEMENT_NAME.getNamespaceURI(),
              PersistentObjectSchemaUpdater.UPDATE_ELEMENT_NAME.getLocalPart());
            this.updates.add(reader.getElementText());
        }
    }
}

