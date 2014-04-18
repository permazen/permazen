
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.util;

import java.util.Arrays;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

/**
 * Support superclass for classes that serialize and deserialize via XML.
 */
public abstract class AbstractXMLStreaming {

    protected AbstractXMLStreaming() {
    }

    /**
     * Scan forward until we see an opening or closing tag.
     * If opening tag is seen, it must match one of {@code names} and then we return true, if not or if {@code names}
     * is empty throw an exception; if a closing tag is seen, return false if {@code closingOK}, else throw exception.
     *
     * @param reader XML input
     * @param closingOK true if a closing tag is OK, otherwise false
     * @param names expected opening tag, or null if we expected a closing tag
     * @return true if matching opening tag seen, false otherwise
     */
    protected boolean expect(XMLStreamReader reader, boolean closingOK, QName... names) throws XMLStreamException {
        final String desc = names.length == 1 ? names[0] + " tag" : "one of " + Arrays.asList(names) + " tags";
        while (true) {
            if (!reader.hasNext())
                throw new XMLStreamException("unexpected end of input", reader.getLocation());
            final int eventType = reader.next();
            if (eventType == XMLStreamConstants.START_ELEMENT)
                break;
            if (eventType == XMLStreamConstants.END_ELEMENT) {
                if (!closingOK) {
                    throw new XMLStreamException("expected opening " + desc + " but found closing "
                      + reader.getName() + " tag instead", reader.getLocation());
                }
                return false;
            }
        }
        if (names.length == 0) {
            throw new XMLStreamException("expected closing tag but found opening "
              + reader.getName() + " tag instead", reader.getLocation());
        }
        if (!Arrays.asList(names).contains(reader.getName())) {
            throw new XMLStreamException("expected opening " + desc
              + " but found " + reader.getName() + " instead", reader.getLocation());
        }
        return true;
    }

    /**
     * Write out a simple XML element containing the given content.
     *
     * @param writer XML output
     * @param element element name
     * @param content simple content
     */
    protected void writeElement(XMLStreamWriter writer, QName element, String content) throws XMLStreamException {
        writer.writeStartElement(element.getNamespaceURI(), element.getLocalPart());
        writer.writeCharacters(content);
        writer.writeEndElement();
    }
}

