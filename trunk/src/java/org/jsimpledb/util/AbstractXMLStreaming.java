
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
        while (true) {
            if (!reader.hasNext())
                throw new XMLStreamException("unexpected end of input", reader.getLocation());
            final int eventType = reader.next();
            if (eventType == XMLStreamConstants.START_ELEMENT)
                break;
            if (eventType == XMLStreamConstants.END_ELEMENT) {
                if (!closingOK) {
                    throw new XMLStreamException("expected " + this.description(names) + " but found closing <"
                      + reader.getName() + "> tag instead", reader.getLocation());
                }
                return false;
            }
        }
        if (names.length == 0) {
            throw new XMLStreamException("expected closing tag but found opening <"
              + reader.getName() + "> tag instead", reader.getLocation());
        }
        if (!Arrays.asList(names).contains(reader.getName())) {
            throw new XMLStreamException("expected " + this.description(names)
              + " but found <" + reader.getName() + "> instead", reader.getLocation());
        }
        return true;
    }

    private String description(QName[] names) {
        switch (names.length) {
        case 0:
            return "closing tag";
        case 1:
            return "opening <" + names[0] + "> tag";
        default:
            final StringBuilder buf = new StringBuilder();
            for (QName name : names) {
                if (buf.length() == 0)
                    buf.append("one of ");
                else
                    buf.append(", ");
                buf.append('<').append(name).append('>');
            }
            return buf.toString();
        }
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

    /**
     * Get an attribute from the current element.
     *
     * @param reader XML input
     * @param name attribute name
     * @param required whether attribute must be present
     * @throws IllegalStateException if the current event is not a start element event
     * @throws IllegalArgumentException if {@code required} is true and no such attribute is found
     */
    protected String getAttr(XMLStreamReader reader, QName name, boolean required) {
        final String value = reader.getAttributeValue(name.getNamespaceURI(), name.getLocalPart());
        if (value == null && required) {
            throw new IllegalArgumentException("<" + reader.getName().getLocalPart() + "> element has no \"" + name
              + "\" attribute at " + reader.getLocation());
        }
        return value;
    }
}

