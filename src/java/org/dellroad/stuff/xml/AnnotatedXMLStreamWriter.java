
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.xml;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * {@link XMLStreamWriter} that adds an extra annotation element to an XML document as it is written.
 * The annotation element will be added as the first element inside the top-level document element.
 *
 * <p>
 * This class can be used in combination with {@link AnnotatedXMLStreamReader} to transparently annotate XML documents.
 * </p>
 *
 * @see AnnotatedXMLStreamReader
 */
public abstract class AnnotatedXMLStreamWriter extends StreamWriterDelegate {

    private final StringBuilder trailingSpace = new StringBuilder();

    // State:
    //  0 = before document element
    //  1 = after document element but before annotation element
    //  2 = after annotation element
    private byte state;
    private boolean needEnding;

    public AnnotatedXMLStreamWriter(XMLStreamWriter parent) {
        super(parent);
    }

    @Override
    public void writeStartElement(String localName) throws XMLStreamException {
        this.checkStartElement();
        super.writeStartElement(localName);
    }

    @Override
    public void writeStartElement(String namespaceURI, String localName) throws XMLStreamException {
        this.checkStartElement();
        super.writeStartElement(namespaceURI, localName);
    }

    @Override
    public void writeStartElement(String prefix, String namespaceURI, String localName) throws XMLStreamException {
        this.checkStartElement();
        super.writeStartElement(prefix, namespaceURI, localName);
    }

    @Override
    public void writeEmptyElement(String namespaceURI, String localName) throws XMLStreamException {
        if (this.state == 0)
            this.writeStartElement(namespaceURI, localName);            // super.writeEndDocument() will add closing tag for us
        else
            super.writeEmptyElement(namespaceURI, localName);
    }

    @Override
    public void writeEmptyElement(String prefix, String namespaceURI, String localName) throws XMLStreamException {
        if (this.state == 0)
            this.writeStartElement(prefix, namespaceURI, localName);    // super.writeEndDocument() will add closing tag for us
        else
            super.writeEmptyElement(prefix, namespaceURI, localName);
    }

    @Override
    public void writeEmptyElement(String localName) throws XMLStreamException {
        if (this.state == 0)
            this.writeStartElement(localName);                          // super.writeEndDocument() will add closing tag for us
        else
            super.writeEmptyElement(localName);
    }

    @Override
    public void writeComment(String data) throws XMLStreamException {
        this.checkNonWhitespace();
        super.writeComment(data);
    }

    @Override
    public void writeProcessingInstruction(String target) throws XMLStreamException {
        this.checkNonWhitespace();
        super.writeProcessingInstruction(target);
    }

    @Override
    public void writeProcessingInstruction(String target, String data) throws XMLStreamException {
        this.checkNonWhitespace();
        super.writeProcessingInstruction(target, data);
    }

    @Override
    public void writeCData(String data) throws XMLStreamException {
        this.checkNonWhitespace();
        super.writeCData(data);
    }

    @Override
    public void writeDTD(String dtd) throws XMLStreamException {
        this.checkNonWhitespace();
        super.writeDTD(dtd);
    }

    @Override
    public void writeEntityRef(String name) throws XMLStreamException {
        this.checkNonWhitespace();
        super.writeEntityRef(name);
    }

    @Override
    public void writeCharacters(String text) throws XMLStreamException {
        this.checkWhitespace(text);
        super.writeCharacters(text);
    }

    @Override
    public void writeCharacters(char[] text, int start, int len) throws XMLStreamException {
        this.checkWhitespace(new String(text, start, len));
        super.writeCharacters(text, start, len);
    }

    @Override
    public void writeEndDocument() throws XMLStreamException {
        this.checkNonWhitespace();
        super.writeEndDocument();
    }

    @Override
    public void writeEndElement() throws XMLStreamException {
        this.checkNonWhitespace();
        super.writeEndElement();
    }

    private void checkStartElement() throws XMLStreamException {
        switch (this.state) {
        case 0:
            this.state++;
            break;
        case 1:
            this.checkNonWhitespace();
            break;
        default:
            break;
        }
    }

    private void checkNonWhitespace() throws XMLStreamException {
        if (this.state != 1)
            return;
        this.state++;
        this.addAnnotationElement(this.getParent());
        if (this.trailingSpace.length() > 0)
            super.writeCharacters(this.trailingSpace.toString());
    }

    private void checkWhitespace(String text) throws XMLStreamException {
        if (this.state != 1)
            return;
        if (text.trim().length() == 0)
            this.trailingSpace.append(text);
        else
            this.checkNonWhitespace();
    }

    /**
     * Get the whitespace found between the opening document tag and the first non-space child.
     */
    protected String getTrailingSpace() {
        return this.trailingSpace.toString();
    }

    /**
     * Add the annotation element.
     *
     * <p>
     * This method should write the start element, followed by by any nested content, and then lastly the
     * end element for the annotation element.
     * </p>
     *
     * @param writer output to which the annotation element should be written
     */
    protected abstract void addAnnotationElement(XMLStreamWriter writer) throws XMLStreamException;
}

