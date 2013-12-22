
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.xml;

import java.util.Arrays;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * Wrapper for an underlying {@link XMLStreamWriter} that automatically adds indentation to the event stream.
 * It also sets add either/both of {@link #DEFAULT_ENCODING} and {@link #DEFAULT_VERSION} to the initial XML
 * declaration if they are missing.
 */
public class IndentXMLStreamWriter extends StreamWriterDelegate {

    private static final String DEFAULT_ENCODING = "UTF-8";
    private static final String DEFAULT_VERSION = "1.0";

    private final String newline = System.getProperty("line.separator", "\\n");
    private final int indent;

    private int lastEvent = -1;
    private int depth;

    /**
     * Constructor.
     *
     * @param writer underlying writer
     * @param indent indent amount, or negative to not add any whitespace
     */
    public IndentXMLStreamWriter(XMLStreamWriter writer, int indent) {
        super(writer);
        this.indent = indent;
    }

    @Override
    public void writeStartDocument(String version) throws XMLStreamException {
        this.writeStartDocument(DEFAULT_ENCODING, version);
    }

    @Override
    public void writeStartDocument() throws XMLStreamException {
        this.writeStartDocument(DEFAULT_ENCODING, DEFAULT_VERSION);
    }

    @Override
    public void writeStartElement(String localName) throws XMLStreamException {
        this.handleOpen(false);
        super.writeStartElement(localName);
    }

    @Override
    public void writeStartElement(String namespaceURI, String localName) throws XMLStreamException {
        this.handleOpen(false);
        super.writeStartElement(namespaceURI, localName);
    }

    @Override
    public void writeStartElement(String prefix, String namespaceURI, String localName) throws XMLStreamException {
        this.handleOpen(false);
        super.writeStartElement(prefix, namespaceURI, localName);
    }

    @Override
    public void writeEmptyElement(String namespaceURI, String localName) throws XMLStreamException {
        this.handleOpen(true);
        super.writeEmptyElement(namespaceURI, localName);
    }

    @Override
    public void writeEmptyElement(String prefix, String namespaceURI, String localName) throws XMLStreamException {
        this.handleOpen(true);
        super.writeEmptyElement(prefix, namespaceURI, localName);
    }

    @Override
    public void writeEmptyElement(String localName) throws XMLStreamException {
        this.handleOpen(true);
        super.writeEmptyElement(localName);
    }

    @Override
    public void writeComment(String data) throws XMLStreamException {
        this.lastEvent = -1;
        super.writeComment(data);
    }

    @Override
    public void writeProcessingInstruction(String target) throws XMLStreamException {
        this.lastEvent = -1;
        super.writeProcessingInstruction(target);
    }

    @Override
    public void writeProcessingInstruction(String target, String data) throws XMLStreamException {
        this.lastEvent = -1;
        super.writeProcessingInstruction(target, data);
    }

    @Override
    public void writeCData(String data) throws XMLStreamException {
        this.lastEvent = -1;
        super.writeCData(data);
    }

    @Override
    public void writeDTD(String dtd) throws XMLStreamException {
        this.lastEvent = -1;
        super.writeDTD(dtd);
    }

    @Override
    public void writeEntityRef(String name) throws XMLStreamException {
        this.lastEvent = -1;
        super.writeEntityRef(name);
    }

    @Override
    public void writeCharacters(String text) throws XMLStreamException {
        this.lastEvent = -1;
        super.writeCharacters(text);
    }

    @Override
    public void writeCharacters(char[] text, int start, int len) throws XMLStreamException {
        this.lastEvent = -1;
        super.writeCharacters(text, start, len);
    }

    @Override
    public void writeEndDocument() throws XMLStreamException {
        while (this.depth > 0) {
            this.writeEndElement();
            this.depth--;
        }
        super.writeEndDocument();
    }

    @Override
    public void writeEndElement() throws XMLStreamException {
        this.depth = Math.max(this.depth - 1, 0);
        if (this.lastEvent == XMLStreamConstants.END_ELEMENT)
            this.indent(this.depth);
        this.lastEvent = XMLStreamConstants.END_ELEMENT;
        super.writeEndElement();
    }

    private void handleOpen(boolean selfClosing) throws XMLStreamException {
        if (this.lastEvent == XMLStreamConstants.START_ELEMENT || this.lastEvent == XMLStreamConstants.END_ELEMENT)
            this.indent(this.depth);
        if (selfClosing)
            this.lastEvent = XMLStreamConstants.END_ELEMENT;
        else {
            this.lastEvent = XMLStreamConstants.START_ELEMENT;
            this.depth++;
        }
    }

    /**
     * Emit a newline followed by indentation to the given depth.
     */
    protected void indent(int depth) throws XMLStreamException {
        if (this.indent < 0)
            return;
        char[] buf = new char[depth * this.indent];
        Arrays.fill(buf, ' ');
        super.writeCharacters(this.newline + new String(buf));
    }
}

