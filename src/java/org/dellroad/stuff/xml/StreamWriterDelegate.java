
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.xml;

import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * Support superclass for filtering {@link XMLStreamWriter} implementations.
 * All methods delegate to the configured parent.
 */
public class StreamWriterDelegate implements XMLStreamWriter {

    private XMLStreamWriter parent;

    /**
     * Default constructor. The parent must be configured via {@link #setParent}.
     */
    public StreamWriterDelegate() {
    }

    /**
     * Constructor.
     *
     * @param parent underlying writer wrapped by this instance
     */
    public StreamWriterDelegate(XMLStreamWriter parent) {
        this.parent = parent;
    }

    /**
     * Get parent instance.
     */
    public XMLStreamWriter getParent() {
        return this.parent;
    }

    /**
     * Set parent instance.
     */
    public void setParent(XMLStreamWriter parent) {
        this.parent = parent;
    }

    @Override
    public void writeStartElement(String localName) throws XMLStreamException {
        this.parent.writeStartElement(localName);
    }

    @Override
    public void writeStartElement(String namespaceURI, String localName) throws XMLStreamException {
        this.parent.writeStartElement(namespaceURI, localName);
    }

    @Override
    public void writeStartElement(String prefix, String namespaceURI, String localName) throws XMLStreamException {
        this.parent.writeStartElement(prefix, namespaceURI, localName);
    }

    @Override
    public void writeEmptyElement(String namespaceURI, String localName) throws XMLStreamException {
        this.parent.writeEmptyElement(namespaceURI, localName);
    }

    @Override
    public void writeEmptyElement(String prefix, String namespaceURI, String localName) throws XMLStreamException {
        this.parent.writeEmptyElement(prefix, namespaceURI, localName);
    }

    @Override
    public void writeEmptyElement(String localName) throws XMLStreamException {
        this.parent.writeEmptyElement(localName);
    }

    @Override
    public void writeEndElement() throws XMLStreamException {
        this.parent.writeEndElement();
    }

    @Override
    public void writeEndDocument() throws XMLStreamException {
        this.parent.writeEndDocument();
    }

    @Override
    public void close() throws XMLStreamException {
        this.parent.close();
    }

    @Override
    public void flush() throws XMLStreamException {
        this.parent.flush();
    }

    @Override
    public void writeAttribute(String localName, String value) throws XMLStreamException {
        this.parent.writeAttribute(localName, value);
    }

    @Override
    public void writeAttribute(String prefix, String namespaceURI, String localName, String value) throws XMLStreamException {
        this.parent.writeAttribute(prefix, namespaceURI, localName, value);
    }

    @Override
    public void writeAttribute(String namespaceURI, String localName, String value) throws XMLStreamException {
        this.parent.writeAttribute(namespaceURI, localName, value);
    }

    @Override
    public void writeNamespace(String prefix, String namespaceURI) throws XMLStreamException {
        this.parent.writeNamespace(prefix, namespaceURI);
    }

    @Override
    public void writeDefaultNamespace(String namespaceURI) throws XMLStreamException {
        this.parent.writeDefaultNamespace(namespaceURI);
    }

    @Override
    public void writeComment(String data) throws XMLStreamException {
        this.parent.writeComment(data);
    }

    @Override
    public void writeProcessingInstruction(String target) throws XMLStreamException {
        this.parent.writeProcessingInstruction(target);
    }

    @Override
    public void writeProcessingInstruction(String target, String data) throws XMLStreamException {
        this.parent.writeProcessingInstruction(target, data);
    }

    @Override
    public void writeCData(String data) throws XMLStreamException {
        this.parent.writeCData(data);
    }

    @Override
    public void writeDTD(String dtd) throws XMLStreamException {
        this.parent.writeDTD(dtd);
    }

    @Override
    public void writeEntityRef(String name) throws XMLStreamException {
        this.parent.writeEntityRef(name);
    }

    @Override
    public void writeStartDocument() throws XMLStreamException {
        this.parent.writeStartDocument();
    }

    @Override
    public void writeStartDocument(String version) throws XMLStreamException {
        this.parent.writeStartDocument(version);
    }

    @Override
    public void writeStartDocument(String encoding, String version) throws XMLStreamException {
        this.parent.writeStartDocument(encoding, version);
    }

    @Override
    public void writeCharacters(String text) throws XMLStreamException {
        this.parent.writeCharacters(text);
    }

    @Override
    public void writeCharacters(char[] text, int start, int len) throws XMLStreamException {
        this.parent.writeCharacters(text, start, len);
    }

    @Override
    public String getPrefix(String uri) throws XMLStreamException {
        return this.parent.getPrefix(uri);
    }

    @Override
    public void setPrefix(String prefix, String uri) throws XMLStreamException {
        this.parent.setPrefix(prefix, uri);
    }

    @Override
    public void setDefaultNamespace(String uri) throws XMLStreamException {
        this.parent.setDefaultNamespace(uri);
    }

    @Override
    public void setNamespaceContext(NamespaceContext context) throws XMLStreamException {
        this.parent.setNamespaceContext(context);
    }

    @Override
    public NamespaceContext getNamespaceContext() {
        return this.parent.getNamespaceContext();
    }

    @Override
    public Object getProperty(String name) {
        return this.parent.getProperty(name);
    }
}

