
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.xml;

import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

/**
 * {@link XMLEventWriter} that adds an extra annotation element to an XML document as it is written.
 * The annotation element will be added as the first element inside the top-level document element.
 *
 * <p>
 * This class can be used in combination with {@link AnnotatedXMLEventReader} to transparently annotate XML documents.
 *
 * @see AnnotatedXMLEventReader
 */
public abstract class AnnotatedXMLEventWriter implements XMLEventWriter {

    protected final XMLEventFactory xmlEventFactory = XMLEventFactory.newFactory();

    private final StringBuilder trailingSpace = new StringBuilder();
    private final XMLEventWriter inner;

    // State:
    //  0 = before document element
    //  1 = after document element but before ananotation element
    //  2 = after annotation element
    private byte state;

    public AnnotatedXMLEventWriter(XMLEventWriter inner) {
        if (inner == null)
            throw new IllegalArgumentException("null inner");
        this.inner = inner;
    }

    @Override
    public void add(XMLEvent event) throws XMLStreamException {
        switch (this.state) {
        case 0:
            if (event.isStartElement())
                this.state++;
            this.inner.add(event);
            break;
        case 1:
            if (event.isCharacters() && event.asCharacters().isWhiteSpace()) {
                this.trailingSpace.append(event.asCharacters().getData());
                this.inner.add(event);
                break;
            }
            this.state++;
            this.addAnnotationElement();
            if (this.trailingSpace.length() > 0)
                this.inner.add(this.xmlEventFactory.createCharacters(this.trailingSpace.toString()));
            this.inner.add(event);
            break;
        case 2:
            this.inner.add(event);
            break;
        default:
            throw new RuntimeException("internal error");
        }
    }

    @Override
    public void add(XMLEventReader reader) throws XMLStreamException {
        if (reader == null)
            throw new XMLStreamException("null reader");
        while (reader.hasNext())
            this.add(reader.nextEvent());
    }

    @Override
    public void flush() throws XMLStreamException {
        this.inner.flush();
    }

    @Override
    public void close() throws XMLStreamException {
        this.inner.close();
    }

    @Override
    public String getPrefix(String uri) throws XMLStreamException {
        return this.inner.getPrefix(uri);
    }

    @Override
    public void setPrefix(String prefix, String uri) throws XMLStreamException {
        this.inner.setPrefix(prefix, uri);
    }

    @Override
    public void setDefaultNamespace(String uri) throws XMLStreamException {
        this.inner.setDefaultNamespace(uri);
    }

    @Override
    public void setNamespaceContext(NamespaceContext context) throws XMLStreamException {
        this.inner.setNamespaceContext(context);
    }

    @Override
    public NamespaceContext getNamespaceContext() {
        return this.inner.getNamespaceContext();
    }

    /**
     * Add the annotation element.
     *
     * <p>
     * This method should {@link #add add()} the {@link javax.xml.stream.events.StartElement} for the annotation element, followed
     * by any nested content, and then lastly the {@link javax.xml.stream.events.EndElement} for the annotation element.
     */
    protected abstract void addAnnotationElement() throws XMLStreamException;
}

