
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.xml;

import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

/**
 * Support superclass for filtering {@link XMLEventWriter} implementations.
 * All methods delegate to the configured parent.
 */
public class EventWriterDelegate implements XMLEventWriter {

    private XMLEventWriter parent;

    /**
     * Default constructor. The parent must be configured via {@link #setParent}.
     */
    public EventWriterDelegate() {
    }

    /**
     * Constructor.
     *
     * @param parent underlying writer wrapped by this instance
     */
    public EventWriterDelegate(XMLEventWriter parent) {
        this.parent = parent;
    }

    /**
     * Get parent instance.
     */
    public XMLEventWriter getParent() {
        return this.parent;
    }

    /**
     * Set parent instance.
     */
    public void setParent(XMLEventWriter parent) {
        this.parent = parent;
    }

    @Override
    public void add(XMLEvent event) throws XMLStreamException {
        this.parent.add(event);
    }

    @Override
    public void flush() throws XMLStreamException {
        this.parent.flush();
    }

    @Override
    public void close() throws XMLStreamException {
        this.parent.close();
    }

    @Override
    public void add(XMLEventReader reader) throws XMLStreamException {
        this.parent.add(reader);
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
}

