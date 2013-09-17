
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.xml;

import java.util.Arrays;

import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

/**
 * Wrapper for an underlying {@link XMLEventWriter} that automatically adds indentation to the event stream.
 */
public class IndentXMLEventWriter implements XMLEventWriter {

    /**
     * The configured event factory for this instance.
     */
    protected final XMLEventFactory factory;

    private final XMLEventWriter writer;
    private final int indent;

    private int lastEvent = -1;
    private int depth;

    /**
     * Constructor.
     *
     * @param writer underlying writer
     * @param factory event factory
     * @param indent indent amount, or negative to not add any whitespace
     * @throws IllegalArgumentException if {@code writer} or {@code factory} is null
     */
    public IndentXMLEventWriter(XMLEventWriter writer, XMLEventFactory factory, int indent) {
        if (writer == null)
            throw new IllegalArgumentException("null writer");
        if (factory == null)
            throw new IllegalArgumentException("null factory");
        this.writer = writer;
        this.factory = factory;
        this.indent = indent;
    }

    /**
     * Convenience constructor. Equivalent to:
     * <blockquote>
     *  {@link #IndentXMLEventWriter(XMLEventWriter, XMLEventFactory, int)
     *      IndentXMLEventWriter(writer, XMLEventFactory.newFactory(), indent)}
     * </blockquote>
     *
     * @param writer underlying writer
     * @param indent indent amount, or negative to not add any whitespace
     * @throws IllegalArgumentException if {@code writer} or {@code factory} is null
     */
    public IndentXMLEventWriter(XMLEventWriter writer, int indent) {
        this(writer, XMLEventFactory.newFactory(), indent);
    }

    @Override
    public void add(XMLEvent event) throws XMLStreamException {
        switch (event.getEventType()) {
        case XMLEvent.START_ELEMENT:
            switch (lastEvent) {
            case XMLEvent.START_ELEMENT:
            case XMLEvent.END_ELEMENT:
                this.indent(this.depth);
                break;
            default:
                break;
            }
            this.depth++;
            break;
        case XMLEvent.END_ELEMENT:
            this.depth--;
            switch (lastEvent) {
            case XMLEvent.START_ELEMENT:
                break;
            case XMLEvent.END_ELEMENT:
                this.indent(this.depth);
                break;
            default:
                break;
            }
            break;
        default:
            break;
        }
        this.lastEvent = event.getEventType();
        this.writer.add(event);
    }

    @Override
    public void flush() throws XMLStreamException {
        this.writer.flush();
    }

    @Override
    public void close() throws XMLStreamException {
        this.writer.close();
    }

    @Override
    public void add(XMLEventReader reader) throws XMLStreamException {
        while (reader.hasNext())
            this.add(reader.nextEvent());
    }

    @Override
    public String getPrefix(String uri) throws XMLStreamException {
        return this.writer.getPrefix(uri);
    }

    @Override
    public void setPrefix(String prefix, String uri) throws XMLStreamException {
        this.writer.setPrefix(prefix, uri);
    }

    @Override
    public void setDefaultNamespace(String uri) throws XMLStreamException {
        this.writer.setDefaultNamespace(uri);
    }

    @Override
    public void setNamespaceContext(NamespaceContext context) throws XMLStreamException {
        this.writer.setNamespaceContext(context);
    }

    @Override
    public NamespaceContext getNamespaceContext() {
        return this.writer.getNamespaceContext();
    }

    /**
     * Emit a newline followed by indentation to the given depth.
     */
    protected void indent(int depth) throws XMLStreamException {
        if (this.indent < 0)
            return;
        char[] buf = new char[1 + depth * this.indent];
        Arrays.fill(buf, ' ');
        buf[0] = '\n';
        this.writer.add(this.factory.createIgnorableSpace(new String(buf)));
    }
}

