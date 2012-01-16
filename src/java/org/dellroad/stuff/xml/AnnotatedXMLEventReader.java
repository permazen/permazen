
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.xml;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Comment;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import javax.xml.stream.util.EventReaderDelegate;

/**
 * {@link XMLEventReader} that reads and removes an initial annotation element from an XML document.
 * The annotation element, if present, must be the first element inside the top-level document element.
 * When the annotation element is encountered, {@link #readAnnotationElement readAnnotationElement()} will be invoked.
 *
 * <p>
 * This class can be used in combination with {@link AnnotatedXMLEventWriter} to transparently annotate XML documents.
 *
 * @see AnnotatedXMLEventWriter
 */
public abstract class AnnotatedXMLEventReader extends EventReaderDelegate {

    // State:
    //  0 = before document element
    //  1 = after document element but before ananotation element (if any)
    //  2 = after annotation element (if any)
    private byte state;

    /**
     * Constructor.
     *
     * <p>
     * The given parameter should expect to read the XML document without the annotation element.
     *
     * @param inner nested reader
     */
    public AnnotatedXMLEventReader(XMLEventReader inner) {
        super(inner);
    }

    @Override
    public void setParent(XMLEventReader reader) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object next() {
        try {
            return this.nextEvent();
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public XMLEvent peek() throws XMLStreamException {
        XMLEvent event = super.peek();
        if (this.state == 1)
            event = this.checkAnnotation(event);
        return event;
    }

    @Override
    public XMLEvent nextEvent() throws XMLStreamException {
        switch (this.state) {
        case 0:
            XMLEvent event = super.nextEvent();
            if (event.isStartElement())
                this.state++;
            return event;
        case 1:
            this.peek();
            return super.nextEvent();
        default:
            return super.nextEvent();
        }
    }

    /**
     * Determine if the given event represents the start of the annotation element.
     */
    protected abstract boolean isAnnotationElement(StartElement event);

    /**
     * Read the annotation element.
     *
     * <p>
     * When this method is invoked, {@code event} represents the {@link StartElement} event for the annotation element
     * (i.e., {@link #isAnnotationElement isAnnotationElement(event)} has returned true); it is also the next event in the
     * pipeline, i.e., the next event returned by {@link #nextEvent}.
     *
     * <p>
     * The last event consumed by this method should be the {@link javax.xml.stream.events.EndElement} event for the
     * annotation element, and the event returned by {@link #peek} should be the next event after the
     * {@link javax.xml.stream.events.EndElement}.
     *
     * @param event the {@link StartElement} event for the annotation element
     */
    protected abstract void readAnnotationElement(StartElement event) throws XMLStreamException;

    /**
     * Skip whitespace.
     *
     * @param event the next event, as returned by {@link #peek}
     * @param comments whether to also skip comments
     * @return the next event, as returned by {@link #peek}, after skipping any leading whitespace (and, optionally, comments)
     */
    protected XMLEvent skipWhiteSpace(XMLEvent event, boolean comments) throws XMLStreamException {
        while (event.isCharacters() && event.asCharacters().isWhiteSpace() || (comments && event instanceof Comment)) {
            super.nextEvent();
            event = super.peek();
        }
        return event;
    }

    /**
     * Advance one event in the event stream.
     *
     * @return the next event, as returned by {@link #peek}, after the advance
     */
    protected XMLEvent advance() throws XMLStreamException {
        super.nextEvent();
        return super.peek();
    }

    /**
     * Scan for the annotation. We only allow whitespace and comments between the first {@link StartElement} and the
     * annotation {@link StartElement}. Whitespace after the annotation {@link javax.xml.stream.events.EndElement} is removed.
     *
     * Pre-condition:
     *  - Given event comes after the document element and is before or equal to updates start element
     *  - Given event is the current "peek event" for this stream
     *
     * Post-condition:
     *  - If event was updates start element, updates were consumed, else no change
     *  - If updates were consumed:
     *      - New "peek event" is whatever follows updates and trailing whitespace
     *      - State has advanced
     *  - Returned event is the (possibly new) "peek event"
     */
    private XMLEvent checkAnnotation(XMLEvent event) throws XMLStreamException {

        // Skip over leading whitespace and comments
        assert this.state == 1;
        if (event.isCharacters() && event.asCharacters().isWhiteSpace())
            return event;
        if (event instanceof Comment)
            return event;

        // Anything else means we either we have found the annotation element or it is not there
        this.state++;
        if (!event.isStartElement() || !this.isAnnotationElement(event.asStartElement()))
            return event;

        // It is there, so read it
        this.readAnnotationElement(event.asStartElement());

        // Skip whitespace after annotation element
        return this.skipWhiteSpace(super.peek(), false);
    }
}

