
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.xml;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Comment;
import javax.xml.stream.events.XMLEvent;
import javax.xml.stream.util.EventReaderDelegate;

/**
 * {@link XMLEventReader} that reads and removes an initial annotation element from an XML document.
 * The annotation element, if present, must be the first element inside the top-level document element.
 * {@link #readAnnotationElement readAnnotationElement()} must be provided by the subclass to determine
 * whether the first non-top element is the expected annotation element, and read it if so.
 * Whoever reads from this {@link AnnotatedXMLEventReader} instance class will not see the annotation element.
 *
 * <p>
 * This class can be used in combination with {@link AnnotatedXMLEventWriter} to transparently annotate XML documents.
 * </p>
 *
 * @see AnnotatedXMLEventWriter
 */
public abstract class AnnotatedXMLEventReader extends EventReaderDelegate {

    // State:
    //  0 = before document element
    //  1 = after document element but before annotation element (if any)
    //  2 = after annotation element (if any)
    private byte state;

    /**
     * Constructor.
     *
     * @param parent parent reader
     */
    public AnnotatedXMLEventReader(XMLEventReader parent) {
        super(parent);
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
     * Determine if the next event from the given {@link XMLEventReader} is the annotation element, and if so, read it.
     *
     * <p>
     * This method should invoke {@link #peek reader.peek()} to determine if the next event is the annotation
     * {@link javax.xml.stream.events.StartElement}; if it's not, this method should not read any events and immediately
     * return false. Otherwise, it should read the annotation {@link javax.xml.stream.events.StartElement} and all
     * subsequent events up <i>through</i> the matching {@link javax.xml.stream.events.EndElement} and return true.
     * </p>
     *
     * @param reader source from which the rest of the annotation element is to be read
     * @return false if the annotation element is not seen, true if seen and fully read
     */
    protected abstract boolean readAnnotationElement(XMLEventReader reader) throws XMLStreamException;

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

        // Sanity check
        assert this.state == 1;

        // Ignore leading whitespace, comments, and PI's
        if (this.isWhiteSpace(event) || event instanceof Comment || event.isProcessingInstruction())
            return event;

        // Anything else means we either we have found the annotation element or it is not there
        this.state++;
        if (!event.isStartElement() || !this.readAnnotationElement(this.getParent()))
            return event;

        // Skip whitespace after annotation element
        for (event = super.peek(); this.isWhiteSpace(event); event = super.peek())
            super.nextEvent();

        // Done
        return event;
    }

    private boolean isWhiteSpace(XMLEvent event) {
        return event.isCharacters() && event.asCharacters().isWhiteSpace();
    }
}

