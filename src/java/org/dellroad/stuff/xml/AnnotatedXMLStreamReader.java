
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.xml;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.util.StreamReaderDelegate;

/**
 * {@link XMLStreamReader} that reads and removes an initial annotation element from an XML document.
 * The annotation element, if present, must be the first element inside the top-level document element.
 * When the annotation element is encountered, {@link #readAnnotationElement readAnnotationElement()} will be invoked.
 *
 * <p>
 * This class can be used in combination with {@link AnnotatedXMLStreamWriter} to transparently annotate XML documents.
 * </p>
 *
 * @see AnnotatedXMLStreamWriter
 */
public abstract class AnnotatedXMLStreamReader extends StreamReaderDelegate {

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
    public AnnotatedXMLStreamReader(XMLStreamReader parent) {
        super(parent);
    }

    @Override
    public int next() throws XMLStreamException {
        int eventType = super.next();
        switch (this.state) {
        case 0:
            if (eventType == START_ELEMENT)
                this.state++;
            break;
        case 1:
            eventType = this.checkAnnotation(eventType);
            break;
        default:
            break;
        }
        return eventType;
    }

    @Override
    public int nextTag() throws XMLStreamException {
        int eventType = this.next();
        while ((eventType == CHARACTERS && this.isWhiteSpace())
          || (eventType == CDATA && this.isWhiteSpace())
          || eventType == SPACE || eventType == PROCESSING_INSTRUCTION || eventType == COMMENT)
            eventType = this.next();
        if (eventType != START_ELEMENT && eventType != END_ELEMENT)
            throw new XMLStreamException("expected start or end tag", this.getLocation());
        return eventType;
    }

    /**
     * Determine if the given {@link XMLStreamReader} is positioned at the annotation element, and if so, read it.
     *
     * <p>
     * When this method is invoked, the {@code reader}'s current event has type {@link #START_ELEMENT} and may represent
     * the start of the annotation element. If it doesn't, this method should not read any events and immediately return false.
     * Otherwise, this method should read subsequent events up <i>through</i> the corresponding {@link #END_ELEMENT}
     * event and return true. Upon return, the {@code reader}'s current event will have type {@link #END_ELEMENT}.
     * </p>
     *
     * @param reader source from which the the annotation element is to be read, if found
     * @return true if the current event is the start of the annotation element and has been read, false otherwise
     */
    protected abstract boolean readAnnotationElement(XMLStreamReader reader) throws XMLStreamException;

    private int checkAnnotation(int eventType) throws XMLStreamException {

        // Sanity check
        assert this.state == 1;

        if (this.getParent().isWhiteSpace() || eventType == COMMENT || eventType == PROCESSING_INSTRUCTION)
            return eventType;

        // Anything else means we either we have found the annotation element or it is not there
        this.state++;
        if (eventType != START_ELEMENT || !this.readAnnotationElement(this.getParent()))
            return eventType;
        if (super.getEventType() != END_ELEMENT)
            throw new XMLStreamException("readAnnotationElement() did not stop on an END_ELEMENT event", this.getLocation());

        // Skip whitespace after annotation element
        do
            eventType = super.next();
        while (super.isWhiteSpace());

        // Done
        return eventType;
    }
}

