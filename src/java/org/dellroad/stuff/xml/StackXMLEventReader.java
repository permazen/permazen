
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.xml;

import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import javax.xml.stream.util.EventReaderDelegate;

/**
 * Wrapper for a {@link XMLEventReader} that keeps track of the current element(s) being parsed as a stack
 * of {@link StartElement} events.
 *
 * <p>
 * Each time a {@link StartElement} is read from the underlying {@link XMLEventReader}, it is pushed onto
 * the stack; each time a {@link javax.xml.stream.events.EndElement} is read from the underlying {@link XMLEventReader},
 * the previously push {@link StartElement} is popped off.
 * </p>
 *
 * <p>
 * Invocations of {@link #peek} do not affect the element stack.
 * </p>
 *
 * <p>
 * If an {@link XMLStreamException} is thrown at any point, the element stack is no longer guaranteed to track properly.
 * </p>
 */
public class StackXMLEventReader extends EventReaderDelegate {

    private final ArrayList<StartElement> stack = new ArrayList<StartElement>();

    /**
     * Constructor.
     *
     * @param reader underlying reader
     * @throws IllegalArgumentException if {@code reader} is null
     */
    public StackXMLEventReader(XMLEventReader reader) {
        super(reader);
        if (reader == null)
            throw new IllegalArgumentException("null reader");
    }

    /**
     * Get the top-most {@link StartElement} event on the stack. This corresponds to the innermost XML
     * element currently being read.
     *
     * @return current element, or null if the current parse position is outside of the document element
     */
    public StartElement getTopElement() {
        return !this.stack.isEmpty() ? this.stack.get(this.stack.size() - 1) : null;
    }

    /**
     * Get the entire stack of {@link StartElement}s.
     *
     * <p>
     * A copy is returned; changes to the returned list do not affect this instance.
     * </p>
     *
     * @return element stack; the first element in the list is the document element
     */
    public List<StartElement> getElementStack() {
        return new ArrayList<StartElement>(this.stack);
    }

    @Override
    public String getElementText() throws XMLStreamException {
        String text = super.getElementText();
        this.stack.remove(this.stack.size() - 1);
        return text;
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
    public XMLEvent nextEvent() throws XMLStreamException {
        return this.adjust(super.nextEvent());
    }

    @Override
    public XMLEvent nextTag() throws XMLStreamException {
        return this.adjust(super.nextTag());
    }

    /**
     * Adjust the stack after reading the given event.
     *
     * @param event newly read event
     * @return {@code event}
     */
    protected XMLEvent adjust(XMLEvent event) {
        switch (event.getEventType()) {
        case XMLEvent.START_ELEMENT:
            this.stack.add(event.asStartElement());
            break;
        case XMLEvent.END_ELEMENT:
            this.stack.remove(this.stack.size() - 1);
            break;
        default:
            break;
        }
        return event;
    }
}

