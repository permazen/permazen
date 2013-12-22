
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.xml;

import java.util.ArrayList;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * Wrapper for an underlying {@link XMLStreamWriter} that replaces consecutive opening and closing XML tags
 * with empty string content with a single self-closing tag.
 */
public class EmptyTagXMLStreamWriter extends StreamWriterDelegate {

    private final ArrayList<PendingAction> pendingList = new ArrayList<PendingAction>();

    private int depth;

    /**
     * Constructor.
     *
     * @param writer underlying writer
     */
    public EmptyTagXMLStreamWriter(XMLStreamWriter writer) {
        super(writer);
    }

    @Override
    public void writeStartElement(final String localName) throws XMLStreamException {
        this.flushPendingList();
        this.pendingList.add(new PendingAction() {

            @Override
            public void apply() throws XMLStreamException {
                EmptyTagXMLStreamWriter.super.writeStartElement(localName);
            }

            @Override
            public void applyBeforeEndElement() throws XMLStreamException {
                EmptyTagXMLStreamWriter.super.writeEmptyElement(localName);
            }
        });
        this.depth++;
    }

    @Override
    public void writeStartElement(final String namespaceURI, final String localName) throws XMLStreamException {
        this.flushPendingList();
        this.pendingList.add(new PendingAction() {

            @Override
            public void apply() throws XMLStreamException {
                EmptyTagXMLStreamWriter.super.writeStartElement(namespaceURI, localName);
            }

            @Override
            public void applyBeforeEndElement() throws XMLStreamException {
                EmptyTagXMLStreamWriter.super.writeEmptyElement(namespaceURI, localName);
            }
        });
        this.depth++;
    }

    @Override
    public void writeStartElement(final String prefix, final String namespaceURI, final String localName)
      throws XMLStreamException {
        this.flushPendingList();
        this.pendingList.add(new PendingAction() {

            @Override
            public void apply() throws XMLStreamException {
                EmptyTagXMLStreamWriter.super.writeStartElement(prefix, namespaceURI, localName);
            }

            @Override
            public void applyBeforeEndElement() throws XMLStreamException {
                EmptyTagXMLStreamWriter.super.writeEmptyElement(prefix, namespaceURI, localName);
            }
        });
        this.depth++;
    }

    @Override
    public void writeEmptyElement(String namespaceURI, String localName) throws XMLStreamException {
        this.flushPendingList();
        super.writeEmptyElement(namespaceURI, localName);
    }

    @Override
    public void writeEmptyElement(String prefix, String namespaceURI, String localName) throws XMLStreamException {
        this.flushPendingList();
        super.writeEmptyElement(prefix, namespaceURI, localName);
    }

    @Override
    public void writeEmptyElement(String localName) throws XMLStreamException {
        this.flushPendingList();
        super.writeEmptyElement(localName);
    }

    @Override
    public void writeAttribute(final String localName, final String value) throws XMLStreamException {
        if (this.pendingList.isEmpty())
            super.writeAttribute(localName, value);
        else {
            this.pendingList.add(new PendingAction() {
                @Override
                public void apply() throws XMLStreamException {
                    EmptyTagXMLStreamWriter.super.writeAttribute(localName, value);
                }
            });
        }
    }

    @Override
    public void writeAttribute(final String prefix, final String namespaceURI, final String localName, final String value)
      throws XMLStreamException {
        if (this.pendingList.isEmpty())
            super.writeAttribute(prefix, namespaceURI, localName, value);
        else {
            this.pendingList.add(new PendingAction() {
                @Override
                public void apply() throws XMLStreamException {
                    EmptyTagXMLStreamWriter.super.writeAttribute(prefix, namespaceURI, localName, value);
                }
            });
        }
    }

    @Override
    public void writeAttribute(final String namespaceURI, final String localName, final String value) throws XMLStreamException {
        if (this.pendingList.isEmpty())
            super.writeAttribute(namespaceURI, localName, value);
        else {
            this.pendingList.add(new PendingAction() {
                @Override
                public void apply() throws XMLStreamException {
                    EmptyTagXMLStreamWriter.super.writeAttribute(namespaceURI, localName, value);
                }
            });
        }
    }

    @Override
    public void writeNamespace(final String prefix, final String namespaceURI) throws XMLStreamException {
        if (this.pendingList.isEmpty())
            super.writeNamespace(prefix, namespaceURI);
        else {
            this.pendingList.add(new PendingAction() {
                @Override
                public void apply() throws XMLStreamException {
                    EmptyTagXMLStreamWriter.super.writeNamespace(prefix, namespaceURI);
                }
            });
        }
    }

    @Override
    public void writeDefaultNamespace(final String namespaceURI) throws XMLStreamException {
        if (this.pendingList.isEmpty())
            super.writeDefaultNamespace(namespaceURI);
        else {
            this.pendingList.add(new PendingAction() {
                @Override
                public void apply() throws XMLStreamException {
                    EmptyTagXMLStreamWriter.super.writeDefaultNamespace(namespaceURI);
                }
            });
        }
    }

    @Override
    public void writeComment(String data) throws XMLStreamException {
        this.flushPendingList();
        super.writeComment(data);
    }

    @Override
    public void writeProcessingInstruction(String target) throws XMLStreamException {
        this.flushPendingList();
        super.writeProcessingInstruction(target);
    }

    @Override
    public void writeProcessingInstruction(String target, String data) throws XMLStreamException {
        this.flushPendingList();
        super.writeProcessingInstruction(target, data);
    }

    @Override
    public void writeCData(String data) throws XMLStreamException {
        this.flushPendingList();
        super.writeCData(data);
    }

    @Override
    public void writeDTD(String dtd) throws XMLStreamException {
        this.flushPendingList();
        super.writeDTD(dtd);
    }

    @Override
    public void writeEntityRef(String name) throws XMLStreamException {
        this.flushPendingList();
        super.writeEntityRef(name);
    }

    @Override
    public void writeCharacters(String text) throws XMLStreamException {
        if (text.length() == 0)
            return;
        this.flushPendingList();
        super.writeCharacters(text);
    }

    @Override
    public void writeCharacters(char[] text, int start, int len) throws XMLStreamException {
        if (len == 0)
            return;
        this.flushPendingList();
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
        this.depth--;
        if (!this.pendingList.isEmpty()) {
            for (PendingAction action : this.pendingList)
                action.applyBeforeEndElement();
            this.pendingList.clear();
            return;
        }
        super.writeEndElement();
    }

    private void flushPendingList() throws XMLStreamException {
        for (PendingAction action : this.pendingList)
            action.apply();
        this.pendingList.clear();
    }

    private abstract class PendingAction {

        public abstract void apply() throws XMLStreamException;

        public void applyBeforeEndElement() throws XMLStreamException {
            this.apply();
        }
    }
}

