
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.dellroad.stuff.xml.IndentXMLStreamWriter;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.util.AbstractXMLStreaming;
import org.jsimpledb.util.ByteUtil;

/**
 * Utility methods for serializing and deserializing the contents of a {@link org.jsimpledb.kv.KVStore} to/from XML.
 *
 * <p>
 * The XML has a simple format; empty values may be omitted:
 *  <pre>
 *  &lt;xml version="1.0" encoding="UTF-8"?&gt;
 *
 *  &lt;entries&gt;
 *      &lt;entry&gt;
 *          &lt;key&gt;013f7b&lt;/key&gt;
 *          &lt;value&gt;5502&lt;/value&gt;
 *      &lt;/entry&gt;
 *      &lt;entry&gt;
 *          &lt;key&gt;ee7698&lt;/key&gt;
 *      &lt;/entry&gt;
 *      ...
 *  &lt;/entries&gt;
 *  </pre>
 * </p>
 */
public class XMLSerializer extends AbstractXMLStreaming {

    public static final QName ENTRIES_TAG = new QName("entries");
    public static final QName ENTRY_TAG = new QName("entry");
    public static final QName KEY_TAG = new QName("key");
    public static final QName VALUE_TAG = new QName("value");

    private final KVStore kv;

    /**
     * Constructor.
     *
     * @param kv key/value store on which to operate
     */
    public XMLSerializer(KVStore kv) {
        if (kv == null)
            throw new IllegalArgumentException("null kv");
        this.kv = kv;
    }

    /**
     * Import key/value pairs into the {@link KVStore} associated with this instance from the given XML input.
     *
     * @param input XML input
     * @throws XMLStreamException if an error occurs
     */
    public void read(InputStream input) throws XMLStreamException {
        this.read(XMLInputFactory.newFactory().createXMLStreamReader(input));
    }

    /**
     * Import key/value pairs into the {@link KVStore} associated with this instance from the given XML input.
     * This method expects to see an opening {@code <entries>} as the next event (not counting whitespace, comments, etc.),
     * which is then consumed up through the closing {@code </entries>} event. Therefore this tag could be part of a
     * larger XML document.
     *
     * @param reader XML reader
     * @throws XMLStreamException if an error occurs
     */
    public void read(XMLStreamReader reader) throws XMLStreamException {
        this.expect(reader, false, ENTRIES_TAG);
        while (this.expect(reader, true, ENTRY_TAG)) {
            this.expect(reader, false, KEY_TAG);
            byte[] key;
            try {
                key = ByteUtil.parse(reader.getElementText());
            } catch (IllegalArgumentException e) {
                throw new XMLStreamException("invalid hexadecimal key", reader.getLocation(), e);
            }
            if (!this.expect(reader, true, VALUE_TAG)) {
                this.kv.put(key, new byte[0]);
                continue;                           // closing </entry> tag alread read
            }
            byte[] value;
            try {
                value = ByteUtil.parse(reader.getElementText());
            } catch (IllegalArgumentException e) {
                throw new XMLStreamException("invalid hexadecimal value", reader.getLocation(), e);
            }
            this.kv.put(key, value);
            this.expect(reader, true);              // read closing </entry> tag
        }
    }

    /**
     * Export all key/value pairs from the {@link KVStore} associated with this instance to the given XML output.
     *
     * @param output XML output; will not be closed by this method
     * @param indent true to indent output, false for all on one line
     * @throws XMLStreamException if an error occurs
     */
    public void write(OutputStream output, boolean indent) throws XMLStreamException {
        XMLStreamWriter writer = XMLOutputFactory.newInstance().createXMLStreamWriter(output, "UTF-8");
        if (indent)
            writer = new IndentXMLStreamWriter(writer);
        writer.writeStartDocument("UTF-8", "1.0");
        this.write(writer, null, null);
    }

    /**
     * Export a range of key/value pairs from the {@link KVStore} associated with this instance to the given XML output.
     *
     * <p>
     * This method writes a start element as its first action, allowing the output to be embedded into a larger XML document.
     * Callers not embedding the output may with to precede invocation of this method with a call to
     * {@link XMLStreamWriter#writeStartDocument writer.writeStartDocument()}.
     * </p>
     *
     * @param writer XML writer; will not be closed by this method
     * @param minKey minimum key (inclusive), or null for none
     * @param maxKey maximum key (exclusive), or null for none
     * @throws XMLStreamException if an error occurs
     */
    public void write(XMLStreamWriter writer, byte[] minKey, byte[] maxKey) throws XMLStreamException {
        writer.setDefaultNamespace(ENTRIES_TAG.getNamespaceURI());
        writer.writeStartElement(ENTRIES_TAG.getNamespaceURI(), ENTRIES_TAG.getLocalPart());
        for (Iterator<KVPair> i = this.kv.getRange(minKey, maxKey, false); i.hasNext(); ) {
            writer.writeStartElement(ENTRY_TAG.getNamespaceURI(), ENTRY_TAG.getLocalPart());
            final KVPair pair = i.next();
            this.writeElement(writer, KEY_TAG, pair.getKey());
            final byte[] value = pair.getValue();
            if (value.length > 0)
                this.writeElement(writer, VALUE_TAG, value);
            writer.writeEndElement();
        }
        writer.writeEndElement();
        writer.flush();
    }

// Internal methods

    private void writeElement(XMLStreamWriter writer, QName element, byte[] value) throws XMLStreamException {
        this.writeElement(writer, element, ByteUtil.toString(value));
    }
}

