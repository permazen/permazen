
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

/**
 * Represents an open "transaction" on a {@link PersistentObject}'s persistent file.
 *
 * <p>
 * This class is used by {@link PersistentObjectSchemaUpdater} and would normally not be used directly.
 */
public class PersistentFileTransaction {

    private static final int BUFFER_SIZE = 32 * 1024 - 32;

    private final XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();
    private final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();
    private final ArrayList<String> updates = new ArrayList<String>();
    private final String systemId;

    private byte[] current;

    /**
     * Constructor.
     *
     * @param source XML input
     * @throws PersistentObjectException if no updates are found
     */
    public PersistentFileTransaction(Source source) throws IOException, XMLStreamException {
        if (source == null)
            throw new IllegalArgumentException("null source");
        this.systemId = source.getSystemId();
        this.read(source);
    }

    /**
     * Get the current XML data. Does not include the XML update list.
     */
    public byte[] getData() {
        return this.current;
    }

    /**
     * Get the system ID of the original source input.
     */
    public String getSystemId() {
        return this.systemId;
    }

    /**
     * Get the updates list associated with this transaction.
     */
    public List<String> getUpdates() {
        return this.updates;
    }

    /**
     * Apply an XSLT transform to the current XML object in this transaction.
     *
     * @throws IllegalStateException if the current root object is null
     * @throws PersistentObjectException if an error occurs
     * @throws TransformerException if the transformation fails
     */
    public void transform(Transformer transformer) throws TransformerException {

        // Sanity check
        if (this.current == null)
            throw new PersistentObjectException("no data to transform");

        // Debug
        //System.out.println("************************** BEFORE TRANSFORM");
        //System.out.println(new String(this.current));

        // Set up source and result
        StreamSource source = new StreamSource(new ByteArrayInputStream(this.current));
        source.setSystemId(this.systemId);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(BUFFER_SIZE);
        StreamResult result = new StreamResult(buffer);
        result.setSystemId(this.systemId);

        // Apply transform
        transformer.transform(source, result);

        // Save result as the new current value
        this.current = buffer.toByteArray();

        // Debug
        //System.out.println("************************** AFTER TRANSFORM");
        //System.out.println(new String(this.current));
    }

    private void read(Source source) throws IOException, XMLStreamException {

        // Read in XML, extracting and removing the updates list in the process
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(BUFFER_SIZE);
        XMLEventWriter eventWriter = this.xmlOutputFactory.createXMLEventWriter(buffer);
        XMLEventReader eventReader = this.xmlInputFactory.createXMLEventReader(source);
        UpdatesXMLEventReader updatesReader = new UpdatesXMLEventReader(eventReader);
        eventWriter.add(updatesReader);
        eventWriter.close();
        eventReader.close();

        // Was the update list found?
        List<String> updateNames = updatesReader.getUpdates();
        if (updateNames == null)
            throw new PersistentObjectException("XML file does not contain an updates list");

        // Save current content (without updates) and updates list
        this.current = buffer.toByteArray();
        this.updates.clear();
        this.updates.addAll(updateNames);
    }
}

