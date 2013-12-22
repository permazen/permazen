
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stax.StAXSource;

import org.w3c.dom.Document;

/**
 * Represents an open "transaction" on a {@link PersistentObject}'s persistent file.
 *
 * <p>
 * This class is used by {@link PersistentObjectSchemaUpdater} and would normally not be used directly.
 */
public class PersistentFileTransaction {

    private final XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();
    private final ArrayList<String> updates = new ArrayList<String>();
    private final String systemId;

    private Document current;

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
    public Document getData() {
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

        // Set up source and result
        final DOMSource source = new DOMSource(this.current, this.systemId);
        final DOMResult result = new DOMResult();
        result.setSystemId(this.systemId);

        // Apply transform
        transformer.transform(source, result);

        // Save result as the new current value
        this.current = (Document)result.getNode();
    }

    private void read(Source input) throws IOException, XMLStreamException {

        // Read in XML into memory, extracting and removing the updates list in the process
        final UpdatesXMLStreamReader reader = new UpdatesXMLStreamReader(this.xmlInputFactory.createXMLStreamReader(input));
        final StAXSource source = new StAXSource(reader);
        final DOMResult result = new DOMResult();
        result.setSystemId(this.systemId);
        try {
            TransformerFactory.newInstance().newTransformer().transform(source, result);
        } catch (TransformerException e) {
            throw new XMLStreamException("error reading XML input from " + this.systemId, e);
        }
        reader.close();

        // Was the update list found?
        final List<String> updateNames = reader.getUpdates();
        if (updateNames == null)
            throw new PersistentObjectException("XML file does not contain an updates list");

        // Save current content (without updates) and updates list
        this.current = (Document)result.getNode();
        this.updates.clear();
        this.updates.addAll(updateNames);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + this.systemId + "]";
    }
}

