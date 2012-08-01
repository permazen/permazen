
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
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

    static final int FILE_BUFFER_SIZE = 32 * 1024 - 32;

    private final XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();
    private final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();
    private final ArrayList<String> updates = new ArrayList<String>();
    private final File file;

    private byte[] current;
    private boolean modified;

    /**
     * Constructor.
     *
     * @param file persistent file
     * @throws PersistentObjectException if no updates are found
     */
    public PersistentFileTransaction(File file) throws IOException, XMLStreamException {

        // Save file
        if (file == null)
            throw new IllegalStateException("null file");
        this.file = file;

        // Read in file if it exists
        if (file.exists())
            this.readFile();
    }

    /**
     * Get the current XML data. Does not include the XML update list.
     */
    public byte[] getData() {
        return this.current;
    }

    /**
     * Set the current XML data. Data should not include the XML update list.
     */
    public void setData(byte[] data) {
        this.current = data;
        this.modified = true;
    }

    /**
     * Commit this transaction. This results in the persistent file being atomically overwritten (including update list).
     * If no data was set, then no file is written.
     */
    public void commit() throws IOException, XMLStreamException {

        // Anything changed?
        if (!this.modified)
            return;

        // Write data with updates to temporary file
        File tempFile = null;
        BufferedOutputStream output = null;
        try {
            XMLEventReader eventReader = this.xmlInputFactory.createXMLEventReader(new ByteArrayInputStream(this.current));
            tempFile = File.createTempFile(this.file.getName(), null, this.file.getParentFile());
            output = new BufferedOutputStream(new FileOutputStream(tempFile), FILE_BUFFER_SIZE);
            XMLEventWriter eventWriter = this.xmlOutputFactory.createXMLEventWriter(output);
            UpdatesXMLEventWriter updatesWriter = new UpdatesXMLEventWriter(eventWriter, this.updates);
            updatesWriter.add(eventReader);
            updatesWriter.close();
            output.close();
            output = null;
            eventReader.close();

            // Move temp file into place
            if (!tempFile.renameTo(this.file))
                throw new IOException("error renaming `" + tempFile.getName() + "' to `" + this.file.getName() + "'");
            tempFile = null;
        } finally {
            if (output != null) {
                try {
                    output.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            if (tempFile != null)
                tempFile.delete();
        }

        // Done
        this.current = null;
        this.updates.clear();
        this.modified = false;
    }

    /**
     * Cancel this transaction.
     */
    public void rollback() {
        this.current = null;
        this.updates.clear();
        this.modified = false;
    }

    /**
     * Get the updates list associated with this transaction.
     *
     * @return unmodifiable list of updates
     */
    public List<String> getUpdates() {
        return Collections.unmodifiableList(this.updates);
    }

    /**
     * Add an update to the list associated with this transaction.
     */
    public void addUpdate(String name) {
        this.updates.add(name);
        this.modified = true;
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
        source.setSystemId(file.toURI().toString());
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(FILE_BUFFER_SIZE);
        StreamResult result = new StreamResult(buffer);
        result.setSystemId(file.toURI().toString());

        // Apply transform
        transformer.transform(source, result);

        // Save result as the new current value
        this.current = buffer.toByteArray();
        this.modified = true;

        // Debug
        //System.out.println("************************** AFTER TRANSFORM");
        //System.out.println(new String(this.current));
    }

    private void readFile() throws IOException, XMLStreamException {

        // Read in file, extracting and removing the updates list in the process
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(FILE_BUFFER_SIZE);
        XMLEventWriter eventWriter = this.xmlOutputFactory.createXMLEventWriter(buffer);
        BufferedInputStream input = new BufferedInputStream(new FileInputStream(this.file), FILE_BUFFER_SIZE);
        XMLEventReader eventReader = this.xmlInputFactory.createXMLEventReader(input);
        UpdatesXMLEventReader updatesReader = new UpdatesXMLEventReader(eventReader);
        eventWriter.add(updatesReader);
        eventWriter.close();
        eventReader.close();
        input.close();

        // Was the update list found?
        List<String> fileUpdates = updatesReader.getUpdates();
        if (fileUpdates == null)
            throw new PersistentObjectException("file `" + this.file + "' does not contain an updates list");

        // Save current content (without updates) and updates list
        this.current = buffer.toByteArray();
        this.updates.clear();
        this.updates.addAll(fileUpdates);
    }
}

