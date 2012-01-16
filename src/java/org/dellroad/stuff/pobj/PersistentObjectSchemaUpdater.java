
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.stax.StAXResult;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;

import org.dellroad.stuff.schema.AbstractSchemaUpdater;

/**
 * Support superclass for {@link PersistentObject} schema updaters.
 *
 * <p>
 * This class holds a nested {@link PersistentObject} and ensures that it's up to date when started.
 * Use {@link #getPersistentObject} to access it.
 *
 * <p>
 * Updates are tracked by "secretly" inserting <code>{@link XMLConstants#UPDATES_ELEMENT_NAME &lt;pobj:updates&gt;}</code>
 * elements into the serialized XML document; these updates are transparently removed when the document is read back.
 * In this way the document and its set of applied updates always travel together.
 *
 * @param <T> type of the root persistent object
 */
public abstract class PersistentObjectSchemaUpdater<T> extends AbstractSchemaUpdater<File, PersistentFileTransaction> {

    protected File file;
    protected long writeDelay;

    private ArrayList<String> updateNames;
    private PersistentObject<T> persistentObject;

    /**
     * Configure the file used to store this object persistently. Required property.
     */
    public void setFile(File file) {
        this.file = file;
    }

    /**
     * Configure the maximum delay after an update operation before a write-back to the persistent file
     * must be initiated. Default is zero.
     */
    public void setWriteDelay(long writeDelay) {
        this.writeDelay = writeDelay;
    }

    /**
     * Start this instance. Does nothing if already started.
     *
     * @throws IllegalArgumentException if an invalid file or write delay is configured
     * @throws PersistentObjectException if an error occurs
     */
    public synchronized void start() {

        // Already started?
        if (this.persistentObject != null)
            return;

        // Do schema updates
        try {
            this.initializeAndUpdateDatabase(this.file);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new PersistentObjectException(e);
        }

        // Create and start persistent object
        this.persistentObject = new UpdaterObject();
        this.persistentObject.start();
    }

    /**
     * Stop this instance. Does nothing if already stopped.
     *
     * @throws PersistentObjectException if a delayed write back is pending and error occurs during writing
     */
    public synchronized void stop() {

        // Already stopped?
        if (this.persistentObject == null)
            return;

        // Stop
        this.persistentObject.stop();
        this.persistentObject = null;
    }

    /**
     * Get the {@link PersistentObject}.
     *
     * @throws IllegalStateException if this instance is not started
     */
    public synchronized PersistentObject<T> getPersistentObject() {
        if (this.persistentObject == null)
            throw new IllegalStateException("not started");
        return this.persistentObject;
    }

    /**
     * Get the initial value for the persistent object when no persistent file is found.
     */
    protected abstract T getInitialValue();

    /**
     * Serialize an instance of the given root object.
     *
     * @throws RuntimeException if an error occurs
     */
    public abstract void serialize(T obj, Result result);

    /**
     * Deserialize an instance of the root object.
     *
     * @throws RuntimeException if an error occurs
     */
    public abstract T deserialize(Source source);

    @Override
    protected boolean databaseNeedsInitialization(PersistentFileTransaction transaction) throws Exception {
        return transaction.getData() == null;
    }

    @Override
    protected void initializeDatabase(PersistentFileTransaction transaction) throws Exception {

        // Get initial value
        T initialValue = this.getInitialValue();
        if (initialValue == null)
            throw new PersistentObjectException("null value returned from initialValue()");

        // Serialize it
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(PersistentFileTransaction.FILE_BUFFER_SIZE);
        StreamResult result = new StreamResult(buffer);
        this.serialize(initialValue, result);

        // Set it in the transaction
        transaction.setData(buffer.toByteArray());
    }

    @Override
    protected PersistentFileTransaction openTransaction(File file) throws Exception {
        return new PersistentFileTransaction(file);
    }

    @Override
    protected void commitTransaction(PersistentFileTransaction transaction) throws Exception {
        this.updateNames = new ArrayList<String>(transaction.getUpdates());
        transaction.commit();
    }

    @Override
    protected void rollbackTransaction(PersistentFileTransaction transaction) throws Exception {
        transaction.rollback();
    }

    @Override
    protected Set<String> getAppliedUpdateNames(PersistentFileTransaction transaction) throws Exception {
        return new HashSet<String>(transaction.getUpdates());
    }

    @Override
    protected void recordUpdateApplied(PersistentFileTransaction transaction, String name) throws Exception {
        transaction.addUpdate(name);
    }

    // Our PersistentObject subclas that hides the updates
    private class UpdaterObject extends PersistentObject<T> {

        private final XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();
        private final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();

        UpdaterObject() {
            super(PersistentObjectSchemaUpdater.this.file, PersistentObjectSchemaUpdater.this.writeDelay);
        }

        /**
         * Serialize object to XML, adding update list.
         */
        @Override
        public void serialize(T obj, Result result) {
            try {
                XMLEventWriter eventWriter = this.xmlOutputFactory.createXMLEventWriter(result);
                UpdatesXMLEventWriter updatesWriter = new UpdatesXMLEventWriter(eventWriter,
                  PersistentObjectSchemaUpdater.this.updateNames);
                PersistentObjectSchemaUpdater.this.serialize(obj, new StAXResult(updatesWriter));
                updatesWriter.close();
            } catch (XMLStreamException e) {
                throw new PersistentObjectException(e);
            }
        }

        /**
         * Deserialize object from XML, removing update list.
         */
        @Override
        public T deserialize(Source source) {
            try {
                XMLEventReader eventReader = this.xmlInputFactory.createXMLEventReader(source);
                UpdatesXMLEventReader updatesReader = new UpdatesXMLEventReader(eventReader);
                return PersistentObjectSchemaUpdater.this.deserialize(new StAXSource(updatesReader));
            } catch (XMLStreamException e) {
                throw new PersistentObjectException(e);
            }
        }
    }
}

