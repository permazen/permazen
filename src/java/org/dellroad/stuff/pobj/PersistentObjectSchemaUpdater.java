
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.xml.namespace.QName;
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
 * Updates are tracked by "secretly" inserting <code>{@link #UPDATES_ELEMENT_NAME &lt;pobj:updates&gt;}</code>
 * elements into the serialized XML document; these updates are transparently removed when the document is read back.
 * In this way the document and its set of applied updates always travel together.
 *
 * <p>
 * Subclasses will typically override {@link #getInitialValue} for when there is no persistent file yet.
 *
 * @param <T> type of the root persistent object
 */
public class PersistentObjectSchemaUpdater<T> extends AbstractSchemaUpdater<File, PersistentFileTransaction> {

    /**
     * XML namespace URI used for nested update elements.
     */
    public static final String NAMESPACE_URI = "http://dellroad-stuff.googlecode.com/ns/persistentObject";

    /**
     * Preferred XML namespace prefix for {@link #NAMESPACE_URI} elements.
     */
    public static final String XML_PREFIX = "pobj";

    /**
     * XML element name for the updates list.
     */
    public static final QName UPDATES_ELEMENT_NAME = new QName(NAMESPACE_URI, "updates", XML_PREFIX);

    /**
     * XML element name for a single update.
     */
    public static final QName UPDATE_ELEMENT_NAME = new QName(NAMESPACE_URI, "update", XML_PREFIX);

    /**
     * XML namespace URI used for namespace declarations.
     */
    public static final QName XMLNS_ATTRIBUTE_NAME = new QName("http://www.w3.org/2000/xmlns/", XML_PREFIX, "xmlns");

    /**
     * Default check interval for "out-of-band" updates to the persistent file ({@value}ms).
     */
    public static final long DEFAULT_CHECK_INTERVAL = 1000;

    protected File file;
    protected long writeDelay;
    protected long checkInterval = DEFAULT_CHECK_INTERVAL;
    protected PersistentObjectDelegate<T> delegate;

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
     * Configure the check interval for "out-of-band" updates to the persistent file.
     * Default is {@link #DEFAULT_CHECK_INTERVAL}.
     */
    public void setCheckInterval(long checkInterval) {
        this.checkInterval = checkInterval;
    }

    /**
     * Configure the {@link PersistentObjectDelegate}. Required property.
     */
    public void setDelegate(PersistentObjectDelegate<T> delegate) {
        this.delegate = delegate;
    }

    /**
     * Start this instance. Does nothing if already started.
     *
     * @throws IllegalArgumentException if an invalid file, write delay, or delegate is configured
     * @throws PersistentObjectException if an error occurs
     */
    public synchronized void start() {

        // Already started?
        if (this.persistentObject != null)
            return;

        // Sanity check
        if (this.file == null)
            throw new IllegalArgumentException("no file configured");
        if (this.writeDelay < 0)
            throw new IllegalArgumentException("negative writeDelay file configured");
        if (this.delegate == null)
            throw new IllegalArgumentException("no delegate configured");

        // Do schema updates
        try {
            this.initializeAndUpdateDatabase(this.file);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new PersistentObjectException(e);
        }

        // Create and start persistent object
        PersistentObject<T> pobj = new PersistentObject<T>(new UpdaterDelegate(), this.file, this.writeDelay, this.checkInterval);
        pobj.start();

        // Done
        this.persistentObject = pobj;
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
     *
     * <p>
     * The implementation in {@link PersistentObjectSchemaUpdater} just returns null, which leaves the
     * initial root object unset. Subclasses should override as desired to provide an initial value.
     *
     * <p>
     * The returned value must properly validate.
     */
    protected T getInitialValue() {
        return null;
    }

    @Override
    protected boolean databaseNeedsInitialization(PersistentFileTransaction transaction) throws Exception {
        return transaction.getData() == null;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void initializeDatabase(PersistentFileTransaction transaction) throws Exception {

        // Get initial value
        T initialValue = this.getInitialValue();
        if (initialValue == null)
            return;

        // Validate it
        Set<ConstraintViolation<T>> violations = this.delegate.validate(initialValue);
        if (!violations.isEmpty())
            throw new PersistentObjectValidationException((Set<ConstraintViolation<?>>)(Object)violations);

        // Serialize it
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(PersistentFileTransaction.FILE_BUFFER_SIZE);
        StreamResult result = new StreamResult(buffer);
        this.delegate.serialize(initialValue, result);

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

    // Our PersistentObjectDelegate that hides the updates when (de)serializing
    private class UpdaterDelegate extends FilterDelegate<T> {

        private final XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();
        private final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();

        UpdaterDelegate() {
            super(PersistentObjectSchemaUpdater.this.delegate);
        }

        /**
         * Serialize object to XML, adding update list.
         */
        @Override
        public void serialize(T obj, Result result) throws IOException {
            try {
                XMLEventWriter eventWriter = this.xmlOutputFactory.createXMLEventWriter(result);
                UpdatesXMLEventWriter updatesWriter = new UpdatesXMLEventWriter(eventWriter,
                  PersistentObjectSchemaUpdater.this.updateNames);
                super.serialize(obj, new StAXResult(updatesWriter));
                updatesWriter.close();
            } catch (IOException e) {
                throw e;
            } catch (XMLStreamException e) {
                throw new PersistentObjectException(e);
            }
        }

        /**
         * Deserialize object from XML, removing update list.
         */
        @Override
        public T deserialize(Source source) throws IOException {
            try {
                XMLEventReader eventReader = this.xmlInputFactory.createXMLEventReader(source);
                UpdatesXMLEventReader updatesReader = new UpdatesXMLEventReader(eventReader);
                return super.deserialize(new StAXSource(updatesReader));
            } catch (IOException e) {
                throw e;
            } catch (XMLStreamException e) {
                throw new PersistentObjectException(e);
            }
        }
    }
}

