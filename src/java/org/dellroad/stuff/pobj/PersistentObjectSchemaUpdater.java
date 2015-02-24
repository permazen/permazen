
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stax.StAXResult;
import javax.xml.transform.stream.StreamResult;

import org.dellroad.stuff.schema.AbstractSchemaUpdater;
import org.dellroad.stuff.schema.UnrecognizedUpdateException;

/**
 * A {@link PersistentObjectDelegate} that is also a {@link AbstractSchemaUpdater} that automatically
 * applies needed updates to the persistent XML file.
 *
 * <p>
 * To use this class, wrap your normal delegate in an instance of this class. This will augment
 * the serialization and deserialization process to keep track of which updates have been applied to the XML structure,
 * and automatically and transparently apply any needed updates during deserialization.
 * </p>
 *
 * <p>
 * Updates are tracked by inserting an <code>{@link #UPDATES_ELEMENT_NAME &lt;pobj:updates&gt;}</code> element
 * into the serialized XML document; this update list is transparently removed when the document is read back,
 * and any missing updates are applied automatically.
 * In this way the document and its set of applied updates always travel together. For example:
 *
 * <blockquote><pre>
 *  &lt;MyConfig&gt;
 *      <b>&lt;pobj:updates xmlns:pobj="http://dellroad-stuff.googlecode.com/ns/persistentObject"&gt;
 *          &lt;pobj:update>some-update-name-1&lt;/pobj:update&gt;
 *          &lt;pobj:update>some-update-name-2&lt;/pobj:update&gt;
 *      &lt;/pobj:updates&gt;</b>
 *      &lt;username&gt;admin&lt;/username&gt;
 *      &lt;password&gt;secret&lt;/password&gt;
 *  &lt;/MyConfig&gt;
 * </pre></blockquote>
 * </p>
 *
 * <p>
 * For Spring applications, {@link SpringPersistentObjectSchemaUpdater} provides a convenient declarative way
 * to define your schema updates via XSLT files.
 * </p>
 *
 * @param <T> type of the root persistent object
 */
public class PersistentObjectSchemaUpdater<T> extends AbstractSchemaUpdater<PersistentFileTransaction, PersistentFileTransaction>
  implements PersistentObjectDelegate<T> {

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

    protected final PersistentObjectDelegate<T> delegate;

    private final XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();

    private List<String> updateNames;
    private TransformerFactory transformerFactory;

// Constructors

    /**
     * Constructor.
     *
     * <p>
     * Callers provide a {@link PersistentObjectDelegate} that will be used for all operations, with the exception
     * that {@linkplain #serialize serialization} and {@linkplain #deserialize deserialization} will add/remove
     * the upate list, and {@linkplain #deserialize deserialization} will apply any needed updates as necessary.
     *
     * @param delegate delegate that will be wrapped by this instance
     * @throws IllegalArgumentException if {@code delegate} is null
     */
    public PersistentObjectSchemaUpdater(PersistentObjectDelegate<T> delegate) {
        if (delegate == null)
            throw new IllegalArgumentException("null delegate");
        this.delegate = delegate;
    }

// Accessors

    /**
     * Configure the {@link TransformerFactory} to be used by this instance when reading the updates from the XML file.
     * Must support reading from a StAX {@link Source}.
     *
     * <p>
     * The implementation in {@link PersistentObjectSchemaUpdater} returns null.
     * </p>
     *
     * @param transformerFactory {@link TransformerFactory} to use or null for the platform default
     */
    public void setTransformerFactory(TransformerFactory transformerFactory) {
        this.transformerFactory = transformerFactory;
    }

// PersistentObjectDelegate methods

    /**
     * Make a deep copy of the given object.
     *
     * <p>
     * The implementation in {@link PersistentObjectSchemaUpdater} delegates to the delegate provided in the constructor.
     *
     * @throws IllegalArgumentException {@inheritDoc}
     * @throws PersistentObjectException {@inheritDoc}
     */
    @Override
    public T copy(T original) {
        return this.delegate.copy(original);
    }

    /**
     * Compare two object graphs.
     *
     * <p>
     * The implementation in {@link PersistentObjectSchemaUpdater} delegates to the delegate provided in the constructor.
     */
    @Override
    public boolean isSameGraph(T root1, T root2) {
        return this.delegate.isSameGraph(root1, root2);
    }

    /**
     * Validate the given instance.
     *
     * <p>
     * The implementation in {@link PersistentObjectSchemaUpdater} delegates to the delegate provided in the constructor.
     *
     * @throws IllegalArgumentException {@inheritDoc}
     */
    @Override
    public Set<ConstraintViolation<T>> validate(T obj) {
        return this.delegate.validate(obj);
    }

    /**
     * Handle an exception thrown during a delayed write-back attempt.
     *
     * <p>
     * The implementation in {@link PersistentObjectSchemaUpdater} delegates to the delegate provided in the constructor.
     */
    @Override
    public void handleWritebackException(PersistentObject<T> pobj, Throwable t) {
        this.delegate.handleWritebackException(pobj, t);
    }

    /**
     * Get the default value for the root object graph.
     *
     * <p>
     * The implementation in {@link PersistentObjectSchemaUpdater} delegates to the delegate provided in the constructor.
     */
    @Override
    public T getDefaultValue() {
        return this.delegate.getDefaultValue();
    }

    /**
     * Serialize object to XML.
     *
     * <p>
     * The implementation in {@link PersistentObjectSchemaUpdater} delegates to the delegate provided in the constructor
     * but also adds the update list as the first XML tag.
     *
     * @throws PersistentObjectException {@inheritDoc}
     */
    @Override
    public void serialize(T obj, Result result) throws IOException {
        try {

            // Get update names
            final List<String> updateNameList = this.updateNames != null ? this.updateNames : this.getAllUpdateNames();

            // Wrap result with a writer that adds the update list
            XMLStreamWriter writer;
            if (result instanceof StreamResult && ((StreamResult)result).getOutputStream() != null)
                writer = this.xmlOutputFactory.createXMLStreamWriter(((StreamResult)result).getOutputStream(), "UTF-8");
            else
                writer = this.xmlOutputFactory.createXMLStreamWriter(result);
            final UpdatesXMLStreamWriter updatesWriter = new UpdatesXMLStreamWriter(writer, updateNameList);

            // Serialize using the provided delegate
            this.delegate.serialize(obj, new StAXResult(updatesWriter));
            updatesWriter.close();
        } catch (RuntimeException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new PersistentObjectException(e);
        }
    }

    /**
     * Deserialize object from XML.
     *
     * <p>
     * The implementation in {@link PersistentObjectSchemaUpdater} delegates to the delegate provided in the constructor
     * but also removes the update list as the first XML tag and applies any needed updates.
     * </p>
     *
     * @throws PersistentObjectException {@inheritDoc}
     */
    @Override
    public T deserialize(Source source) throws IOException {
        try {

            // Create a temporary in-memory "database" containing the XML content
            PersistentFileTransaction transaction = new PersistentFileTransaction(source, this.transformerFactory);

            // Apply schema updates as necessary to update the XML structure
            this.initializeAndUpdateDatabase(transaction);

            // Save updates names so we can preserve their order
            this.updateNames = transaction.getUpdates();

            // Sanity check that all updates were applied
            final HashSet<String> unappliedUpdates = new HashSet<String>(this.getAllUpdateNames());
            unappliedUpdates.removeAll(this.updateNames);
            if (!unappliedUpdates.isEmpty())
                throw new PersistentObjectException("internal inconsistency: unapplied updates remain: " + unappliedUpdates);

            // Deserialize the now up-to-date XML structure using the provided delegate
            return this.delegate.deserialize(new DOMSource(transaction.getData(), transaction.getSystemId()));
        } catch (UnrecognizedUpdateException e) {
            throw new PersistentObjectException(e);
        } catch (RuntimeException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new PersistentObjectException(e);
        }
    }

// AbstractSchemaUpdater methods

    @Override
    protected boolean databaseNeedsInitialization(PersistentFileTransaction transaction) throws Exception {
        return false;
    }

    @Override
    protected void initializeDatabase(PersistentFileTransaction transaction) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected PersistentFileTransaction openTransaction(PersistentFileTransaction transaction) throws Exception {
        return transaction;
    }

    @Override
    protected void commitTransaction(PersistentFileTransaction transaction) throws Exception {
        // nothing to do
    }

    @Override
    protected void rollbackTransaction(PersistentFileTransaction transaction) throws Exception {
        // nothing to do
    }

    @Override
    protected Set<String> getAppliedUpdateNames(PersistentFileTransaction transaction) throws Exception {
        return new HashSet<String>(transaction.getUpdates());
    }

    @Override
    protected void recordUpdateApplied(PersistentFileTransaction transaction, String name) throws Exception {
        transaction.getUpdates().add(name);
    }
}

