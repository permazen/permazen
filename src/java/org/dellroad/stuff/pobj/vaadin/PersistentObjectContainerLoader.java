
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj.vaadin;

import com.vaadin.server.VaadinSession;

import org.dellroad.stuff.pobj.PersistentObject;
import org.dellroad.stuff.vaadin7.AbstractSimpleContainer;

/**
 * Facilitates populating a {@link AbstractSimpleContainer} with data from {@link PersistentObject}
 * and automatically updating it when than {@link PersistentObject} changes.
 *
 * <p>
 * The constructor requires a {@link Generator} to generate, from the {@link PersistentObject}
 * root object, the Java objects that populate the {@link AbstractSimpleContainer}.
 * </p>
 *
 * <p>
 * <b>Note:</b> Instances of this class start out in a non-listening state. You must invoke {@link #connect connect()}
 * to start listening to the {@link PersistentObject} and (re)load the container contents.
 * You should invoke {@link #disconnect} whenever the container is no longer used to avoid a memory leak.
 * Typically, {@link #connect connect()} and {@link #disconnect} are invoked from the
 * {@link com.vaadin.ui.Component#attach attach()} and {@link com.vaadin.ui.Component#detach detach()} methods (respectively)
 * of the GUI widget that is using the container.
 * </p>
 *
 * @param <T> type of the persistent object
 * @param <K> the type of the Java objects that back each {@link com.vaadin.data.Item} in the container
 */
@SuppressWarnings("serial")
public class PersistentObjectContainerLoader<T, K> {

    private final AbstractSimpleContainer<?, K> container;
    private final Generator<T, K> generator;

    private VaadinPersistentObjectListener<T> listener;

    /**
     * Constructor.
     *
     * @param container container to populate
     * @param generator object that generates the Java objects that back the {@link com.vaadin.data.Item}s in the container
     * @throws IllegalArgumentException if {@code container} is null
     * @throws IllegalArgumentException if {@code generator} is null
     */
    public PersistentObjectContainerLoader(AbstractSimpleContainer<?, K> container, Generator<T, K> generator) {
        if (container == null)
            throw new IllegalArgumentException("null container");
        if (generator == null)
            throw new IllegalArgumentException("null generator");
        this.container = container;
        this.generator = generator;
    }

    /**
     * Register this container as a listener on the persistent object and load the container.
     * This method may only be invoked while the {@link VaadinSession} is locked by the current thread.
     * If this method is invoked twice with no intervening {@link #disconnect}, this instance will
     * automatically disconnect from the old {@link PersistentObject} before connecting to the new one.
     *
     * @throws IllegalArgumentException if {@code persistentObject} is null
     * @throws IllegalStateException if there is no {@link VaadinSession} associated with the current thread
     */
    public void connect(PersistentObject<T> persistentObject) {

        // Sanity check
        this.assertSession();
        if (persistentObject == null)
            throw new IllegalArgumentException("null persistentObject");

        // Connect
        boolean needReload = false;
        T root = null;
        synchronized (this) {
            if (this.listener != null)
                this.listener.unregister();
            this.listener = new VaadinPersistentObjectListener<T>(persistentObject) {
                @Override
                protected void handlePersistentObjectChange(T oldRoot, T newRoot, long version) {
                    PersistentObjectContainerLoader.this.reload(newRoot);
                }
            };
            this.listener.register();
            root = persistentObject.getSharedRoot();
            needReload = true;
        }

        // Load container if there is data
        if (needReload)
            this.reload(root);
    }

    /**
     * Shutdown this container. This unregisters the container from the {@link PersistentObject} it was previously
     * {@linkplain #connect connected} to. This method may be invoked from any thread and is idempotent.
     * The associated container is not notified about any change.
     *
     * @throws IllegalStateException if there is no {@link VaadinSession} associated with the current thread
     */
    public synchronized void disconnect() {
        this.assertSession();
        if (this.listener != null) {
            this.listener.unregister();
            this.listener = null;
        }
    }

    /**
     * Reload the container associated with this instance using the given {@link PersistentObject} root.
     *
     * @throws IllegalStateException if there is no {@link VaadinSession} associated with the current thread
     */
    protected void reload(T root) {
        this.assertSession();
        this.container.load(this.generator.generateContainerObjects(root));
    }

    /**
     * Verify there is a {@link VaadinSession} associated with the current thread.
     *
     * @throws IllegalStateException if there is no {@link VaadinSession} associated with the current thread
     */
    protected void assertSession() {
        if (VaadinSession.getCurrent() == null)
             throw new IllegalStateException("there is no VaadinSession associated with the current thread");
    }

// Generator

    /**
     * Interface used by {@link PersistentObjectContainerLoader} to derive {@link AbstractSimpleContainer} backing
     * objects from a {@link PersistentObject} root.
     *
     * @param <T> type of the persistent object
     * @param <K> the type of the Java objects that back each {@link com.vaadin.data.Item} in the {@link AbstractSimpleContainer}
     */
    public interface Generator<T, K> {

        /**
         * Generate the Java objects that will back the {@link AbstractSimpleContainer} associated with
         * a {@link PersistentObjectContainerLoader} from the given {@link PersistentObject} root.
         *
         * <p>
         * This method should not modify {@code root}.
         * </p>
         *
         * <p>
         * If the associated {@link PersistentObject} allows empty starts or stops, then {@code root} might be null.
         * </p>
         *
         * @param root updated persistent object root, or null in the case of an empty start or stop
         * @return container backing objects
         */
        Iterable<? extends K> generateContainerObjects(T root);
    }
}

