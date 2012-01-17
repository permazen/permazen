
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.validation.ConstraintViolation;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.dellroad.stuff.validation.ValidationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for Simple XML Persistence Objects (POBJ).
 *
 * <p>
 * The model is that there is a root Java object which, together with the tree of objects is points to,
 * represents the current "database" state. XML (de)serializers provide correspondence between the in-memory
 * Java object tree and an XML document and define what the "tree" is. Just like the persistent XML file that
 * backs it, the object tree is read and written in its entirety, atomically, and so is always treated as "one thing".
 *
 * <p>
 * Support for optimistic locking is included. There is a current version number which is incremented each
 * time the object tree is updated.
 *
 * <p>
 * Support for delayed write-back is included. This allows modifications that occur in rapid succession to be consolidated
 * into a single filesystem write operation.
 *
 * <p>
 * File system writes use the atomic rename operation.
 *
 * <p>
 * Subclasses must implement the XML (de)serialization procedure.
 *
 * @param <T> type of the root persistent object
 * @see org.dellroad.stuff.pobj
 */
public abstract class PersistentObject<T> {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * The XML file containing the persisted object.
     */
    protected final File persistentFile;

    private final HashSet<PersistentObjectListener<T>> listeners = new HashSet<PersistentObjectListener<T>>();
    private final long writeDelay;

    private T root;
    private T sharedRoot;
    private ScheduledExecutorService writebackExecutor;
    private ExecutorService notifyExecutor;
    private ScheduledFuture pendingWrite;
    private long version;
    private boolean started;

    /**
     * Constructor.
     *
     * <p>
     * The {@code writeDelay} is the maximum delay after an update operation before a write-back to the persistent file
     * must be initiated.
     *
     * @param file the file used to persist
     * @param writeDelay write delay in milliseconds, or zero for immediate write-back
     * @throws IllegalArgumentException if {@code writeDelay} is negative
     */
    protected PersistentObject(File file, long writeDelay) {
        if (file == null)
            throw new IllegalArgumentException("null file");
        if (writeDelay < 0)
            throw new IllegalArgumentException("negative writeDelay");
        this.persistentFile = file;
        this.writeDelay = writeDelay;
    }

    /**
     * Get the maximum delay after an update operation before a write-back to the persistent file
     * must be initiated.
     *
     * @return write delay in milliseconds, or zero for immediate write-back
     */
    public long getWriteDelay() {
        return this.writeDelay;
    }

    /**
     * Get the version of the current root.
     *
     * <p>
     * This returns a value which increases monotonically with each update.
     */
    public synchronized long getVersion() {
        return this.version;
    }

    /**
     * Start this instance. Does nothing if already started.
     *
     * @throws PersistentObjectException if an error occurs
     */
    public synchronized void start() {

        // Already started?
        if (this.started)
            return;

        // Read file (if it exists)
        this.log.info(this + ": starting");
        if (this.persistentFile.exists()) {
            this.root = this.read();
            this.version++;
        } else
            this.log.info(this + ": persistent file `" + this.persistentFile + "' does not exist yet");

        // Create executor services
        this.writebackExecutor = Executors.newSingleThreadScheduledExecutor();
        this.notifyExecutor = Executors.newSingleThreadExecutor();
        this.started = true;
    }

    /**
     * Stop this instance. Does nothing if already stopped.
     *
     * @throws PersistentObjectException if a delayed write back is pending and error occurs in {@link #write}
     */
    public synchronized void stop() {

        // Already stopped?
        if (!this.started)
            return;

        // Perform any lingering pending save now
        if (this.pendingWrite != null) {
            this.pendingWrite.cancel(false);
            this.pendingWrite = null;
            this.write(this.root);
        }

        // Stop executor services
        this.log.info(this + ": shutting down");
        this.writebackExecutor.shutdown();
        this.notifyExecutor.shutdown();

        // Reset
        this.writebackExecutor = null;
        this.notifyExecutor = null;
        this.root = null;
        this.started = false;
    }

    /**
     * Atomically read the root object.
     *
     * <p>
     * If there is no persistent file and no value has been set, null will be returned.
     * This condition is checked on each invocation of this method, so the persistent file
     * is allowed to appear at any time. However, this is the only case where modifications
     * to the underlying file are allowed while this instance is started.
     *
     * <p>
     * This returns a deep copy of the current root object; any subsequent modifications are not written back.
     *
     * @return the current root instance, or be null if no value has ever been written
     * @throws IllegalStateException if this instance is not started
     * @throws PersistentObjectException if an error occurs
     */
    public synchronized T getRoot() {

        // Sanity check
        if (!this.started)
            throw new IllegalStateException("not started");

        // Try again to read the file (if it exists)
        if (this.root == null && this.persistentFile.exists()) {
            this.root = this.read();
            this.version++;
        }

        // Copy root
        return this.root != null ? this.copy(this.root) : null;
    }

    /**
     * Get a shared copy of the root object.
     *
     * <p>
     * This returns a copy of the root object, but it returns the same copy each time (until the next change).
     * This method is more efficient than {@link #getRoot} when used by callers who mutually agree not to
     * modify the returned object.
     */
    public synchronized T getSharedRoot() {
        if (this.sharedRoot == null)
            this.sharedRoot = this.getRoot();
        return this.sharedRoot;
    }

    /**
     * Atomically update the root object.
     *
     * <p>
     * The given object is deep-copied and the copy replaces the current root.
     *
     * <p>
     * If {@code version} is non-zero, then if the current version is not equal to it,
     * a {@link PersistentObjectVersionException} exception is thrown. This mechanism
     * can be used for optimistic locking.
     *
     * @param newRoot new persistent object
     * @param expectedVersion expected current version number, or zero for none
     * @throws IllegalArgumentException if {@code newRoot} is null
     * @throws IllegalArgumentException if {@code version} is negative
     * @throws IllegalStateException if this instance is not started
     * @throws PersistentObjectException if an error occurs
     * @throws PersistentObjectVersionException if {@code version} is non-zero and not equal to the current version
     * @throws PersistentObjectValidationException if the new root has validation errors
     */
    @SuppressWarnings("unchecked")
    public final synchronized void setRoot(T newRoot, long expectedVersion) {

        // Sanity check
        if (newRoot == null)
            throw new IllegalArgumentException("null newRoot");
        if (this.root == null)
            throw new IllegalStateException("not started");
        if (expectedVersion < 0)
            throw new IllegalStateException("negative expectedVersion");

        // Check version number
        if (expectedVersion != 0 && this.version != expectedVersion)
            throw new PersistentObjectVersionException(this.version, expectedVersion);

        // Validate the new root
        Set<ConstraintViolation<T>> violations = this.validate(newRoot);
        if (!violations.isEmpty())
            throw new PersistentObjectValidationException((Set<ConstraintViolation<?>>)(Object)violations);

        // Do the update
        final T oldRoot = this.root;
        this.root = this.copy(newRoot);
        this.version++;
        this.sharedRoot = null;

        // Perform write-back, either now or later
        if (this.writeDelay == 0)
            this.write(this.root);
        else if (this.pendingWrite == null) {
            this.pendingWrite = this.writebackExecutor.schedule(new Runnable() {
                @Override
                public void run() {
                    PersistentObject.this.writeTimeout();
                }
            }, this.writeDelay, TimeUnit.MILLISECONDS);
        }

        // Notify listeners
        this.notifyListeners(this.version, oldRoot, newRoot);
    }

    /**
     * Atomically update the root object.
     *
     * <p>
     * The is a convenience method, equivalent to:
     *  <blockquote>
     *  <code>{@link #setRoot(Object, long) setRoot}(newRoot, 0)</code>
     *  </blockquote>
     *
     * <p>
     * This method cannot throw {@link PersistentObjectVersionException}.
     */
    public final synchronized void setRoot(T newRoot) {
        this.setRoot(newRoot, 0);
    }

    /**
     * Make a deep copy of the given object.
     *
     * <p>
     * The implementation in {@link PersistentObject} does this by serializing and then deserializing
     * the object graph. Subclasses are encouraged to provide a more efficient implementation.
     *
     * @throws IllegalArgumentException if {@code original} is null
     * @throws PersistentObjectException if an error occurs
     */
    public T copy(T original) {
        if (original == null)
            throw new IllegalArgumentException("null original");
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(32 * 1024 - 32);
        this.serialize(original, new StreamResult(buffer));
        StreamSource source = new StreamSource(new ByteArrayInputStream(buffer.toByteArray()));
        T copy = this.deserialize(source);
        if (copy == null)
            throw new PersistentObjectException("null object returned by deserialize()");
        return copy;
    }

    /**
     * Validate the given instance.
     *
     * <p>
     * The implementation in {@link PersistentObject} performs validation using {@link ValidationContext#validate()}.
     *
     * @throws IllegalArgumentException if {@code obj} is null
     * @throws PersistentObjectException if validation fails
     */
    public Set<ConstraintViolation<T>> validate(T obj) {
        return new ValidationContext<T>(obj).validate();
    }

    /**
     * Add a listener.
     *
     * @throws IllegalArgumentException if {@code listener} is null
     */
    public void addListener(PersistentObjectListener<T> listener) {
        if (listener == null)
            throw new IllegalArgumentException("null listener");
        synchronized (this.listeners) {
            this.listeners.add(listener);
        }
    }

    /**
     * Remove a listener.
     */
    public void removeListener(PersistentObjectListener<T> listener) {
        synchronized (this.listeners) {
            this.listeners.remove(listener);
        }
    }

    /**
     * Serialize an instance of the given root object.
     *
     * @param obj object to serialize; must not be modified
     * @param result XML destination
     * @throws PersistentObjectException if an error occurs
     */
    public abstract void serialize(T obj, Result result);

    /**
     * Deserialize an instance of the root object.
     *
     * @param source XML source
     * @return deserialized object
     * @throws PersistentObjectException if an error occurs
     */
    public abstract T deserialize(Source source);

    /**
     * Get a simple string description of this instance. Used for (among other things) log messages.
     */
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + this.persistentFile.getName() + "]";
    }

    /**
     * Read the persistent file.
     *
     * @throws PersistentObjectException if an error occurs
     */
    protected T read() {

        // Open file
        this.log.info(this + ": reading persistent file `" + this.persistentFile + "'");
        BufferedInputStream input;
        try {
            input = new BufferedInputStream(new FileInputStream(this.persistentFile));
        } catch (IOException e) {
            throw new PersistentObjectException("error opening persistent file", e);
        }

        // Parse XML
        T obj;
        try {
            StreamSource source = new StreamSource(input);
            source.setSystemId(this.persistentFile);
            obj = this.deserialize(source);
        } finally {
            try {
                input.close();
            } catch (IOException e) {
                // ignore
            }
        }

        // Check result
        if (obj == null)
            throw new PersistentObjectException("null object returned by deserialize()");

        // Done
        return obj;
    }

    /**
     * Write the persistent file.
     *
     * <p>
     * A temporary file is created in the same directory and then renamed to provide for an atomic update.
     *
     * @throws IllegalArgumentException if {@code obj} is null
     * @throws PersistentObjectException if an error occurs
     */
    protected final synchronized void write(T obj) {

        // Sanity check
        if (obj == null)
            throw new IllegalArgumentException("null obj");

        // Create temporary file
        this.log.info(this + ": writing persistent file `" + this.persistentFile + "'");
        File tempFile;
        try {
            tempFile = File.createTempFile(this.persistentFile.getName(), null, this.persistentFile.getParentFile());
        } catch (IOException e) {
            throw new PersistentObjectException("error creating temporary file", e);
        }
        try {

            // Open temporary file
            BufferedOutputStream output;
            try {
                output = new BufferedOutputStream(new FileOutputStream(tempFile));
            } catch (IOException e) {
                throw new PersistentObjectException("error opening to temporary file", e);
            }

            // Serialize to XML
            this.log.info(this + ": saving persistent object");
            try {
                StreamResult result = new StreamResult(output);
                result.setSystemId(tempFile);
                this.serialize(obj, result);
                try {
                    output.close();
                } catch (IOException e) {
                    throw new PersistentObjectException("error closing temporary file", e);
                }
                output = null;
            } finally {
                try {
                    if (output != null)
                        output.close();
                } catch (IOException e) {
                    // ignore
                }
            }

            // Rename file
            if (!tempFile.renameTo(this.persistentFile)) {
                throw new PersistentObjectException("error renaming temporary file `"
                  + tempFile.getName() + "' to `" + this.persistentFile.getName() + "'");
            }
            tempFile = null;
        } finally {
            if (tempFile != null)
                tempFile.delete();
        }
    }

    /**
     * Notify listeners.
     *
     * @param newVersion the version number associated with the new root
     */
    protected void notifyListeners(long newVersion, T oldRoot, T newRoot) {

        // Snapshot listeners
        final ArrayList<PersistentObjectListener<T>> listenersCopy = new ArrayList<PersistentObjectListener<T>>();
        synchronized (this.listeners) {
            listenersCopy.addAll(this.listeners);
        }

        // Notify them
        final PersistentObjectEvent<T> event = new PersistentObjectEvent<T>(this, newVersion, oldRoot, newRoot);
        this.notifyExecutor.submit(new Runnable() {
            @Override
            public void run() {
                PersistentObject.this.doNotifyListeners(listenersCopy, event);
            }
        });
    }

    /**
     * Handle an exception thrown during a delayed write-back attempt. {@link ThreadDeath} exceptions are not
     * passed to this method, but all others are.
     *
     * <p>
     * The implementation in {@link PersistentObject} simply logs the exception.
     *
     * @param t the exception thrown
     */
    protected void handleWritebackException(Throwable t) {
        this.log.error(this + ": error during write-back", t);
    }

    // Handle a write-back timeout
    private synchronized void writeTimeout() {

        // Check for cancel race
        if (this.pendingWrite == null)
            return;
        this.pendingWrite = null;

        // Write it
        try {
            this.write(this.root);
        } catch (ThreadDeath t) {
            throw t;
        } catch (Throwable t) {
            this.handleWritebackException(t);
        }
    }

    // Notify listeners. This is invoked in a separate thread.
    private void doNotifyListeners(ArrayList<PersistentObjectListener<T>> list, PersistentObjectEvent<T> event) {
        for (PersistentObjectListener<T> listener : list) {
            try {
                listener.handleEvent(event);
            } catch (ThreadDeath t) {
                throw t;
            } catch (Throwable t) {
                this.log.error(this + ": error notifying listener " + listener, t);
            }
        }
    }
}

