
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
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

import org.dellroad.stuff.io.AtomicUpdateFileOutputStream;
import org.dellroad.stuff.io.FileStreamRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for Simple XML Persistence Objects (POBJ).
 *
 * <h3>Overview</h3>
 *
 * <p>
 * A {@link PersistentObject} instance manages an in-memory database with ACID semantics and strict validation that is
 * represented by a regular Java object graph (i.e., a root Java object and all of the other objects that it (indirectly)
 * references). The object graph can be (de)serialized to/from XML, and this XML representation is used for persistence
 * in the file system. The backing XML file is read in at initialization time and updated after each change.
 * </p>
 *
 * <p>
 * All changes required copying the entire object graph, and are atomic and strictly serialized. In other words, the
 * entire object graph is read and written by value. On the filesystem, the persistent XML file
 * is updated by writing out a new, temporary copy and renaming the copy onto the original, using {@link File#renameTo
 * File.renameTo()} so that changes are also atomic from a filesystem perspective, i.e., it's not possible to open an
 * invalid or partial XML file (assuming filesystem renames are atomic, e.g., all UNIX variants). This class also
 * supports information flowing in the other direction, where we pick up "out of band" updates to the XML file (see below).
 * </p>
 *
 * <p>
 * This class is most appropriate for use with information that must be carefully controlled and validated,
 * but that doesn't change frequently. Configuration information for an application stored in a {@code config.xml}
 * file is a typical use case; beans whose behavior is determined by the configured information can subclass
 * {@link AbstractConfiguredBean} and have their lifecycles managed automatically.
 * </p>
 *
 * <p>
 * Because each change involves copying of the entire graph, an efficient graph copy operation is desirable.
 * The {@linkplain AbstractDelegate#copy default method} is to serialize and then deserialize the object graph
 * to/from XML in memory. See {@link org.dellroad.stuff.java.GraphCloneable} for a much more efficient approach.
 * </p>
 *
 * <h3>Validation</h3>
 *
 * <p>
 * Validation is peformed in Java (not in XML) and defined by the provided {@link PersistentObjectDelegate}. This delegate
 * guarantees that only a valid root Java object can be read or written. Setting an invalid Java root object via
 * {@link #setRoot setRoot()} with throw an exception; reading an invalid XML file on disk will generate
 * an error (or be ignored; see "Empty Starts" below).
 * </p>
 *
 * <h3>Update Details</h3>
 *
 * <p>
 * When the object graph is updated, it must pass validation checks, and then the persistent XML file is updated and
 * listener notifications are sent out. Listeners are always notified in a separate thread from the one that invoked
 * {@link #setRoot setRoot()}. Support for delayed write-back of the persistent XML file is included: this
 * allows modifications that occur in rapid succession to be consolidated into a single filesystem write operation.
 * </p>
 *
 * <p>
 * Support for optimistic locking is included. There is a "current version" number which is incremented each
 * time the object graph is updated; writes may optionally specify this number to ensure no intervening changes
 * have occurred. If concurrent updates are expected, applications may choose to implement a 3-way merge algorithm
 * of some kind to handle optimistic locking failures.
 * </p>
 *
 * <p>
 * To implement a truly atomic read-modify-write operation without the possibility of locking failure, simply
 * synchronize on this instance, e.g.:
 *  <blockquote><pre>
 *  synchronized (pobj) {
 *      MyRoot root = pboj.getRoot();
 *      root.setFoobar("new value");    // or whatever else we want to modify
 *      pobj.setRoot(root);
 *  }
 *  </pre></blockquote>
 * </p>
 *
 * <p>
 * Instances can also be configured to automatically preserve one or more backup copies of the persistent file on systems that
 * support hard links (requires <a href="https://github.com/twall/jna">JNA</a>; see {@link FileStreamRepository}).
 * Set the {@link #getNumBackups numBackups} property to enable.
 * </p>
 *
 * <h3>"Out-of-band" Writes</h3>
 *
 * <p>
 * When a non-zero {@linkplain #getCheckInterval check interval} is configured, instances support "out-of-band" writes
 * directly to the XML persistent file by some other process. This can be handy in cases where the other process (perhaps hand
 * edits) is updating the persistent file and you want to have the running Java process pick up the changes just as if
 * {@link PersistentObject#setRoot setRoot()} had been invoked. In particular, instances will detect the appearance
 * of a newly appearing persistent file after has starting without one (see "empty starts" below). In all cases, persistent
 * objects must properly validate. To avoid reading a partial file the external process should write the file atomically by
 * creating a temporary file and renaming it; however, this race window is small and in any case the problem is self-correcting
 * because a partially written XML file will not validate, and so it will be ignored and retried after another
 * {@linkplain #getCheckInterval check interval} milliseconds has passed.
 * </p>
 *
 * <p>
 * A special case of this is effected when {@link PersistentObject#setRoot setRoot()} is never explicitly invoked
 * by the application. Then some other process must be responsible for all database updates via XML file, and this class
 * will automatically pick them up, validate them, and send out notifications to listeners.
 * </p>
 *
 * <h3>Empty Starts and Stops</h3>
 *
 * <p>
 * An "empty start" occurs when an instance is {@linkplain #start started} but the persistent XML file is either missing,
 * does not validate, or cannot be read for some other reason. In such cases, the instance will start with no object graph,
 * and {@link #getRoot} will return null. This situation will correct itself as soon as the object graph is written via
 * {@link #setRoot setRoot()} or the persistent file appears (effecting an "out-of-band" update). At that time, {@linkplain
 * #addListener listeners} will be notified for the first time, with {@link PersistentObjectEvent#getOldRoot} returning null.
 * </p>
 *
 * <p>
 * Whether empty starts are allowed is determined by the {@link #isAllowEmptyStart allowEmptyStart} property (default
 * {@code false}). When empty starts are disallowed and the persistent XML file cannot be successfully read,
 * then {@link #start} will instead throw an immediate {@link PersistentObjectException}.
 * </p>
 *
 * <p>
 * Similarly, "empty stops" are allowed when the {@link #isAllowEmptyStop allowEmptyStop} property is set to {@code true}
 * (by default it is {@code false}). An "empty stop" occurs when a null value is passed to {@link #setRoot setRoot()}
 * (note, an invalid XML file appearing on disk does not cause an empty start).
 * Subsequent invocations of {@link #getRoot} will return {@code null}. The persistent file is <b>not</b>
 * modified when null is passed to {@link #setRoot}. When empty stops are disallowed, then invoking
 * {@link #setRoot} with a {@code null} object will result in an {@link IllegalArgumentException}.
 * </p>
 *
 * <p>
 * Allowing empty starts and/or stops essentially creates an "unconfigured" state represented by a null root object.
 * When empty starts and empty stops are both disallowed, there is no "unconfigured" state: once started, {@link #getRoot}
 * can be relied upon to always return a non-null, validated root object.
 * </p>
 *
 * <p>
 * See {@link AbstractConfiguredBean} for a useful superclass that automatically handles starting and stopping
 * based on the state of an associated {@link PersistentObject}.
 * </p>
 *
 * <h3>Shared Roots</h3>
 *
 * Each time {@link #setRoot setRoot()} or {@link #getRoot getRoot()} is invoked, a deep copy of the root object
 * is made. This prevents external code from changing any node in the "official" object graph held by this instance,
 * and allows invokers of {@link #getRoot getRoot()} to modify to the returned object graph without affecting other invokers.
 * However, there may be cases where this deep copy is an expensive operation in terms of time or memory.
 * The {@link #getSharedRoot getSharedRoot()} method can be used in these situations. This method returns the same
 * root object each time it is invoked (this shared root is itself a deep copy of the "official" root). Therefore, only
 * the very first invocation pays the price of a copy. However, all invokers of {@link #getSharedRoot getSharedRoot()}
 * must treat the object graph as read-only to avoid each other seeing unexpected changes.
 * </p>
 *
 * <h3>Delegate Function</h3>
 *
 * <p>
 * Instances must be configured with a {@link PersistentObjectDelegate} that knows how to validate the object graph
 * and perform conversions to and from XML. See {@link PersistentObjectDelegate} and its implementations for details.
 * </p>
 *
 * <h3>Schema Changes</h3>
 *
 * <p>
 * Like any database, the XML schema may evolve over time. The {@link PersistentObjectSchemaUpdater} class provides a simple
 * way to apply and manage schema updates using XSLT transforms.
 * </p>
 *
 * @param <T> type of the root object
 * @see PersistentObjectDelegate
 * @see PersistentObjectSchemaUpdater
 * @see AbstractConfiguredBean
 */
public class PersistentObject<T> {

    private static final long EXECUTOR_SHUTDOWN_TIMEOUT = 1000;             // 1 second

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final HashSet<PersistentObjectListener<T>> listeners = new HashSet<PersistentObjectListener<T>>();

    private PersistentObjectDelegate<T> delegate;
    private FileStreamRepository streamRepository;
    private long writeDelay;
    private long checkInterval;

    private T root;                                         // current root object (private)
    private T sharedRoot;                                   // current root object (shared)
    private T writebackRoot;                                // pending persistent file writeback value
    private ScheduledExecutorService scheduledExecutor;     // used for file checking and delayed write-back
    private ExecutorService notifyExecutor;                 // used to notify listeners
    private ScheduledFuture<?> pendingWrite;                // a pending delayed write-back
    private long version;                                   // current root object version
    private long timestamp;                                 // timestamp of persistent file when we last read it
    private boolean allowEmptyStart;
    private boolean allowEmptyStop;
    private boolean started;

    /**
     * Constructor.
     *
     * <p>
     * The {@code writeDelay} is the maximum delay after an update operation before a write-back to the persistent file
     * must be initiated.
     *
     * @param delegate delegate supplying required operations
     * @param file the file used to persist
     * @param writeDelay write delay in milliseconds, or zero for immediate write-back
     * @param checkInterval check interval in milliseconds, or zero to disable persistent file checks
     * @throws IllegalArgumentException if {@code delegate} or {@code file} is null
     * @throws IllegalArgumentException if {@code writeDelay} or {@code checkInterval} is negative
     */
    public PersistentObject(PersistentObjectDelegate<T> delegate, File file, long writeDelay, long checkInterval) {
        this.setDelegate(delegate);
        this.setFile(file);
        this.setWriteDelay(writeDelay);
        this.setCheckInterval(checkInterval);
    }

    /**
     * Simplified constructor configuring for immediate write-back and no persistent file checks.
     *
     * <p>
     * Equivalent to:
     * <blockquote><code>
     *  PersistentObject(delegate, file, 0L, 0L);
     * </code></blockquote>
     */
    public PersistentObject(PersistentObjectDelegate<T> delegate, File file) {
        this(delegate, file, 0, 0);
    }

    /**
     * Default constructor. Caller must still configure the delegate and persistent file prior to start.
     */
    public PersistentObject() {
    }

    /**
     * Get the configured {@link PersistentObjectDelegate}.
     *
     * @return the delegate supplying required operations
     */
    public synchronized PersistentObjectDelegate<T> getDelegate() {
        return this.delegate;
    }

    /**
     * Configure the {@link PersistentObjectDelegate}.
     *
     * @param delegate delegate supplying required operations
     * @throws IllegalArgumentException if {@code delegate} is null
     * @throws IllegalStateException if this instance is started
     */
    public synchronized void setDelegate(PersistentObjectDelegate<T> delegate) {
        if (delegate == null)
            throw new IllegalArgumentException("null delegate");
        if (this.isStarted())
            throw new IllegalStateException("can't set the delegate while started");
        this.delegate = delegate;
    }

    /**
     * Get the persistent file containing the XML form of the persisted object.
     *
     * @return file used to persist the root object
     */
    public synchronized File getFile() {
        return this.streamRepository != null ? this.streamRepository.getFile() : null;
    }

    /**
     * Set the persistent file containing the XML form of the persisted object.
     *
     * @param file the file used to persist the root object
     * @throws IllegalArgumentException if {@code file} is null
     * @throws IllegalStateException if this instance is started
     */
    public synchronized void setFile(File file) {
        if (file == null)
            throw new IllegalArgumentException("null file");
        if (this.isStarted())
            throw new IllegalStateException("can't set the persistent file while started");
        this.streamRepository = new FileStreamRepository(file);
    }

    /**
     * Get the maximum delay after an update operation before a write-back to the persistent file
     * must be initiated.
     *
     * @return write delay in milliseconds, or zero for immediate write-back
     */
    public synchronized long getWriteDelay() {
        return this.writeDelay;
    }

    /**
     * Set the maximum delay after an update operation before a write-back to the persistent file
     * must be initiated.
     *
     * @param writeDelay write delay in milliseconds, or zero for immediate write-back
     * @throws IllegalArgumentException if {@code writeDelay} is negative
     * @throws IllegalStateException if this instance is started
     */
    public synchronized void setWriteDelay(long writeDelay) {
        if (writeDelay < 0)
            throw new IllegalArgumentException("negative writeDelay");
        if (this.isStarted())
            throw new IllegalStateException("can't set the write delay while started");
        this.writeDelay = writeDelay;
    }

    /**
     * Get the delay time between periodic checks for changes in the underlying persistent file.
     *
     * @return check interval in milliseconds, or zero if periodic checks are disabled
     */
    public synchronized long getCheckInterval() {
        return this.checkInterval;
    }

    /**
     * Set the delay time between periodic checks for changes in the underlying persistent file.
     *
     * @param checkInterval check interval in milliseconds, or zero if periodic checks are disabled
     * @throws IllegalArgumentException if {@code writeDelay} is negative
     * @throws IllegalStateException if this instance is started
     */
    public synchronized void setCheckInterval(long checkInterval) {
        if (checkInterval < 0)
            throw new IllegalArgumentException("negative checkInterval");
        if (this.isStarted())
            throw new IllegalStateException("can't set the check interval while started");
        this.checkInterval = checkInterval;
    }

    /**
     * Get the version of the current root.
     *
     * <p>
     * This returns a value which increases monotonically with each update.
     * The version number is not persisted with the persistent file; each instance of this class keeps
     * its own version count. The version is reset to zero when {@link #stop stop()} is invoked.
     *
     * @return the current positive object version, or zero if no value has been loaded yet or this instance is not started
     */
    public synchronized long getVersion() {
        return this.version;
    }

    /**
     * Get the number of backup copies to preserve.
     *
     * <p>
     * Backup files have suffixes of the form <code>.1</code>,  <code>.2</code>, etc.,
     * in reverse chronological order. Each time a new root object is written, the existing files are rotated.
     *
     * <p>
     * Back-ups are created via hard links and are only supported on UNIX systems.
     *
     * <p>
     * The default is zero, which disables backups.
     */
    public int getNumBackups() {
        return this.streamRepository != null ? this.streamRepository.getNumBackups() : 0;
    }

    /**
     * Set the number of backup copies to preserve.
     *
     * @throws IllegalArgumentException if {@code numBackups} is negative
     * @throws IllegalStateException if no persistent file has been configured yet
     * @see #getNumBackups
     */
    public void setNumBackups(int numBackups) {
        if (this.streamRepository == null)
            throw new IllegalStateException("the persistent file must be configured prior to the number of backups");
        this.streamRepository.setNumBackups(numBackups);
    }

    /**
     * Determine whether this instance should allow an "empty start".
     *
     * <p>
     * The default for this property is false.
     */
    public boolean isAllowEmptyStart() {
        return this.allowEmptyStart;
    }

    /**
     * Configure whether an "empty start" is allowed.
     *
     * <p>
     * The default for this property is false.
     *
     * @throws IllegalStateException if this instance is started
     */
    public synchronized void setAllowEmptyStart(boolean allowEmptyStart) {
        if (this.isStarted())
            throw new IllegalStateException("can't change whether empty starts are allowed while started");
        this.allowEmptyStart = allowEmptyStart;
    }

    /**
     * Determine whether this instance should allow an "empty stop".
     *
     * <p>
     * The default for this property is false.
     */
    public boolean isAllowEmptyStop() {
        return this.allowEmptyStop;
    }

    /**
     * Configure whether an "empty stop" is allowed.
     *
     * <p>
     * The default for this property is false.
     *
     * @throws IllegalStateException if this instance is started
     */
    public synchronized void setAllowEmptyStop(boolean allowEmptyStop) {
        if (this.isStarted())
            throw new IllegalStateException("can't set the check interval while started");
        this.allowEmptyStop = allowEmptyStop;
    }

    /**
     * Determine whether this instance is started.
     */
    public synchronized boolean isStarted() {
        return this.started;
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

        // Sanity check
        if (this.streamRepository == null)
            throw new PersistentObjectException("no file configured");
        if (this.delegate == null)
            throw new PersistentObjectException("no delegate configured");

        // Create executor services
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        this.notifyExecutor = Executors.newSingleThreadExecutor();

        // Read file (if it exists)
        this.log.info(this + ": starting");
        if (this.getFile().exists()) {

            // Apply file
            try {
                this.applyFile(this.getFile().lastModified(), true);
            } catch (PersistentObjectException e) {
                if (!this.isAllowEmptyStart())
                    throw e;
                this.log.warn("empty start: unable to load persistent file `" + this.getFile() + "': " + e);
            }
        } else {

            // Persistent file does not exist, so get default value from delegate
            final T defaultValue = this.delegate.getDefaultValue();

            // If no default value, we have an empty start situation, otherwise apply the default value
            if (defaultValue == null) {
                if (!this.isAllowEmptyStart()) {
                    throw new PersistentObjectException("persistent file `" + this.getFile() + "' does not exist,"
                      + " there is no default value, and empty starts are disallowed");
                }
                this.log.info(this + ": empty start: persistent file `" + this.getFile() + "' does not exist"
                  + " and there is no default value");
            } else {
                this.log.info(this + ": persistent file `" + this.getFile() + "' does not exist, applying default value");
                try {
                    this.setRootInternal(defaultValue, 0, false, true);
                } catch (PersistentObjectException e) {                             // e.g., validation failure
                    if (!this.isAllowEmptyStart())
                        throw e;
                    this.log.warn("empty start: unable to apply default value: " + e);
                }
            }
        }

        // Start checking the file
        if (this.checkInterval > 0) {
            this.scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    PersistentObject.this.checkFileTimeout();
                }
            }, this.checkInterval, this.checkInterval, TimeUnit.MILLISECONDS);
        }

        // Done
        this.started = true;
    }

    /**
     * Stop this instance. Does nothing if already stopped.
     *
     * @throws PersistentObjectException if a delayed write back is pending and error occurs while performing the write
     */
    public synchronized void stop() {

        // Already stopped?
        if (!this.started)
            return;

        // Perform any lingering pending save now
        if (this.cancelPendingWrite())
            this.writeback();

        // Stop executor services
        this.log.info(this + ": shutting down");
        this.scheduledExecutor.shutdown();
        this.notifyExecutor.shutdown();
        this.awaitTermination(this.scheduledExecutor, "scheduledExecutor");
        this.awaitTermination(this.notifyExecutor, "notifyExecutor");

        // Reset
        this.scheduledExecutor = null;
        this.notifyExecutor = null;
        this.root = null;
        this.sharedRoot = null;
        this.writebackRoot = null;
        this.timestamp = 0;
        this.started = false;
    }

    /**
     * Determine whether this instance has a non-null root object.
     *
     * @return true if this instance has a non-null root object, false if this instance is not started
     *  or is in an empty start or empty stop state
     */
    public boolean hasRoot() {
        return this.root != null;
    }

    /**
     * Atomically read the root object.
     *
     * <p>
     * In the situation of an empty start or empty stop, this instance is "unconfigured" and null will be returned.
     * This can only happen if {@link #setAllowEmptyStart setAllowEmptyStart(true)} or {@link #setAllowEmptyStop
     * setAllowEmptyStop(true)} has been invoked.
     *
     * <p>
     * This returns a deep copy of the current root object; any subsequent modifications are not written back.
     * Use {@link #getSharedRoot} instead to avoid the cost of the deep copy at the risk of seeing modifications
     * caused by other invokers.
     *
     * @return the current root instance, or null during an empty start or empty stop
     * @throws IllegalStateException if this instance is not started
     * @throws PersistentObjectException if an error occurs
     */
    public synchronized T getRoot() {

        // Sanity check
        if (!this.started)
            throw new IllegalStateException("not started");

        // Copy root
        return this.root != null ? this.delegate.copy(this.root) : null;
    }

    /**
     * Get a shared copy of the root object.
     *
     * <p>
     * This returns a copy of the root object, but it returns the same copy each time until the next change.
     * This method is more efficient than {@link #getRoot}, but all callers must agree not to modify the returned object
     * or any object in its graph of references.
     *
     * @return shared copy of the root instance, or null during an empty start or empty stop
     * @throws IllegalStateException if this instance is not started
     */
    public synchronized T getSharedRoot() {
        if (this.sharedRoot == null)
            this.sharedRoot = this.getRoot();
        return this.sharedRoot;
    }

    /**
     * Read the root object (as with {@link #getRoot}) and its version (as with {@link #getVersion}) in one atomic operation.
     * This avoids the race condition inherent in trying to perform these operations separately.
     *
     * @return snapshot of the current root, or null during an empty start or empty stop
     * @throws IllegalStateException if this instance is not started
     * @throws PersistentObjectException if an error occurs
     * @see #getRoot
     */
    public synchronized Snapshot getRootSnapshot() {
        T myRoot = this.getRoot();
        if (myRoot == null)
            return null;
        return new Snapshot(myRoot, this.version);
    }

    /**
     * Read the shared root object (as with {@link #getSharedRoot}) and its version (as with {@link #getVersion})
     * in one atomic operation.
     * This avoids the race condition inherent in trying to perform these operations separately.
     *
     * @return snapshot of the current shared root, or null during an empty start or empty stop
     * @throws IllegalStateException if this instance is not started
     * @throws PersistentObjectException if an error occurs
     * @see #getSharedRoot
     */
    public synchronized Snapshot getSharedRootSnapshot() {
        T mySharedRoot = this.getSharedRoot();
        if (mySharedRoot == null)
            return null;
        return new Snapshot(mySharedRoot, this.version);
    }

    /**
     * Atomically update the root object.
     *
     * <p>
     * The given object is deep-copied, the copy replaces the current root, and the new version number is returned.
     * If there is no write delay configured, the new version is written to the underlying file immediately and
     * a successful return from this method indicates the new root has been persisted. Otherwise, the write will
     * occur at a later time in a separate thread.
     * </p>
     *
     * <p>
     * If {@code expectedVersion} is non-zero, then if the current version is not equal to it,
     * a {@link PersistentObjectVersionException} exception is thrown. This mechanism
     * can be used for optimistic locking.
     * </p>
     *
     * <p>
     * If empty stops are allowed, then {@code newRoot} may be null, in which case it replaces the
     * current root and subsequent calls to {@link #getRoot} will return null. When a null
     * {@code newRoot} is set, the persistent file is <b>not</b> modified.
     * </p>
     *
     * <p>
     * If the given root object is {@linkplain PersistentObjectDelegate#isSameGraph the same as} the current
     * root object, then no action is taken and the current (unchanged) version number is returned.
     * </p>
     *
     * <p>
     * After a successful change, any registered {@linkplain PersistentObjectListener listeners} are notified in a
     * separate thread from the one that invoked this method.
     * </p>
     *
     * @param newRoot new persistent object
     * @param expectedVersion expected current version number, or zero to ignore the current version number
     * @return the new current version number (unchanged if {@code newRoot} is
     *  {@linkplain PersistentObjectDelegate#isSameGraph the same as} the current root)
     * @throws IllegalArgumentException if {@code newRoot} is null and empty stops are disallowed
     * @throws IllegalArgumentException if {@code expectedVersion} is negative
     * @throws IllegalStateException if this instance is not started
     * @throws PersistentObjectException if an error occurs
     * @throws PersistentObjectVersionException if {@code expectedVersion} is non-zero and not equal to the current version
     * @throws PersistentObjectValidationException if the new root has validation errors
     */
    public final synchronized long setRoot(T newRoot, long expectedVersion) {
        return this.setRootInternal(newRoot, expectedVersion, false, false);
    }

    private synchronized long setRootInternal(T newRoot, long expectedVersion, boolean readingFile, boolean allowNotStarted) {

        // Sanity check
        if (newRoot == null && !this.isAllowEmptyStop())
            throw new IllegalArgumentException("newRoot is null but empty stops are not enabled");
        if (!this.started && !allowNotStarted)
            throw new IllegalStateException("not started");
        if (expectedVersion < 0)
            throw new IllegalStateException("negative expectedVersion");

        // Check version number
        if (expectedVersion != 0 && this.version != expectedVersion)
            throw new PersistentObjectVersionException(this.version, expectedVersion);

        // Check for sameness
        if (this.root == null && newRoot == null)
            return this.version;
        if (this.root != null && newRoot != null && this.delegate.isSameGraph(this.root, newRoot))
            return this.version;

        // Validate the new root
        if (newRoot != null) {
            Set<ConstraintViolation<T>> violations = this.delegate.validate(newRoot);
            if (!violations.isEmpty())
                throw new PersistentObjectValidationException(violations);
        }

        // Do the update
        final T oldRoot = this.root;
        this.root = newRoot != null ? this.delegate.copy(newRoot) : null;
        this.version++;
        this.sharedRoot = null;

        // Perform write-back, either now or later
        if (!readingFile && this.root != null) {
            this.writebackRoot = this.root;
            if (this.writeDelay == 0)
                this.writeback();
            else if (this.pendingWrite == null) {
                this.pendingWrite = this.scheduledExecutor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        PersistentObject.this.writeTimeout();
                    }
                }, this.writeDelay, TimeUnit.MILLISECONDS);
            }
        }

        // Notify listeners
        this.notifyListeners(this.version, oldRoot, newRoot);

        // Done
        return this.version;
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
    public final synchronized long setRoot(T newRoot) {
        return this.setRoot(newRoot, 0);
    }

    /**
     * Check the persistent file for an "out-of-band" update.
     *
     * <p>
     * If the persistent file has a newer timestamp than the timestamp of the most recently read
     * or written version, then it will be read and applied to this instance.
     *
     * @throws IllegalStateException if this instance is not started
     * @throws PersistentObjectException if an error occurs
     */
    public synchronized void checkFile() {

        // Sanity check
        if (!this.started)
            throw new IllegalStateException("not started");

        // Get file timestamp
        long fileTime = this.getFile().lastModified();
        if (fileTime == 0)
            return;

        // Check whether file has newly appeared or just been updated
        if (this.timestamp != 0 && fileTime <= this.timestamp)
            return;

        // Read new file
        this.log.info(this + ": detected out-of-band update of persistent file `" + this.getFile() + "'");
        this.applyFile(fileTime, false);
    }

    /**
     * Add a listener to be notified each time the object graph changes.
     *
     * <p>
     * Listeners are notified in a separate thread from the one that caused the root object to change.
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
     * Remove a listener added via {@link #addListener addListener()}.
     */
    public void removeListener(PersistentObjectListener<T> listener) {
        synchronized (this.listeners) {
            this.listeners.remove(listener);
        }
    }

    /**
     * Get a simple string description of this instance. This description appears in all log messages.
     */
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + this.getFile().getName() + "]";
    }

    /**
     * Read the persistent file. Does not validate it.
     *
     * @throws PersistentObjectException if the file does not exist or cannot be read
     * @throws PersistentObjectException if an error occurs
     */
    protected T read() {

        // Open file
        this.log.info(this + ": reading persistent file `" + this.getFile() + "'");
        BufferedInputStream input;
        try {
            input = new BufferedInputStream(this.streamRepository.getInputStream());
        } catch (IOException e) {
            throw new PersistentObjectException("error opening persistent file", e);
        }

        // Parse XML
        try {
            StreamSource source = new StreamSource(input);
            source.setSystemId(this.getFile());
            return PersistentObject.read(this.delegate, source, false);
        } finally {
            try {
                input.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Write the persistent file and rotate any backups.
     *
     * <p>
     * A temporary file is created in the same directory and then renamed to provide for an atomic update
     * (on supporting operating systems).
     * </p>
     *
     * @param obj root object to write
     * @throws IllegalArgumentException if {@code obj} is null
     * @throws PersistentObjectException if an error occurs
     */
    protected final synchronized void write(T obj) {

        // Sanity check
        if (obj == null)
            throw new IllegalArgumentException("null obj");

        // Open atomic update output and buffer it
        AtomicUpdateFileOutputStream updateOutput;
        try {
            updateOutput = this.streamRepository.getOutputStream();
        } catch (IOException e) {
            throw new PersistentObjectException("error creating temporary file", e);
        }
        BufferedOutputStream output = new BufferedOutputStream(updateOutput);

        // Serialize to XML
        try {

            // Set up XML result
            Result result = this.createResult(output, updateOutput.getTempFile());

            // Serialize root object
            PersistentObject.write(obj, this.delegate, result);

            // Commit output
            try {
                output.close();
            } catch (IOException e) {
                throw new PersistentObjectException("error closing temporary file", e);
            }

            // Success
            output = null;
        } finally {
            if (output != null)
                updateOutput.cancel();
        }

        // Update file timestamp
        this.timestamp = this.streamRepository.getFileTimestamp();
    }

    /**
     * Create a {@link Result} targeting the given {@link OutputStream}.
     *
     * <p>
     * The implementation in {@link PersistentObject} creates and returns a {@link StreamResult}.
     * </p>
     *
     * @param output XML output stream
     * @param systemId system ID
     */
    protected Result createResult(OutputStream output, File systemId) {
        StreamResult result = new StreamResult(output);
        result.setSystemId(systemId);
        return result;
    }

    /**
     * Notify listeners of a change in value.
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

    // Read the persistent file and apply it
    private synchronized void applyFile(long newTimestamp, boolean allowNotStarted) {
        this.cancelPendingWrite();
        this.timestamp = newTimestamp;                              // update timestamp even if update fails to avoid loops
        this.setRootInternal(this.read(), 0, true, allowNotStarted);
    }

    // Handle a write-back timeout
    private synchronized void writeTimeout() {

        // Check for cancel race
        if (this.pendingWrite == null)
            return;
        this.pendingWrite = null;

        // Write it
        try {
            this.writeback();
        } catch (ThreadDeath t) {
            throw t;
        } catch (Throwable t) {
            this.delegate.handleWritebackException(this, t);
        }
    }

    // Handle a check file timeout
    private synchronized void checkFileTimeout() {

        // Handle race condition
        if (!this.started)
            return;

        // Check file
        try {
            this.checkFile();
        } catch (ThreadDeath t) {
            throw t;
        } catch (Throwable t) {
            this.log.error(this + ": error attempting to apply out-of-band update", t);
        }
    }

    // Write back root to persistent file
    private synchronized void writeback() {
        T objectToWrite = this.writebackRoot;
        assert objectToWrite != null;
        this.writebackRoot = null;
        this.write(objectToWrite);
    }

    // Cancel a pending write and return true if there was one
    private synchronized boolean cancelPendingWrite() {
        if (this.pendingWrite == null)
            return false;
        this.pendingWrite.cancel(false);
        this.pendingWrite = null;
        return true;
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

    // Wait for an ExecutorService to completely shut down
    private void awaitTermination(ExecutorService executor, String name) {
        boolean shutdown = false;
        try {
            shutdown = executor.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            this.log.warn(this + ": interrupted while awaiting " + name + " termination");
            Thread.currentThread().interrupt();
        }
        if (!shutdown)
            this.log.warn(this + ": failed to completely shut down " + name);
    }

// Convience reader & writer

    /**
     * Read in a persistent object using the given delegate.
     *
     * <p>
     * This is a convenience method that can be used for a one-time deserialization from an XML {@link Source} without having
     * to go through the whole {@link PersistentObject} lifecycle.
     * </p>
     *
     * @param delegate delegate supplying required operations
     * @param source source for serialized root object
     * @param validate whether to also validate the root object
     * @return deserialized root object, never null
     * @throws IllegalArgumentException if any parameter is null
     * @throws PersistentObjectValidationException if {@code validate} is true and the deserialized root has validation errors
     * @throws PersistentObjectException if an error occurs
     */
    public static <T> T read(PersistentObjectDelegate<T> delegate, Source source, boolean validate) {

        // Sanity check
        if (delegate == null)
            throw new IllegalArgumentException("null delegate");
        if (source == null)
            throw new IllegalArgumentException("null source");

        // Parse XML
        T root;
        try {
            root = delegate.deserialize(source);
        } catch (IOException e) {
            throw new PersistentObjectException("error reading persistent object", e);
        }

        // Check result
        if (root == null)
            throw new PersistentObjectException("null object returned by delegate.deserialize()");

        // Validate result
        if (validate) {
            final Set<ConstraintViolation<T>> violations = delegate.validate(root);
            if (!violations.isEmpty())
                throw new PersistentObjectValidationException(violations);
        }

        // Done
        return root;
    }

    /**
     * Read in a persistent object from the given {@link File} using the given delegate.
     *
     * <p>
     * This is a wrapper around {@link #read(PersistentObjectDelegate, Source, boolean)} that handles
     * opening and closing the given {@link File}.
     * </p>
     *
     * @param delegate delegate supplying required operations
     * @param file file to read from
     * @param validate whether to also validate the root object
     * @return deserialized root object, never null
     * @throws IllegalArgumentException if any parameter is null
     * @throws PersistentObjectValidationException if {@code validate} is true and the deserialized root has validation errors
     * @throws PersistentObjectException if {@code file} cannot be read
     * @throws PersistentObjectException if an error occurs
     */
    public static <T> T read(PersistentObjectDelegate<T> delegate, File file, boolean validate) {

        // Open file
        BufferedInputStream input;
        try {
            input = new BufferedInputStream(new FileInputStream(file));
        } catch (IOException e) {
            throw new PersistentObjectException("error opening persistent file", e);
        }

        // Parse XML
        try {
            StreamSource source = new StreamSource(input);
            source.setSystemId(file);
            return PersistentObject.read(delegate, source, validate);
        } finally {
            try {
                input.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Write a persistent object using the given delegate.
     *
     * <p>
     * This is a convenience method that can be used for one-time serialization to an XML {@link Result} without having
     * to go through the whole {@link PersistentObject} lifecycle.
     * </p>
     *
     * @param root root object to serialize
     * @param delegate delegate supplying required operations
     * @param result destination
     * @throws IllegalArgumentException if any parameter is null
     * @throws PersistentObjectException if an error occurs
     */
    public static <T> void write(T root, PersistentObjectDelegate<T> delegate, Result result) {

        // Sanity check
        if (root == null)
            throw new IllegalArgumentException("null root");
        if (delegate == null)
            throw new IllegalArgumentException("null delegate");
        if (result == null)
            throw new IllegalArgumentException("null result");

        // Serialize root object
        try {
            delegate.serialize(root, result);
        } catch (IOException e) {
            throw new PersistentObjectException("error writing persistent file", e);
        }
    }

// Snapshot class

    /**
     * Holds a "snapshot" of a {@link PersistentObject} root object along with the version number
     * corresponding to the snapshot.
     *
     * @see PersistentObject#getRootSnapshot
     */
    public class Snapshot {

        private final T root;
        private final long version;

        public Snapshot(T root, long version) {
            this.root = root;
            this.version = version;
        }

        /**
         * Get the persistent root associated with this instance.
         */
        public T getRoot() {
            return this.root;
        }

        /**
         * Get the version number of the persistent root associated with this instance.
         */
        public long getVersion() {
            return this.version;
        }
    }
}

