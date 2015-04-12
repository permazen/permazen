
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.util;

import java.io.Closeable;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps track of {@link Closeable} objects (termed <i>items</i>) that must be reliably {@linkplain Closeable#close closed}
 * prior to shutting down some associated context, but that also must not be closed prior to becoming unreachable, when
 * the straightforward strategy of just storing them until context shutdown would require too much memory. In a
 * typical scenario, these items are returned (indirectly by reference from some associated <i>holder</i> object) from some
 * method to callers who cannot be expected to reliably close them. Following the simple strategy of just storing all unclosed
 * items until context shutdown means unbounded memory use can occur; this class solves that problem.
 *
 * <p>
 * For each such context, an instance of this class may be used to register and track the associated items,
 * guaranteeing that they will always eventually be closed, but doing so in a way that avoids memory leaks:
 * For each item, there must be a corresponding <i>holder</i> object containing a reference to it. The holder objects are
 * then tracked by this class using weak references. When a holder object is no longer strongly referenced, the corresponding
 * item is closed. With this scheme, no memory leak occurs due to this tracking, even when arbitrarily many items are created.
 * This of course assumes that when a holder object is no longer referenced, the associated item may be safely closed.
 * </p>
 *
 * <p>
 * A registered {@link Closeable} item is closed at the first occurrence of any of the following:
 * <ul>
 *  <li>
 *  <li>The application itself invokes {@link Closeable#close Closeable.close()} on the item</li>
 *  <li>The associated holder object is no longer strongly referenced, and then {@link #poll} is invoked</li>
 *  <li>{@link CloseableTracker#close} is invoked</li>
 *  <li>{@link CloseableTracker#finalize} is invoked (i.e., this instance is garbage collected)</li>
 * </ul>
 *
 * <p>
 * Note that {@link Closeable#close Closeable.close()} is required to be idempotent, so application usage
 * that results in eager closing of items is appropriate and encouraged. Use of this class may occasionally
 * result in {@link Closeable#close Closeable.close()} being invoked more than once for registered items.
 * </p>
 *
 * <p>
 * Instances of this class are thread safe.
 * </p>
 */
public class CloseableTracker implements Closeable {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private HashSet<HolderRef> unclosedItems;
    private ReferenceQueue<Object> queue;

// Constructors

    /**
     * Constructor.
     */
    public CloseableTracker() {
        this.reset();
    }

// Public API

    /**
     * Add an item to the set of items being tracked by this instance.
     *
     * @param holder item's holder
     * @param item item to track
     * @throws IllegalArgumentException if either parameter is null
     */
    public synchronized void add(Object holder, Closeable item) {
        this.unclosedItems.add(new HolderRef(holder, item, this.queue));
    }

    /**
     * Poll the internal weak reference queue and close any unclosed items whose holders are no longer reachable.
     *
     * <p>
     * Applications should invoke this method periodically to avoid memory leaks.
     * </p>
     */
    public void poll() {

        // Poll for refs and close the associated items
        ArrayList<HolderRef> closedItems = null;
        for (HolderRef ref; (ref = (HolderRef)this.queue.poll()) != null; ) {
            if (closedItems == null)
                closedItems = new ArrayList<>();
            final Closeable item = ref.getItem();
            try {
                item.close();
            } catch (Throwable e) {
                this.exceptionDuringClose(item, e);
            }
            closedItems.add(ref);
        }

        // Remove closed items from the set of unclosed items
        if (closedItems != null) {
            synchronized (this) {
                this.unclosedItems.removeAll(closedItems);
            }
        }
    }

    /**
     * Close all unclosed items associated with this instance.
     *
     * <p>
     * The implementation in {@link CloseableTracker} just invokes {@link #reset}.
     * </p>
     */
    @Override
    public void close() {
        this.reset();
    }

    /**
     * Reset this instance.
     *
     * <p>
     * Results in the forced closing of all unclosed items associated with this instance.
     * </p>
     *
     * <p>
     * This instance may be reused after invoking this method.
     * </p>
     */
    public void reset() {

        // Snapshot remaining items and reset state
        final HashSet<HolderRef> itemsToClose;
        synchronized (this) {
            itemsToClose = this.unclosedItems;
            this.unclosedItems = new HashSet<>();
            this.queue = new ReferenceQueue<>();            // discard old queue, everything on it is about to be closed
        }

        // This case happens during construction
        if (itemsToClose == null)
            return;

        // Close unclosed items
        for (HolderRef ref : itemsToClose) {
            final Closeable item = ref.getItem();
            try {
                item.close();
            } catch (Throwable e) {
                this.exceptionDuringClose(item, e);
            }
        }
    }

// Subclass methods

    /**
     * Handle an exception thrown while attempting to {@link Closeable#close close()} and item.
     *
     * <p>
     * The implementation in {@link CloseableTracker} just logs the exception at debug level and returns.
     * </p>
     *
     * @param item item that was closed
     * @param e exeption thrown
     */
    protected void exceptionDuringClose(Closeable item, Throwable e) {
        if (this.log.isDebugEnabled())
            this.log.debug("caught exception attempting to close " + item + " (ignoring)", e);
    }

// Object

    @Override
    protected void finalize() throws Throwable {
        try {
            this.close();
        } finally {
            super.finalize();
        }
    }

// HolderRef

    private static class HolderRef extends WeakReference<Object> {

        private final Closeable item;

        HolderRef(Object holder, Closeable item, ReferenceQueue<Object> queue) {
            super(holder, queue);
            if (holder == null)
                throw new IllegalArgumentException("null holder");
            if (item == null)
                throw new IllegalArgumentException("null item");
            this.item = item;
        }

        public Closeable getItem() {
            return this.item;
        }
    }
}

