
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import io.permazen.core.ObjId;
import io.permazen.core.util.ObjIdMap;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationTargetException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Caches {@link PermazenObject}'s for a {@link PermazenTransaction}.
 */
@ThreadSafe
class PermazenObjectCache {

    private final PermazenTransaction ptx;
    private final ReferenceQueue<PermazenObject> referenceQueue = new ReferenceQueue<>();

    /**
     * Mapping from object ID to {@link PermazenObject}.
     *
     * <p>
     * As a special case, null values in this map indicate that the corresponding {@link PermazenObject}
     * is currently under construction by some thread.
     */
    @GuardedBy("itself")
    private final ObjIdMap<JObjRef> cache = new ObjIdMap<>();

    /**
     * Mapping of {@link PermazenObject}s currently under construction by the current thread.
     *
     * <p>
     * These {@link PermazenObject}s are kept private to each thread until the Java constructor successfully returns.
     */
    private final ThreadLocal<ObjIdMap<PermazenObject>> instantiations = new ThreadLocal<>();

    PermazenObjectCache(PermazenTransaction ptx) {
        this.ptx = ptx;
        assert this.ptx != null;
    }

    /**
     * Get the Java model object corresponding to the given object ID if it exists.
     *
     * @param id object ID
     * @return Java model object, or null if object not created yet
     * @throws IllegalArgumentException if {@code id} is null
     */
    public PermazenObject getIfExists(ObjId id) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");

        // Check for existing entry
        synchronized (this.cache) {
            final JObjRef ref = this.cache.get(id);
            if (ref != null)
                return ref.get();
        }

        // Does not exist - or is currently being instantiated
        return null;
    }

    /**
     * Get the Java model object corresponding to the given object ID, creating it if necessary.
     *
     * @param id object ID
     * @return Java model object
     * @throws IllegalArgumentException if {@code id} is null
     */
    public PermazenObject get(ObjId id) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");

        // Check for existing entry
        boolean interrupted = false;
        synchronized (this.cache) {

            // Check for existing PermazenObject, or null if object is being instantiated
            while (true) {

                // Garbage collect
                this.gc();

                // Get weak reference
                final JObjRef ref = this.cache.get(id);
                if (ref != null) {

                    // If weak reference still valid, return corresponding PermazenObject
                    final PermazenObject pobj = ref.get();
                    if (pobj != null)
                        return pobj;

                    // The weak reference has been cleared; we will construct a new PermazenObject replacement
                    // this.cache.remove(id);   // not necessary; see below
                } else if (this.cache.containsKey(id)) {    // null value indicates object is being instantiated by some thread

                    // Is the current thread the one instantiating the object?
                    final ObjIdMap<PermazenObject> threadInstantiations = this.instantiations.get();
                    if (threadInstantiations != null && threadInstantiations.containsKey(id)) {
                        final PermazenObject pobj = threadInstantiations.get(id);
                        if (pobj != null)
                            return pobj;
                        throw new RuntimeException(String.format(
                          "illegal reentrant query for object %s during object construction", id));
                    }

                    // Some other thread is instantiating the object, so wait for it to finish doing so
                    try {
                        this.cache.wait();
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                    continue;
                }

                // Set a null value in the cache to indicate that some thread (i.e., this one) is instantiating the object
                this.cache.put(id, null);
                break;
            }
        }

        // Instantiate new PermazenObject instance
        PermazenObject pobj = null;
        try {
            pobj = this.createPermazenObject(id);
        } finally {
            synchronized (this.cache) {
                assert this.cache.containsKey(id) && this.cache.get(id) == null;
                this.gc();

                // Add PermazenObject to the cache, or else remove the 'under construction' flag
                if (pobj != null)
                    this.cache.put(id, new JObjRef(pobj, this.referenceQueue));
                else
                    this.cache.remove(id);

                // Wakeup any waiting threads
                this.cache.notifyAll();
            }
        }

        // Re-interrupt the current thread if needed
        if (interrupted)
            Thread.currentThread().interrupt();

        // Done
        return pobj;
    }

    /**
     * Register the given {@link PermazenObject} with this instance.
     *
     * Use of this method is required to handle the case where the Java model class constructor invokes, before the constructor
     * has even returned, a {@link PermazenTransaction} method that needs to find the {@link PermazenObject} being constructed
     * by its object ID. In that case, we don't have the {@link PermazenObject} in our cache yet. This methods puts it in there
     * if necessary.
     */
    void register(PermazenObject pobj) {

        // Sanity check
        Preconditions.checkArgument(pobj != null, "null pobj");
        Preconditions.checkArgument(pobj.getTransaction() == this.ptx, "wrong tx");

        // Get object ID
        final ObjId id = pobj.getObjId();

        // Is the PermazenObject under construction by the current thread? If not, don't do anything.
        final ObjIdMap<PermazenObject> threadInstantiations = this.instantiations.get();
        if (threadInstantiations == null || !threadInstantiations.containsKey(id))
            return;

        // Add PermazenObject to this thread's 'under construction' map; also check for weird conflict
        final PermazenObject previous = threadInstantiations.put(id, pobj);
        if (previous != null && previous != pobj) {
            threadInstantiations.put(id, previous);
            throw new IllegalArgumentException(String.format("conflicting PermazenObject registration: %s != %s", pobj, previous));
        }
    }

    /**
     * Create the {@link PermazenObject} for the given object ID.
     *
     * <p>
     * Put an entry in this thread's instantiation map while doing so.
     */
    private PermazenObject createPermazenObject(ObjId id) {

        // Get ClassGenerator
        final PermazenClass<?> pclass = this.ptx.pdb.pclassesByStorageId.get(id.getStorageId());
        final ClassGenerator<?> classGenerator = pclass != null ?
          pclass.getClassGenerator() : this.ptx.pdb.getUntypedClassGenerator();

        // Set flag indicating that the object is being instantiated by this thread
        ObjIdMap<PermazenObject> threadInstantiations = this.instantiations.get();
        if (threadInstantiations == null) {
            threadInstantiations = new ObjIdMap<>(1);
            this.instantiations.set(threadInstantiations);
        }
        threadInstantiations.put(id, null);

        // Instantiate the PermazenObject
        final PermazenObject pobj;
        final PermazenObject registered;
        try {
            pobj = (PermazenObject)classGenerator.getConstructor().newInstance(this.ptx, id);
        } catch (Exception e) {
            Throwable cause = e;
            if (cause instanceof InvocationTargetException)
                cause = ((InvocationTargetException)cause).getTargetException();
            Throwables.throwIfUnchecked(cause);
            throw new PermazenException(String.format("can't instantiate object for ID %s", id), cause);
        } finally {

            // Get object registered in the meantime, if any
            registered = threadInstantiations.remove(id);

            // Discard thread local if no longer needed
            if (threadInstantiations.isEmpty())
                this.instantiations.remove();
        }

        // Sanity check we didn't register the wrong object
        if (registered != null && registered != pobj) {
            throw new IllegalArgumentException(String.format(
              "conflicting PermazenObject registration: %s != %s", pobj, registered));
        }

        // Done
        assert pobj != null;
        assert pobj.getObjId().equals(id);
        return pobj;
    }

    private void gc() {
        assert Thread.holdsLock(this.cache);
        while (true) {
            final JObjRef ref = (JObjRef)this.referenceQueue.poll();
            if (ref == null)
                break;
            assert ref.get() == null;
            final ObjId id = ref.getObjId();
            if (this.cache.get(id) == ref)  // avoid race where old reference is cleared after being replaced in the cache
                this.cache.remove(id);
        }
    }

// JObjRef

    private static class JObjRef extends WeakReference<PermazenObject> {

        private final long id;                                  // try to be memory efficient, avoiding extra objects

        JObjRef(PermazenObject pobj, ReferenceQueue<PermazenObject> queue) {
            super(pobj, queue);
            this.id = pobj.getObjId().asLong();
        }

        public ObjId getObjId() {
            return new ObjId(this.id);
        }
    }
}
