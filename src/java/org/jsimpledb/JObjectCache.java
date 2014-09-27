
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import org.jsimpledb.core.ObjId;

abstract class JObjectCache {

    private final JSimpleDB jdb;
    private final ThreadLocal<HashMap<ObjId, JObject>> instantiating = new ThreadLocal<>();
    private final LoadingCache<ObjId, JObject> cache = CacheBuilder.newBuilder().weakValues().build(
      new CacheLoader<ObjId, JObject>() {
        @Override
        public JObject load(ObjId id) throws Exception {
            return JObjectCache.this.createJObject(id);
        }
    });

    JObjectCache(JSimpleDB jdb) {
        if (jdb == null)
            throw new IllegalArgumentException("null jdb");
        this.jdb = jdb;
    }

    /**
     * Get the Java model object corresponding to the given object ID.
     *
     * @param id object ID
     * @return Java model object
     * @throws IllegalArgumentException if {@code id} is null
     */
    public JObject getJObject(ObjId id) {

        // Sanity check
        if (id == null)
            throw new IllegalArgumentException("null id");

        // Check current instantiations to avoid "recursive load" exception
        final HashMap<ObjId, JObject> currentInvocations = this.instantiating.get();
        if (currentInvocations != null) {
            final JObject jobj = currentInvocations.get(id);
            if (jobj != null)
                return jobj;
        }

        // Query cache
        Throwable cause;
        try {
            return this.cache.get(id);
        } catch (ExecutionException e) {
            cause = e.getCause() != null ? e.getCause() : e;
        } catch (UncheckedExecutionException e) {
            cause = e.getCause() != null ? e.getCause() : e;
        }

        // Handle exception
        if (cause instanceof InvocationTargetException)
            cause = ((InvocationTargetException)cause).getTargetException();
        if (cause instanceof JSimpleDBException)
            throw (JSimpleDBException)cause;
        if (cause instanceof Error)
            throw (Error)cause;
        throw new JSimpleDBException("can't instantiate object for ID " + id, cause);
    }

    /**
     * Register the given {@link JObject} with this instance in the case that the Java model class construtor invokes
     * a {@link JTransaction} method before returning (and so we won't have registered it yet). This allows re-entrant
     * invocations of {@link #getJObject} involving Java model class constructors to work properly. Otherwise,
     * a "recursive load" exception is thrown by Guava's {@link LoadingCache}.
     */
    void registerJObject(JObject jobj) {

        // Are we currently instantiating this JObject?
        if (jobj == null)
            return;
        final HashMap<ObjId, JObject> currentInvocations = this.instantiating.get();
        if (currentInvocations == null)
            return;
        final ObjId id = jobj.getObjId();
        if (!currentInvocations.containsKey(id))
            return;

        // Associate the JObject in our thread-local map until it can be properly loaded into the cache
        final JObject previous = currentInvocations.put(jobj.getObjId(), jobj);
        if (previous != null && previous != jobj)
            throw new IllegalArgumentException("conflicting jobj registration: " + jobj + " != " + previous);
    }

    private JObject createJObject(ObjId id) throws Exception {

        // Get ClassGenerator
        final JClass<?> jclass = this.jdb.getJClass(id.getStorageId());
        final ClassGenerator<?> classGenerator = jclass != null ?
          jclass.getClassGenerator() : this.jdb.getUntypedClassGenerator();

        // Set up currently instantiating objects map, if not already set up
        HashMap<ObjId, JObject> currentInvocations = this.instantiating.get();
        boolean cleanup = false;
        if (currentInvocations == null) {
            currentInvocations = new HashMap<ObjId, JObject>();
            this.instantiating.set(currentInvocations);
            cleanup = true;
        }

        // Set flag indicating that we are instantiating this JObject
        currentInvocations.put(id, null);

        // Instantiate new JObject
        try {
            return this.instantiate(classGenerator, id);
        } finally {
            if (cleanup)
                this.instantiating.remove();
            else
                currentInvocations.remove(id);
        }
    }

    protected abstract JObject instantiate(ClassGenerator<?> classGenerator, ObjId id) throws Exception;
}

