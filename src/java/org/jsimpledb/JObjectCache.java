
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;

import org.jsimpledb.core.ObjId;

class JObjectCache {

    private final JTransaction jtx;
    private final ThreadLocal<HashMap<ObjId, JObject>> instantiating = new ThreadLocal<>();
    private final ConcurrentMap<ObjId, JObject> cache = new MapMaker().weakValues().makeMap();

    JObjectCache(JTransaction jtx) {
        this.jtx = jtx;
        assert this.jtx != null;
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
        Preconditions.checkArgument(id != null, "null id");

        // Check current instantiations to avoid "recursive load" exception
        final HashMap<ObjId, JObject> currentInvocations = this.instantiating.get();
        if (currentInvocations != null) {
            final JObject jobj = currentInvocations.get(id);
            if (jobj != null)
                return jobj;
        }

        // Query cache
        JObject jobj = this.cache.get(id);
        if (jobj == null) {
            synchronized (this.cache) {
                if ((jobj = this.cache.get(id)) == null) {
                    jobj = this.createJObject(id);
                    this.cache.put(id, jobj);
                }
            }
        }

        // Done
        return jobj;
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

    private JObject createJObject(ObjId id) {

        // Get ClassGenerator
        final JClass<?> jclass = this.jtx.jdb.jclasses.get(id.getStorageId());
        final ClassGenerator<?> classGenerator = jclass != null ?
          jclass.getClassGenerator() : this.jtx.jdb.getUntypedClassGenerator();

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
            return (JObject)classGenerator.getConstructor().newInstance(this.jtx, id);
        } catch (Exception e) {
            Throwable cause = e;
            if (cause instanceof InvocationTargetException)
                cause = ((InvocationTargetException)cause).getTargetException();
            if (cause instanceof RuntimeException)
                throw (RuntimeException)cause;
            if (cause instanceof Error)
                throw (Error)cause;
            throw new JSimpleDBException("can't instantiate object for ID " + id, cause);
        } finally {
            if (cleanup)
                this.instantiating.remove();
            else
                currentInvocations.remove(id);
        }
    }
}

