
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.core.ObjId;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * Context for importing plain (POJO) objects into a {@link JTransaction}.
 *
 * <p>
 * Plain objects (POJO's) can be imported into a {@link JTransaction} to the extent that the POJO class and
 * the corresponding Permazen model class share the same properties. The simplest example of this is when
 * the POJO class is also the Permazen model class (implying a non-abstract class; see also
 * {@link io.permazen.annotation.PermazenType#autogenNonAbstract &#64;PermazenType.autogenNonAbstract()}). Also possible are POJO
 * classes and model classes that implement common Java interfaces.
 *
 * <p>
 * The {@link ObjId} for the corresponding imported Permazen object is determined by the configured {@code storageIdMapper}.
 * If {@code storageIdMapper} returns null, the POJO is not imported, and nulls replace any copied references to it; otherwise,
 * the returned object must exist in the transaction. If {@code storageIdMapper} is itself null, the default behavior is
 * to create a new Permazen object using {@link JTransaction#create(Class)}, providing the POJO's class as the model class.
 *
 * <p>
 * Instances ensure that an already-imported POJO will be recognized and not imported twice.
 * Note this determination is based on object identity, not {@link Object#equals Object.equals()}.
 * The {@code storageIdMapper} is invoked at most once for any POJO.
 *
 * <p>
 * Once the target object for a POJO has been identified, common properties are copied, overwriting any previous values.
 * POJO properties that are not Permzen properties are ignored; conversely, properties that exist in the Permazen model
 * object type but are missing in the POJO are left unmodified.
 *
 * <p>
 * Permazen reference fields are automatically imported as POJO's, recursively. In other words, the entire transitive closure
 * of POJO's reachable from an imported object is imported. Cycles in the graph of references are handled properly.
 *
 * <p><b>Conversion Details</b></p>
 *
 * {@link Counter} fields import from any {@link Number} property.
 *
 * @see ExportContext
 */
public class ImportContext {

    private final JTransaction jtx;
    private final Function<Object, ObjId> storageIdMapper;
    private final IdentityHashMap<Object, ObjId> objectMap = new IdentityHashMap<>();
    private final IdentityHashMap<Object, ObjId> needingFieldsCopied = new IdentityHashMap<>();

    /**
     * Constructor.
     *
     * <p>
     * Uses a default {@code storageIdMapper} that creates new instances for imported objects via
     * {@link JTransaction#create(Class)}, using the Permazen model type found by
     * {@link Permazen#findJClass(Class)} when given the POJO's class.
     *
     * @param jtx the transaction in which to import objects
     * @throws IllegalArgumentException if {@code jtx} is null
     */
    public ImportContext(JTransaction jtx) {
        Preconditions.checkArgument(jtx != null);
        this.jtx = jtx;
        this.storageIdMapper = obj -> {
            final JClass<?> modelClass = this.jtx.jdb.findJClass(obj.getClass());
            if (modelClass == null)
                throw new IllegalArgumentException("no Permazen model class corresponds to POJO " + obj.getClass());
            return ((JObject)this.jtx.create(modelClass)).getObjId();
        };
    }

    /**
     * Constructor.
     *
     * @param jtx the transaction in which to import objects
     * @param storageIdMapper function assigning {@link ObjId}'s to imported objects (or null to skip the corresponding object)
     * @throws IllegalArgumentException if either parameter is null
     */
    public ImportContext(JTransaction jtx, Function<Object, ObjId> storageIdMapper) {
        Preconditions.checkArgument(jtx != null);
        Preconditions.checkArgument(storageIdMapper != null);
        this.jtx = jtx;
        this.storageIdMapper = storageIdMapper;
    }

    /**
     * Get the destination transaction for imported objects.
     *
     * @return associated transaction
     */
    public JTransaction getTransaction() {
        return this.jtx;
    }

    /**
     * Get the mapping from already imported POJO's to their corresponding database objects.
     *
     * @return mapping from imported POJO to corresponding database object ID
     */
    public Map<Object, ObjId> getObjectMap() {
        return Collections.unmodifiableMap(this.objectMap);
    }

    /**
     * Import a plain Java object (POJO), along with all other objects reachable from it via copied reference fields.
     *
     * <p>
     * If {@code obj} has already been imported, the previously assigned {@link JObject} is returned.
     *
     * @param obj object to import; must not be null
     * @return imported object, or null if the {@code storageIdMapper} returned null for {@code obj}
     * @throws io.permazen.core.DeletedObjectException if {@code storageIdMapper} returns the object ID of a non-existent object
     * @throws io.permazen.core.TypeNotInSchemaVersionException if {@code storageIdMapper} returns an object ID that does not
     *  corresponding to any Permazen model class
     * @throws IllegalArgumentException if {@code obj} is null
     */
    public JObject importPlain(Object obj) {

        // Sanity check
        Preconditions.checkArgument(obj != null, "null obj");

        // Import object (if not already imported)
        final ObjId id = this.doImportPlain(obj);

        // Recursively copy any fields needing to be copied
        this.recurseOnFields();

        // Done
        return this.jtx.get(id);
    }

    private void recurseOnFields() {
        while (!this.needingFieldsCopied.isEmpty()) {

            // Remove the next object needing its fields copied
            final Iterator<Map.Entry<Object, ObjId>> i = this.needingFieldsCopied.entrySet().iterator();
            final Map.Entry<Object, ObjId> entry = i.next();
            final Object obj = entry.getKey();
            final ObjId id = entry.getValue();
            i.remove();

            // Copy fields
            for (JField jfield : this.jtx.jdb.getJClass(id).jfields.values())
                jfield.importPlain(this, obj, id);
        }
    }

    // Import POJO, returning corresponding JObject or null if object is not supposed to be imported, but don't recurse (yet)
    ObjId doImportPlain(Object obj) {

        // Already imported?
        ObjId id = this.objectMap.get(obj);
        if (id != null || this.objectMap.containsKey(obj))              // null means "don't import this object"
            return id;

        // Get object ID assignment and associated type
        if ((id = this.storageIdMapper.apply(obj)) == null) {
            this.objectMap.put(obj, null);                              // null means "don't import this object"
            return null;
        }

        // Record association with POJO
        this.objectMap.put(obj, id);

        // Mark this object as needing its fields copied
        assert !this.needingFieldsCopied.containsKey(obj);
        this.needingFieldsCopied.put(obj, id);

        // Done
        return id;
    }
}
