
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.annotation.PermazenType;
import io.permazen.core.ObjId;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * Context for importing plain (POJO) objects into a {@link PermazenTransaction}.
 *
 * <p>
 * Plain objects (POJO's) can be imported into a {@link PermazenTransaction} to the extent that the POJO class and
 * the corresponding Permazen model class share the same properties. The simplest example of this is when
 * the POJO class is also the Permazen model class (implying a non-abstract class; see also
 * {@link PermazenType#autogenNonAbstract &#64;PermazenType.autogenNonAbstract()}). Also possible are POJO
 * classes and model classes that implement common Java interfaces.
 *
 * <p>
 * The {@link ObjId} for the corresponding imported Permazen object is determined by the configured {@code objectIdMapper}.
 * If {@code objectIdMapper} returns null, the POJO is not imported, and nulls replace any copied references to it; otherwise,
 * the returned object must exist in the transaction. If {@code objectIdMapper} is itself null, the default behavior is
 * to create a new Permazen object using {@link PermazenTransaction#create(Class)}, providing the POJO's class as the model class.
 *
 * <p>
 * Instances ensure that an already-imported POJO will be recognized and not imported twice.
 * Note this determination is based on object identity, not {@link Object#equals Object.equals()}.
 * The {@code objectIdMapper} is invoked at most once for any POJO.
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

    private final PermazenTransaction ptx;
    private final Function<Object, ObjId> objectIdMapper;
    private final IdentityHashMap<Object, ObjId> objectMap = new IdentityHashMap<>();
    private final IdentityHashMap<Object, ObjId> needingFieldsCopied = new IdentityHashMap<>();

    /**
     * Constructor.
     *
     * <p>
     * Uses a default {@code objectIdMapper} that creates new instances for imported objects via
     * {@link PermazenTransaction#create(Class)}, using the Permazen model type found by
     * {@link Permazen#findPermazenClass(Class)} when given the POJO's class.
     *
     * @param ptx the transaction in which to import objects
     * @throws IllegalArgumentException if {@code ptx} is null
     */
    public ImportContext(PermazenTransaction ptx) {
        Preconditions.checkArgument(ptx != null);
        this.ptx = ptx;
        this.objectIdMapper = obj -> {
            final PermazenClass<?> modelClass = this.ptx.pdb.findPermazenClass(obj.getClass());
            if (modelClass == null) {
                throw new IllegalArgumentException(String.format(
                  "no Permazen model class corresponds to POJO %s",  obj.getClass()));
            }
            return ((PermazenObject)this.ptx.create(modelClass)).getObjId();
        };
    }

    /**
     * Constructor.
     *
     * @param ptx the transaction in which to import objects
     * @param objectIdMapper function assigning {@link ObjId}'s to imported objects (or null to skip the corresponding object)
     * @throws IllegalArgumentException if either parameter is null
     */
    public ImportContext(PermazenTransaction ptx, Function<Object, ObjId> objectIdMapper) {
        Preconditions.checkArgument(ptx != null);
        Preconditions.checkArgument(objectIdMapper != null);
        this.ptx = ptx;
        this.objectIdMapper = objectIdMapper;
    }

    /**
     * Get the destination transaction for imported objects.
     *
     * @return associated transaction
     */
    public PermazenTransaction getPermazenTransaction() {
        return this.ptx;
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
     * If {@code obj} has already been imported, the previously assigned {@link PermazenObject} is returned.
     *
     * @param obj object to import; must not be null
     * @return imported object, or null if the {@code objectIdMapper} returned null for {@code obj}
     * @throws io.permazen.core.DeletedObjectException if {@code objectIdMapper} returns the object ID of a non-existent object
     * @throws io.permazen.core.TypeNotInSchemaException if {@code objectIdMapper} returns an object ID that does not
     *  corresponding to any Permazen model class
     * @throws IllegalArgumentException if {@code obj} is null
     */
    public PermazenObject importPlain(Object obj) {

        // Sanity check
        Preconditions.checkArgument(obj != null, "null obj");

        // Import object (if not already imported)
        final ObjId id = this.doImportPlain(obj);

        // Recursively copy any fields needing to be copied
        this.recurseOnFields();

        // Done
        return this.ptx.get(id);
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
            for (PermazenField pfield : this.ptx.pdb.getPermazenClass(id).fieldsByName.values())
                pfield.importPlain(this, obj, id);
        }
    }

    // Import POJO, returning corresponding PermazenObject or null if object is not supposed to be imported, but don't recurse (yet)
    ObjId doImportPlain(Object obj) {

        // Already imported?
        ObjId id = this.objectMap.get(obj);
        if (id != null || this.objectMap.containsKey(obj))              // null means "don't import this object"
            return id;

        // Get object ID assignment and associated type
        if ((id = this.objectIdMapper.apply(obj)) == null) {
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
