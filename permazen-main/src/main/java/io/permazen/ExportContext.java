
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.annotation.PermazenType;
import io.permazen.core.ObjId;
import io.permazen.core.util.ObjIdMap;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * Context for exporting plain (POJO) objects from a {@link PermazenTransaction}.
 *
 * <p>
 * Plain objects (POJO's) can be exported from a {@link PermazenTransaction} to the extent that the Permazen model class and
 * the corresponding target POJO class share the same properties. The simplest example of this is when the Permazen model class
 * is also the POJO class (implying a non-abstract class; see also
 * {@link PermazenType#autogenNonAbstract &#64;PermazenType.autogenNonAbstract()}). Also possible are POJO
 * classes and model classes that implement common Java interfaces.
 *
 * <p>
 * The POJO corresponding to an exported database object is supplied by the configured {@code objectMapper}.
 * If {@code objectMapper} returns null, the database object is not exported, and nulls replace any copied references to it.
 * If {@code objectMapper} is null, the default behavior is to create a new POJO using the model class' default constructor,
 * which of course implies the model class cannot be abstract.
 *
 * <p>
 * Instances ensure that an already-exported database object will be recognized and not exported twice.
 * The {@code objectMapper} is invoked at most once for any object ID.
 *
 * <p>
 * When a database objext is exported, its fields are copied to the POJO. Fields for which no corresponding
 * POJO property exists are omitted.
 *
 * <p>
 * Reference fields are traversed and the referenced objects are automatically exported as POJO's, recursively.
 * In other words, the entire transitive closure of objects reachable from an exported object is exported.
 * Cycles in the graph of references are handled properly.
 *
 * <p><b>Conversion Details</b></p>
 *
 * {@link Counter} fields export to any {@link Number} property. Collection fields export to an existing collection,
 * so that a setter method is not required; however, if the getter returns null, a setter is required and the export
 * will attempt to use an appropriate collection class ({@link java.util.HashSet} for property of type {@link java.util.Set},
 * {@link java.util.TreeSet} for a property of type {@link java.util.SortedSet}, etc). To avoid potential mismatch with
 * collection types, initialize collection properties.
 *
 * @see ImportContext
 */
public class ExportContext {

    private final PermazenTransaction ptx;
    private final Function<ObjId, Object> objectMapper;
    private final ObjIdMap<Object> pobjectMap = new ObjIdMap<>();
    private final ObjIdMap<Object> needingFieldsCopied = new ObjIdMap<>();

    /**
     * Constructor.
     *
     * <p>
     * Uses a default {@code objectMapper} that creates new exported objects using the default constructor of the model class.
     *
     * @param ptx the transaction from which to export objects
     * @throws IllegalArgumentException if {@code ptx} is null
     */
    public ExportContext(PermazenTransaction ptx) {
        Preconditions.checkArgument(ptx != null);
        this.ptx = ptx;
        this.objectMapper = id -> {
            final Class<?> type = this.ptx.pdb.getPermazenClass(id).getType();
            try {
                return type.getConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("can't instatiate " + type + " using default constructor for POJO export", e);
            }
        };
    }

    /**
     * Constructor.
     *
     * @param ptx the transaction from which to export objects
     * @param objectMapper function returning the POJO used to export a database object (or null to skip the corresponding object)
     * @throws IllegalArgumentException if either parameter is null
     */
    public ExportContext(PermazenTransaction ptx, Function<ObjId, Object> objectMapper) {
        Preconditions.checkArgument(ptx != null, "null ptx");
        Preconditions.checkArgument(objectMapper != null, "null objectMapper");
        this.ptx = ptx;
        this.objectMapper = objectMapper;
    }

    /**
     * Get the transaction from which objects are exported.
     *
     * @return associated transaction
     */
    public PermazenTransaction getPermazenTransaction() {
        return this.ptx;
    }

    /**
     * Get the mapping from already exported database object to the corresponding POJO.
     *
     * @return mapping from exported database object ID to corresponding POJO
     */
    public Map<ObjId, Object> getJObjectMap() {
        return Collections.unmodifiableMap(this.pobjectMap);
    }

    /**
     * Export a {@link PermazenObject} as a plain Java object, along with all other objects reachable from it via
     * copied reference fields.
     *
     * <p>
     * Equivalent to {@link #exportPlain(ObjId) exportPlain}{@code (pobj.getObjId())}.
     *
     * @param pobj object to export; must not be null
     * @return exported object, or null if the {@code objectMapper} returned null for {@code pobj.getObjId()}
     * @throws io.permazen.core.DeletedObjectException if {@code id} refers to an object that does not exist
     *  in the transaction associated with this instance
     * @throws io.permazen.core.TypeNotInSchemaException if {@code pobj} is an {@link UntypedPermazenObject}
     * @throws IllegalArgumentException if {@code pobj} is null
     */
    public Object exportPlain(PermazenObject pobj) {
        Preconditions.checkArgument(pobj != null, "null pobj");
        return this.exportPlain(pobj.getObjId());
    }

    /**
     * Export the {@link PermazenObject} with the given {@link ObjId} as a plain Java object, along with all other objects
     * reachable from it via copied reference fields.
     *
     * <p>
     * If the {@link PermazenObject} has already been exported, the previously returned {@link Object} is returned.
     *
     * @param id object ID of the object to export; must not be null
     * @return exported object, or null if the {@code objectMapper} returned null for {@code id}
     * @throws io.permazen.core.DeletedObjectException if {@code id} refers to an object that does not exist
     *  in the transaction associated with this instance
     * @throws io.permazen.core.TypeNotInSchemaException if {@code id} refers to a type that does not exist
     *  in this instance's transaction's schema
     * @throws IllegalArgumentException if {@code id} is null
     */
    public Object exportPlain(ObjId id) {

        // Sanity check
        Preconditions.checkArgument(id != null, "null id");

        // Export object (if not already imported)
        final Object obj = this.doExportPlain(id);

        // Recursively copy any fields needing to be copied
        this.recurseOnFields();

        // Done
        return obj;
    }

    private void recurseOnFields() {
        while (!this.needingFieldsCopied.isEmpty()) {

            // Remove the next object needing its fields copied
            final Iterator<Map.Entry<ObjId, Object>> i = this.needingFieldsCopied.entrySet().iterator();
            final Map.Entry<ObjId, Object> entry = i.next();
            final ObjId id = entry.getKey();
            final Object obj = entry.getValue();
            i.remove();

            // Copy fields
            for (PermazenField pfield : this.ptx.pdb.getPermazenClass(id).fieldsByName.values())
                pfield.exportPlain(this, id, obj);
        }
    }

    // Export POJO, returning corresponding Object or null if object is not supposed to be imported, but don't recurse (yet)
    Object doExportPlain(ObjId id) {

        // Already exported?
        Object obj = this.pobjectMap.get(id);
        if (obj != null || this.pobjectMap.containsKey(id))             // null means "don't import this object"
            return obj;

        // Get POJO
        if ((obj = this.objectMapper.apply(id)) == null) {
            this.pobjectMap.put(id, null);                              // null means "don't import this object"
            return null;
        }

        // Record ID association with POJO
        this.pobjectMap.put(id, obj);

        // Mark this object as needing its fields copied
        assert !this.needingFieldsCopied.containsKey(id);
        this.needingFieldsCopied.put(id, obj);

        // Done
        return obj;
    }
}
