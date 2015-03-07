
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

/**
 * Represents a composite index on an {@link ObjType}.
 */
public class CompositeIndex extends SchemaItem {

    final ObjType objType;
    final List<SimpleField<?>> fields;

    /**
     * Constructor.
     *
     * @param name the name of the index
     * @param storageId index storage ID
     * @param version schema version
     * @param objType containing object type
     * @param fields indexed fields
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    CompositeIndex(String name, int storageId, SchemaVersion version, ObjType objType, Iterable<? extends SimpleField<?>> fields) {
        super(name, storageId, version);
        if (objType == null)
            throw new IllegalArgumentException("null objType");
        if (fields == null)
            throw new IllegalArgumentException("null fields");
        this.objType = objType;
        this.fields = Collections.unmodifiableList(Lists.newArrayList(fields));
    }

// Public methods

    /**
     * Get the containing object type.
     *
     * @return indexed object type
     */
    public ObjType getObjType() {
        return this.objType;
    }

    /**
     * Get the indexed fields.
     *
     * @return list of indexed fields, always having length two or greater
     */
    public List<SimpleField<?>> getFields() {
        return this.fields;
    }

    @Override
    public String toString() {
        return "composite index `" + this.name + "' on fields " + Lists.transform(this.fields,
          new Function<SimpleField<?>, String>() {
            @Override
            public String apply(SimpleField<?> field) {
                return field.name;
            }
        });
    }

// Non-public methods

    @Override
    CompositeIndexStorageInfo toStorageInfo() {
        return new CompositeIndexStorageInfo(this);
    }
}

