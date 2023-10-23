
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
     * @param schema schema version
     * @param objType containing object type
     * @param fields indexed fields
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code name} is invalid
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    CompositeIndex(String name, int storageId, Schema schema, ObjType objType, Iterable<? extends SimpleField<?>> fields) {
        super(name, storageId, schema);
        Preconditions.checkArgument(objType != null, "null objType");
        Preconditions.checkArgument(fields != null, "null fields");
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
        return "composite index \"" + this.name + "\" on fields " + this.fields.stream()
          .map(field -> field.name)
          .collect(Collectors.toList());
    }

// Non-public methods

    @Override
    CompositeIndexStorageInfo toStorageInfo() {
        return new CompositeIndexStorageInfo(this);
    }
}

