
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.collect.Lists;

import io.permazen.encoding.Encoding;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a simple or composite index on an {@link ObjType}.
 */
public abstract class Index extends SchemaItem {

    final ObjType objType;
    final List<SimpleField<?>> fields;
    final List<Encoding<?>> encodings;

// Constructor

    Index(Schema schema, io.permazen.schema.SchemaItem index,
      String name, ObjType objType, Iterable<? extends SimpleField<?>> fields) {
        super(schema, index, name);

        // Initialize
        this.objType = objType;
        this.fields = Collections.unmodifiableList(Lists.newArrayList(fields));
        assert this.fields.size() >= 1 && this.fields.size() <= Database.MAX_INDEXED_FIELDS;

        // Gather encodings
        final ArrayList<Encoding<?>> encodingList = new ArrayList<>(this.fields.size());
        for (int i = 0; i < this.fields.size(); i++)
            encodingList.add(this.fields.get(i).getEncoding());
        this.encodings = Collections.unmodifiableList(encodingList);
    }

// Public methods

    /**
     * Get the object type that contains the field(s) in this index.
     *
     * @return indexed fields' object type
     */
    public ObjType getObjType() {
        return this.objType;
    }

    /**
     * Get the indexed field(s).
     *
     * @return list of indexed fields
     */
    public List<SimpleField<?>> getFields() {
        return this.fields;
    }

    /**
     * Determine whether this is a composite index, i.e., and index on two or more fields.
     *
     * @return true if composite
     */
    public boolean isComposite() {
        return this.fields.size() > 1;
    }

    /**
     * Get this index's view of the given transaction.
     *
     * @param tx transaction
     * @return view of this index in {@code tx}
     * @throws IllegalArgumentException if {@code tx} is null
     */
    public abstract AbstractCoreIndex<ObjId> getIndex(Transaction tx);

// IndexSwitch

    /**
     * Apply visitor pattern.
     *
     * @param target target to invoke
     * @param <R> visitor return type
     * @return return value from the method of {@code target} corresponding to this instance's type
     * @throws NullPointerException if {@code target} is null
     */
    public abstract <R> R visit(IndexSwitch<R> target);

// Object

    @Override
    public String toString() {
        return String.format("index \"%s\" on field%s %s",
          this.name, this.fields.size() != 1 ? "s" : "",
          this.fields.stream().map(this::getFieldDisplayName).collect(Collectors.joining(", ")));
    }

// Package Methods

    String getFieldDisplayName(SimpleField<?> field) {
        return field.name;
    }
}
