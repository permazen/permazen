
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.jsimpledb.core.Database;
import org.jsimpledb.schema.SchemaCompositeIndex;

/**
 * A composite index.
 */
public class JCompositeIndex extends JSchemaObject {

    final List<JSimpleField> jfields;

    /**
     * Constructor.
     *
     * @param name the name of the object type
     * @param storageId object type storage ID
     * @param type object type Java model class
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    JCompositeIndex(String name, int storageId, JSimpleField... jfields) {
        super(name, storageId, "composite index `" + name + "' on fields " + Arrays.asList(jfields));
        if (name == null)
            throw new IllegalArgumentException("null name");
        if (jfields.length < 2 || jfields.length > Database.MAX_INDEXED_FIELDS)
            throw new IllegalArgumentException("invalid number of fields");
        this.jfields = Collections.unmodifiableList(Arrays.asList(jfields));
    }

// Public API

    /**
     * Get the {@link JSimpleField}s on which this index is based.
     */
    public List<JSimpleField> getJFields() {
        return this.jfields;
    }

// Package methods

    /**
     * Create a {@link JCompositeIndexInfo} instance that corresponds to this instance.
     */
    JCompositeIndexInfo toJCompositeIndexInfo() {
        return new JCompositeIndexInfo(this);
    }

    @Override
    SchemaCompositeIndex toSchemaItem(JSimpleDB jdb) {
        final SchemaCompositeIndex schemaIndex = new SchemaCompositeIndex();
        this.initialize(jdb, schemaIndex);
        return schemaIndex;
    }

    void initialize(JSimpleDB jdb, SchemaCompositeIndex schemaIndex) {
        super.initialize(jdb, schemaIndex);
        for (JSimpleField jfield : this.jfields)
            schemaIndex.getIndexedFields().add(jfield.getStorageId());
    }
}

