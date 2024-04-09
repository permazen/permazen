
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.core.SchemaItem;
import io.permazen.schema.SchemaId;

import java.util.function.Consumer;

/**
 * Superclass for the {@link PermazenClass}, {@link PermazenField}, and {@link PermazenCompositeIndex} classes
 * which describe the schema associated with a {@link Permazen} instance.
 */
public abstract class PermazenSchemaItem {

    final String name;
    final String description;

    Object schemaItem;      // at first a io.permazen.schema.SchemaItem, then later a io.permazen.core.SchemaItem
    int storageId;          // at first might be zero, later updated to actual value

// Constructor

    PermazenSchemaItem(String name, int storageId, String description) {
        Preconditions.checkArgument(name != null, "null name");
        Preconditions.checkArgument(storageId >= 0, "invalid storageId");
        Preconditions.checkArgument(description != null, "null description");
        this.name = name;
        this.storageId = storageId;
        this.description = description;
    }

// Public Methods

    /**
     * Get the name of this instance.
     *
     * @return this instance's name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the storage ID of this instance.
     *
     * @return this instance's actual storage ID
     */
    public int getStorageId() {
        return this.storageId;
    }

    /**
     * Get the schema ID of this instance.
     *
     * @return this instance's schema ID
     */
    public SchemaId getSchemaId() {
        return this.getSchemaItem().getSchemaId();
    }

    /**
     * Get the corresonding core API database instance.
     *
     * @return this instance's correpsonding {@link SchemaItem}
     */
    public SchemaItem getSchemaItem() {
        return (SchemaItem)this.schemaItem;
    }

// Object

    @Override
    public String toString() {
        return this.description;
    }

// Package Methods

    io.permazen.schema.SchemaItem toSchemaItem() {
        io.permazen.schema.SchemaItem item = this.createSchemaItem();
        item.setName(this.name);
        item.setStorageId(this.storageId);
        this.schemaItem = item;
        return item;
    }

    abstract io.permazen.schema.SchemaItem createSchemaItem();

    void visitSchemaItems(Consumer<? super PermazenSchemaItem> visitor) {
        Preconditions.checkArgument(visitor != null, "null visitor");
        visitor.accept(this);
    }

    final <T extends PermazenSchemaItem> void visitSchemaItems(Class<T> nodeType, Consumer<? super T> visitor) {
        Preconditions.checkArgument(nodeType != null, "null nodeType");
        this.visitSchemaItems(item -> {
            if (nodeType.isInstance(item))
                visitor.accept(nodeType.cast(item));
        });
    }
}
