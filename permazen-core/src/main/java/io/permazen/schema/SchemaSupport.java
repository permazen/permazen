
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import com.google.common.base.Preconditions;

import io.permazen.core.InvalidSchemaException;
import io.permazen.util.AbstractXMLStreaming;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * Base class for schema items providing clone and lockdown support.
 */
abstract class SchemaSupport extends AbstractXMLStreaming implements Cloneable {

    boolean lockedDown1;    // locked down for everything except storage ID's
    boolean lockedDown2;    // lockedDown1 + storage ID's

    // Cached info (after lockdown1 only)
    private SchemaId schemaId;

// Recursion

    /**
     * Visit all {@link SchemaItem} descendents of this instance with the given visitor.
     *
     * <p>
     * If this instance is also a {@link SchemaItem}, then also visit this instance.
     *
     * <p>
     * The traversal is depth first, pre-order.
     *
     * @param visitor visitor for schema items
     * @throws IllegalArgumentException if {@code visitor} is null
     */
    public void visitSchemaItems(Consumer<? super SchemaItem> visitor) {
        Preconditions.checkArgument(visitor != null, "null visitor");
        if (this instanceof SchemaItem)
            visitor.accept((SchemaItem)this);
    }

    /**
     * Visit this schema item and all of its descendents matching the given type with the given visitor.
     *
     * <p>
     * The traversal is depth first, post-order.
     *
     * @param nodeType node type to include
     * @param visitor visitor for schema items
     * @throws IllegalArgumentException if either parameter is null
     */
    public final <T extends SchemaItem> void visitSchemaItems(Class<T> nodeType, Consumer<? super T> visitor) {
        Preconditions.checkArgument(nodeType != null, "null nodeType");
        this.visitSchemaItems(item -> {
            if (nodeType.isInstance(item))
                visitor.accept(nodeType.cast(item));
        });
    }

// Lockdown

    /**
     * Determine whether this instance is locked down.
     *
     * @param includingStorageIds false to test all but storage ID's, true to require storage ID's to be locked down as well
     * @return true if instance is locked down, otherwise false
     */
    public final boolean isLockedDown(boolean includingStorageIds) {
        return includingStorageIds ? this.lockedDown2 : this.lockedDown1;
    }

    final void verifyNotLockedDown(boolean storageIds) {
        if (this.lockedDown2 || (!storageIds && this.lockedDown1))
            throw new UnsupportedOperationException("instance is locked down");
    }

    <T extends SchemaItem> NavigableMap<String, T> lockDownMap1(NavigableMap<String, T> map) {
        map.values().forEach(SchemaItem::lockDown1);
        return Collections.unmodifiableNavigableMap(map);
    }

    void lockDown1() {
        this.lockedDown1 = true;
    }

    void lockDown2() {
        this.lockedDown2 = true;
    }

// Structural Compatibility

    /**
     * Generate a unique {@link SchemaId} corresponding to the type and encoding structure of this schema item.
     *
     * <p>
     * The {@link SchemaId} does <i>not</i> depend on the storage ID.
     *
     * <p>
     * This instance must be {@linkplain #isLockedDown locked down} except for storage ID's.
     * Repeated invocations of this method will be very fast, just returning the cached previous result.
     *
     * @return schema ID
     * @see SchemaId
     * @throws IllegalStateException if this instance is not locked down
     */
    public final SchemaId getSchemaId() {
        if (this.schemaId == null) {
            Preconditions.checkState(this.lockedDown1, "not locked down");
            this.schemaId = this.computeSchemaId();
        }
        return this.schemaId;
    }

    private SchemaId computeSchemaId() {
        final MessageDigest sha256;
        try {
            sha256 = MessageDigest.getInstance("SHA256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        final OutputStream discardOutputStream = new OutputStream() {
            @Override
            public void write(int b) {
            }
            @Override
            public void write(byte[] b, int off, int len) {
            }
        };
        try (DataOutputStream output = new DataOutputStream(new DigestOutputStream(discardOutputStream, sha256))) {
            this.writeSchemaIdHashData(output);
        } catch (IOException e) {
            throw new RuntimeException("unexpected exception", e);
        }
        final byte[] hash = sha256.digest();
        final byte[] bytes = new byte[SchemaId.NUM_HASH_BYTES];
        for (int i = 0; i < hash.length; i++)
            bytes[i % SchemaId.NUM_HASH_BYTES] ^= hash[i];
        return new SchemaId(this.getItemType(), bytes);
    }

    void writeSchemaIdHashData(DataOutputStream output) throws IOException {
        output.writeUTF(this.getItemType().getTypeCode());
    }

    /**
     * Get the {@link ItemType} corresponding to this instance.
     *
     * @return schema item type
     */
    public abstract ItemType getItemType();

// Validation

    void verifyMappedNames(String desc, Map<String, ? extends SchemaItem> map) {
        for (Map.Entry<String, ? extends SchemaItem> entry : map.entrySet()) {
            final String name = entry.getKey();
            final SchemaItem item = entry.getValue();
            if (!item.getName().equals(name)) {
                throw new InvalidSchemaException(String.format(
                  "%s \"%s\" mapped under the wrong name \"%s\"", desc, item.getName(), name));
            }
        }
    }

    <T extends SchemaItem> void verifyBackReferences(String desc, Map<String, T> map, Function<T, Object> refGetter) {
        for (T item : map.values()) {
            if (refGetter.apply(item) != this)
                throw new InvalidSchemaException(String.format("wrong %s reference in %s", desc, item));
        }
    }

// XML

    void writeSchemaIdComment(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeComment(String.format(" \"%s\" ", this.lockedDown1 ? this.getSchemaId() : this.computeSchemaId()));
    }

// Cloneable

    /**
     * Deep-clone this instance.
     *
     * <p>
     * The returned instance will <b>not</b> be {@linkplain #isLockedDown locked down} even if this one is.
     */
    @Override
    protected SchemaSupport clone() {
        final SchemaSupport clone;
        try {
            clone = (SchemaSupport)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        clone.schemaId = null;
        clone.lockedDown1 = false;
        clone.lockedDown2 = false;
        return clone;
    }

    @SuppressWarnings("unchecked")
    <T extends SchemaItem> NavigableMap<String, T> cloneMap(NavigableMap<String, T> map) {
        final TreeMap<String, T> mapClone = new TreeMap<>(map);
        for (Map.Entry<String, T> entry : mapClone.entrySet())
            entry.setValue((T)entry.getValue().clone());
        return mapClone;
    }
}
