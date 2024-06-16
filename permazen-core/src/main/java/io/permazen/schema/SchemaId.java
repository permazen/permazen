
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

import com.google.common.base.Preconditions;

import io.permazen.core.SchemaMismatchException;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A unique identifier that corresponds to the structure and encoding of an individual {@link SchemaItem} component
 * of a {@link SchemaModel}, or an entire {@link SchemaModel}.
 *
 * <p>
 * {@link SchemaId}'s are calculated by taking a secure hash over the structural components of some schema item such as
 * an object type, a field, or an index, or over an entire {@link SchemaModel}.
 *
 * <p>
 * For {@link SchemaItem}s, they provide a simple way to determine whether two schema items have the same structure,
 * meaning they are <i>identified</i> and <i>encoded</i> in the same way. Two {@link SchemaItem} that have the same
 * {@link SchemaId} will always have the same storage ID assigned when they are recorded in a database.
 *
 * <p>
 * For an entire {@link SchemaModel}, the {@link SchemaId} serves the same purpose by encompassing all of the
 * {@link SchemaItem} components of the {@link SchemaModel}. Two {@link SchemaModel}s that have the same {@link SchemaId}
 * will always have the same schema index assigned when they are recorded in a database (that is, they are considered
 * the same schema).
 *
 * <p>
 * The {@link String} form of a {@link SchemaId} looks like {@code SimpleField_12e983a72e72ed56741ddc45e47d3377},
 * where the prefix before the underscore indicates the thing being identified.
 *
 * <p><b>{@link SchemaId}s for {@link SchemaItem}s</b>
 *
 * <p>
 * For {@link SchemaItem}s, the {@link SchemaId} hash function covers its "structure", which is defined as:
 * <ul>
 *  <li>For each {@link SchemaObjectType}, its object type name.
 *  <li>For each {@link ComplexSchemaField}, its field name, complex field type (list, set, or map),
 *      and the structures of its sub-field(s).
 *  <li>For counter fields, its name and field type (i.e., counter).
 *  <li>For simple fields, its name field type (i.e., simple), and {@linkplain SimpleSchemaField#getEncodingId encoding}.
 *  <li>For enum and enum array fields, also the enum's {@linkplain AbstractEnumSchemaField#getIdentifiers identifier list}.
 *  <li>For {@link SchemaCompositeIndex}s, the structure(s) of the indexed field(s).
 * </ul>
 *
 * <p><b>{@link SchemaId}s for {@link SchemaModel}s</b>
 *
 * <p>
 * {@link SchemaId}'s are also used to determine whether two {@link SchemaModel}s are structurally compatible.
 * Two {@link SchemaModel}s with the same {@link SchemaId} will always share the same schema index in a database.
 *
 * <p>
 * The {@link SchemaId} hash of a {@link SchemaModel} includes all of its {@link SchemaItem} components. This hash does
 * <i>not</i> include any explicit storage ID assignments. As a result, attempting to register a {@link SchemaModel}
 * having explicit storage ID assignments that disagree with assignments already recorded in a database will result
 * in a {@link SchemaMismatchException}. For this reason, explicit storage ID assignments are discouraged.
 *
 * <p>
 * Note that {@link SchemaModel#equals equals()} includes explicit storage ID assignments, so in the case that any exist
 * this is a stronger test than just comparing {@link SchemaId}s.
 */
public class SchemaId implements Serializable {

    /**
     * The regular expression that all {@link SchemaId}'s must match.
     */
    public static final String PATTERN;
    static {
        final StringBuilder buf = new StringBuilder();
        for (ItemType itemType : ItemType.values()) {
            buf.append(buf.length() > 0 ? '|' : '(');
            buf.append(Pattern.quote(itemType.getTypeCode()));
        }
        buf.append(")_([0-9a-f]{32})");
        PATTERN = buf.toString();
    }

    /**
     * The number of hash bytes in a {@link SchemaId}.
     */
    public static final int NUM_HASH_BYTES = 16;

    private static final long serialVersionUID = 8837653387481503031L;

    private ItemType itemType;
    private final String id;

// Constructors

    /**
     * Constructor.
     *
     * @param id the schema structure ID in string form
     * @throws IllegalArgumentException if {@code id} is null or invalid
     */
    public SchemaId(String id) {
        Preconditions.checkArgument(id != null, "null id");
        final Matcher matcher = Pattern.compile(PATTERN).matcher(id);
        if (!matcher.matches())
            throw new IllegalArgumentException(String.format("invalid schema ID \"%s\"", id));
        try {
            this.itemType = ItemType.forTypeCode(matcher.group(1));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("invalid schema ID \"%s\": %s", id, e.getMessage()), e);
        }
        this.id = id;
    }

    /**
     * Constructor.
     *
     * @param itemType schema item type
     * @param hash {@link #NUM_HASH_BYTES} hash bytes
     * @throws IllegalArgumentException if either parameter is null
     * @throws IllegalArgumentException if {@code bytes} has the wrong length
     */
    public SchemaId(ItemType itemType, byte[] hash) {
        this(SchemaId.formatHash(itemType, hash));
    }

    private static String formatHash(ItemType itemType, byte[] hash) {
        Preconditions.checkArgument(itemType != null, "null itemType");
        Preconditions.checkArgument(hash != null, "null hash");
        Preconditions.checkArgument(hash.length == NUM_HASH_BYTES, "hash has the wrong length");
        final StringBuilder buf = new StringBuilder();
        buf.append(itemType.getTypeCode()).append('_');
        for (int i = 0; i < NUM_HASH_BYTES; i++)
            buf.append(String.format("%02x", hash[i] & 0xff));
        return buf.toString();
    }

// Public methods

    /**
     * Get the schema ID in string form.
     *
     * @return schema structure ID
     */
    public String getId() {
        return this.id;
    }

    /**
     * Get the {@link ItemType} of the schema item that originally generated this schema ID.
     *
     * @return original schema item type
     * @see SchemaItem#getItemType
     */
    public ItemType getItemType() {
        return this.itemType;
    }

// Object

    @Override
    public String toString() {
        return this.id;
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode() ^ this.id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final SchemaId that = (SchemaId)obj;
        return this.id.equals(that.id);
    }
}
