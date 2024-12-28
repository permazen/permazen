
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.permazen.encoding.Encoding;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVTransaction;
import io.permazen.util.ByteData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A simple or composite index on some field(s) an {@link ObjType}.
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

        // Gather encodings, with special handling for reference encodings
        final ArrayList<Encoding<?>> encodingList = new ArrayList<>(this.fields.size());
        for (int i = 0; i < this.fields.size(); i++)
            encodingList.add(Index.genericize(this.fields.get(i).getEncoding()));
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
     * Get the field encoding(s).
     *
     * <p>
     * Note that the core API treats reference fields with the same name as the same field, regardless of their
     * object type restrictions. Therefore, the encodings in an index corresponding to reference fields do not
     * have type restrictions and therefore can be different from the encodings associated with the fields themselves.
     *
     * @return list of indexed fields
     */
    public List<Encoding<?>> getEncodings() {
        return this.encodings;
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

    /**
     * Get the key in the underlying key/value store corresponding to the given value tuple in this index.
     *
     * <p>
     * The returned key will be the prefix of all index entries with the given value tuple over all objects.
     *
     * @param values indexed values
     * @return the corresponding {@link KVDatabase} key
     * @throws IllegalArgumentException if {@code values} is null
     * @throws IllegalArgumentException if {@code values} has the wrong length for this index
     * @throws IllegalArgumentException if any value in {@code values} has the wrong type
     * @see KVTransaction#watchKey KVTransaction.watchKey()
     */
    public ByteData getKey(Object... values) {
        Preconditions.checkArgument(values != null, "null values");
        Preconditions.checkArgument(values.length == this.encodings.size(), "wrong number of values");
        final ByteData.Writer writer = ByteData.newWriter();
        Encodings.UNSIGNED_INT.write(writer, this.storageId);
        int i = 0;
        for (Encoding<?> encoding : this.encodings)
            encoding.validateAndWrite(writer, values[i++]);
        return writer.toByteData();
    }

    /**
     * Get the key in the underlying key/value store corresponding to the given value tuple and target object
     * in this index.
     *
     * @param id target object ID
     * @param values indexed values
     * @return the corresponding {@link KVDatabase} key
     * @throws IllegalArgumentException if {@code values} is null
     * @throws IllegalArgumentException if {@code values} has the wrong length for this index
     * @throws IllegalArgumentException if any value in {@code values} has the wrong type
     * @see KVTransaction#watchKey KVTransaction.watchKey()
     */
    public ByteData getKey(ObjId id, Object... values) {
        Preconditions.checkArgument(id != null, "null id");
        final ByteData.Writer writer = ByteData.newWriter();
        writer.write(this.getKey(values));
        id.writeTo(writer);
        return writer.toByteData();
    }

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
          this.fields.stream().map(SimpleField::getFullName).collect(Collectors.joining(", ")));
    }

// Package Methods

    /**
     * Genericize the given encoding for use in a index.
     *
     * <p>
     * For encodings other than {@link ReferenceEncoding}, this just returns the given encoding unchanged.
     * For {@link ReferenceEncoding}, this returns the encoding with all type restrictions removed.
     * This is required because the type restrictions associated with reference fields are allowed to vary
     * across schemas without changing the identity of the field. When querying indexes, reference fields
     * are instead restricted according to the types specified in the query.
     *
     * @param encoding simple field encoding
     * @return {@code encoding} genericized for use in an index, or {@code encoding} itself if no change is needed
     */
    @SuppressWarnings("unchecked")
    public static <T> Encoding<T> genericize(Encoding<T> encoding) {
        if (encoding instanceof ReferenceEncoding)
            encoding = (Encoding<T>)((ReferenceEncoding)encoding).genericizeForIndex();
        return encoding;
    }
}
