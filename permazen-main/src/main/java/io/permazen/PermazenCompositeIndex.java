
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.core.CompositeIndex;
import io.permazen.core.Database;
import io.permazen.core.ObjType;
import io.permazen.encoding.Encoding;
import io.permazen.index.Index2;
import io.permazen.index.Index3;
import io.permazen.schema.SchemaCompositeIndex;
import io.permazen.util.ParseContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A composite index.
 */
public class PermazenCompositeIndex extends PermazenSchemaItem {

    final Class<?> declaringType;
    final List<PermazenSimpleField> pfields;
    final boolean unique;
    final List<List<Object>> uniqueExcludes;    // note: these are core API values, sorted lexicographically by pfield.encoding
    final Comparator<List<Object>> uniqueComparator;

    PermazenCompositeIndex(String name, int storageId, Class<?> declaringType,
      io.permazen.annotation.PermazenCompositeIndex annotation, PermazenSimpleField... pfields) {
        super(name, storageId, String.format("composite index \"%s\" on fields %s",
          name, Stream.of(pfields).map(field -> "\"" + field.getName() + "\"").collect(Collectors.joining(", "))));
        Preconditions.checkArgument(name != null, "null name");
        Preconditions.checkArgument(declaringType != null, "null declaringType");
        Preconditions.checkArgument(pfields.length >= 2 && pfields.length <= Database.MAX_INDEXED_FIELDS, "invalid field count");
        Preconditions.checkArgument(annotation != null, "null annotation");
        this.declaringType = declaringType;
        this.pfields = Collections.unmodifiableList(Arrays.asList(pfields));
        this.unique = annotation.unique();

        // Parse uniqueExcludes
        final int numExcludes = annotation.uniqueExclude().length;
        if (numExcludes > 0) {
            assert this.unique;

            // Parse value lists
            this.uniqueExcludes = new ArrayList<>(numExcludes);
            final int numFields = this.pfields.size();
            for (String string : annotation.uniqueExclude()) {
                final ParseContext ctx = new ParseContext(string);
                final ArrayList<Object> values = new ArrayList<>(numFields);
                for (PermazenSimpleField pfield : this.pfields) {
                    try {
                        values.add(pfield.encoding.fromParseableString(ctx));
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException(String.format(
                          "invalid uniqueExclude() value \"%s\" for %s: %s", string, pfield, e.getMessage()), e);
                    }
                    if (values.size() < numFields) {
                        ctx.skipWhitespace();
                        ctx.expect(',');
                        ctx.skipWhitespace();
                    }
                }
                this.uniqueExcludes.add(values);
            }

            // Build value list comparator
            Comparator<List<Object>> comparator = null;
            for (int i = 0; i < this.pfields.size(); i++)
                comparator = this.addFieldComparator(comparator, i, this.pfields.get(i).encoding);
            this.uniqueComparator = comparator;

            // Sort excluded values
            Collections.sort(this.uniqueExcludes, this.uniqueComparator);
        } else {
            this.uniqueExcludes = null;
            this.uniqueComparator = null;
        }
    }

    // This method exists solely to bind the generic type parameters
    private <T> Comparator<List<Object>> addFieldComparator(Comparator<List<Object>> comparator, int i, Encoding<T> encoding) {
        assert (comparator == null) == (i == 0);
        final Function<List<Object>, T> valueExtractor = list -> encoding.validate(list.get(i));
        return comparator != null ?
          comparator.thenComparing(valueExtractor, encoding) : Comparator.comparing(valueExtractor, encoding);
    }

// Public API

    /**
     * Get the {@link PermazenSimpleField}s on which this index is based.
     *
     * @return this index's fields, in indexed order
     */
    public List<PermazenSimpleField> getFields() {
        return this.pfields;
    }

    /**
     * View this index.
     *
     * <p>
     * The returned index will have time {@link Index2}, {@link Index3}, etc., depending on the number of fields indexed.
     *
     * @param ptx transaction
     * @return view of this index in {@code ptx}
     * @throws StaleTransactionException if {@code ptx} is no longer usable
     * @throws IllegalArgumentException if {@code ptx} is null
     */
    public Object getIndex(PermazenTransaction ptx) {
        Preconditions.checkArgument(ptx != null, "null ptx");
        return ptx.queryIndex(this.storageId);
    }

    @Override
    public CompositeIndex getSchemaItem() {
        return (CompositeIndex)super.getSchemaItem();
    }

// Package methods

    void replaceSchemaItems(ObjType objType) {
        this.schemaItem = objType.getCompositeIndex(this.name);
    }

    @Override
    SchemaCompositeIndex toSchemaItem() {
        final SchemaCompositeIndex schemaIndex = (SchemaCompositeIndex)super.toSchemaItem();
        this.pfields.forEach(pfield -> schemaIndex.getIndexedFields().add(pfield.name));
        return schemaIndex;
    }

    @Override
    SchemaCompositeIndex createSchemaItem() {
        return new SchemaCompositeIndex();
    }

    Class<?>[] getQueryInfoValueTypes() {
        final Class<?>[] rawTypes = new Class<?>[this.pfields.size()];
        for (int i = 0; i < rawTypes.length; i++)
            rawTypes[i] = this.pfields.get(i).typeToken.wrap().getRawType();
        return rawTypes;
    }
}
