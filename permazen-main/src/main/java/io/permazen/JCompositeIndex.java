
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.core.Database;
import io.permazen.core.encoding.Encoding;
import io.permazen.schema.SchemaCompositeIndex;
import io.permazen.util.ParseContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/**
 * A composite index.
 */
public class JCompositeIndex extends JSchemaObject {

    final Class<?> declaringType;
    final List<JSimpleField> jfields;
    final boolean unique;
    final List<List<Object>> uniqueExcludes;    // note: these are core API values, sorted lexicographically by jfield.encoding
    final Comparator<List<Object>> uniqueComparator;

    /**
     * Constructor.
     *
     * @param jdb associated database
     * @param name the name of the object type
     * @param storageId object type storage ID
     * @param declaringType object type annotation is declared on (for scoping unique())
     * @param annotation original annotation
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code storageId} is non-positive
     */
    JCompositeIndex(Permazen jdb, String name, int storageId, Class<?> declaringType,
      io.permazen.annotation.JCompositeIndex annotation, JSimpleField... jfields) {
        super(jdb, name, storageId, "composite index \"" + name + "\" on fields " + Arrays.asList(jfields));
        Preconditions.checkArgument(name != null, "null name");
        Preconditions.checkArgument(declaringType != null, "null declaringType");
        Preconditions.checkArgument(jfields.length >= 2 && jfields.length <= Database.MAX_INDEXED_FIELDS, "invalid field count");
        Preconditions.checkArgument(annotation != null, "null annotation");
        this.declaringType = declaringType;
        this.jfields = Collections.unmodifiableList(Arrays.asList(jfields));
        this.unique = annotation.unique();

        // Parse uniqueExcludes
        final int numExcludes = annotation.uniqueExclude().length;
        if (numExcludes > 0) {
            assert this.unique;

            // Parse value lists
            this.uniqueExcludes = new ArrayList<>(numExcludes);
            final int numFields = this.jfields.size();
            for (String string : annotation.uniqueExclude()) {
                final ParseContext ctx = new ParseContext(string);
                final ArrayList<Object> values = new ArrayList<>(numFields);
                for (JSimpleField jfield : this.jfields) {
                    try {
                        values.add(jfield.encoding.fromParseableString(ctx));
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException(
                          String.format("invalid uniqueExclude() value \"%s\": %s", string, e.getMessage()), e);
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
            for (int i = 0; i < this.jfields.size(); i++)
                comparator = this.addFieldComparator(comparator, i, this.jfields.get(i).encoding);
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
     * Get the {@link JSimpleField}s on which this index is based.
     *
     * @return this index's fields, in indexed order
     */
    public List<JSimpleField> getJFields() {
        return this.jfields;
    }

// Package methods

    @Override
    IndexInfo toIndexInfo() {
        return new CompositeIndexInfo(this);
    }

    @Override
    SchemaCompositeIndex toSchemaItem(Permazen jdb) {
        final SchemaCompositeIndex schemaIndex = new SchemaCompositeIndex();
        this.initialize(jdb, schemaIndex);
        return schemaIndex;
    }

    void initialize(Permazen jdb, SchemaCompositeIndex schemaIndex) {
        super.initialize(jdb, schemaIndex);
        for (JSimpleField jfield : this.jfields)
            schemaIndex.getIndexedFields().add(jfield.getStorageId());
    }

    Class<?>[] getQueryInfoValueTypes() {
        final Class<?>[] rawTypes = new Class<?>[this.jfields.size()];
        for (int i = 0; i < rawTypes.length; i++)
            rawTypes[i] = this.jfields.get(i).typeToken.wrap().getRawType();
        return rawTypes;
    }
}
