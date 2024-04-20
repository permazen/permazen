
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.annotation.Values;
import io.permazen.annotation.ValuesList;
import io.permazen.core.CompositeIndex;
import io.permazen.core.Database;
import io.permazen.core.ObjType;
import io.permazen.index.Index2;
import io.permazen.index.Index3;
import io.permazen.schema.SchemaCompositeIndex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A composite index.
 */
public class PermazenCompositeIndex extends PermazenSchemaItem {

    final Class<?> declaringType;
    final List<PermazenSimpleField> pfields;
    final boolean unique;
    final List<List<ValueMatch<?>>> uniqueExcludes;

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
        final int numUniqueExcludes = annotation.uniqueExcludes().length;
        if (numUniqueExcludes > 0) {
            assert this.unique;

            // Parse @ValuesList annotations
            this.uniqueExcludes = new ArrayList<>(numUniqueExcludes);
            final String valuesName = Values.class.getSimpleName();
            final String valuesListName = ValuesList.class.getSimpleName();
            int valuesListIndex = 1;
            for (ValuesList valuesListAnnot : annotation.uniqueExcludes()) {

                // Parse @ValuesList annotation
                final Values[] valuesList = valuesListAnnot.value();
                try {

                    // Verify the correct number of values
                    if (valuesList.length != this.pfields.size()) {
                        throw new IllegalArgumentException(String.format(
                          "annotation contains %d != %d @%s annotations", valuesList.length, this.pfields.size(), valuesName));
                    }

                    // Parse each @Values annotation in the list
                    final ArrayList<ValueMatch<?>> valueMatchList = new ArrayList<>(valuesList.length);
                    for (int i = 0; i < valuesList.length; i++) {
                        final PermazenSimpleField pfield = this.pfields.get(i);
                        final Values values = valuesList[i];

                        // Parse @Values annotation
                        final ValueMatch<?> valueMatch;
                        try {
                            valueMatch = ValueMatch.create(pfield.encoding, values);
                            valueMatch.validate(true);
                        } catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException(String.format(
                              "invalid @%s annotation for field \"%s\": %s", valuesName, pfield.name, e.getMessage()), e);
                        }

                        // Add value matcher
                        valueMatchList.add(valueMatch);
                    }

                    // Verify the overall @ValuesList doesn't always match
                    if (valueMatchList.stream().allMatch(ValueMatch::alwaysMatches))
                        throw new IllegalArgumentException("annotation will always match");

                    // Add value matcher list
                    this.uniqueExcludes.add(valueMatchList);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(String.format(
                      "invalid uniqueExcludes() @%s annotation (#%d): %s", valuesListName, valuesListIndex, e.getMessage()), e);
                }
                valuesListIndex++;
            }
        } else
            this.uniqueExcludes = null;
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
