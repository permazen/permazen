
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;

import java.util.ArrayList;
import java.util.List;

import io.permazen.core.FieldType;

/**
 * Represents a composite index.
 *
 * <p>
 * An instance of this class exists for each storage ID for which at least one {@link JCompositeIndex} exists.
 */
class CompositeIndexInfo extends IndexInfo {

    private final List<Integer> storageIds;
    private final List<FieldType<?>> fieldTypes;
    private final List<Class<? extends Enum<?>>> enumTypes;
    private final List<ConverterProvider> converterProviders;

    CompositeIndexInfo(JCompositeIndex index) {
        super(index.storageId);
        this.storageIds = new ArrayList<>(index.jfields.size());
        this.fieldTypes = new ArrayList<>(index.jfields.size());
        this.enumTypes = new ArrayList<>(index.jfields.size());
        this.converterProviders = new ArrayList<>(index.jfields.size());
        for (JSimpleField jfield : index.jfields) {
            this.storageIds.add(jfield.storageId);
            this.fieldTypes.add(jfield.fieldType);
            this.converterProviders.add(ConverterProvider.identityForNull(jfield::getConverter));
        }
    }

    /**
     * Get the indexed field storage ID's.
     *
     * @return indexed field storage ID's
     */
    public List<Integer> getStorageIds() {
        return this.storageIds;
    }

    /**
     * Get the indexed field types.
     *
     * @return indexed field types
     */
    public List<FieldType<?>> getFieldTypes() {
        return this.fieldTypes;
    }

    /**
     * Get a {@link Converter} that converts the specified indexed value type from what the core database returns
     * to what the Java application expects.
     *
     * @param jtx transaction
     * @param valueIndex value index
     * @return {@link Converter} from core API to Java, never null
     */
    public Converter<?, ?> getConverter(JTransaction jtx, int valueIndex) {
        return this.converterProviders.get(valueIndex).getConverter(jtx);
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[storageId=" + this.getStorageId()
          + ",storageIds=" + this.getStorageIds()
          + ",fieldTypes=" + this.fieldTypes
          + ",enumTypes=" + this.enumTypes
          + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final CompositeIndexInfo that = (CompositeIndexInfo)obj;
        return this.storageIds.equals(that.storageIds)
          && this.fieldTypes.equals(that.fieldTypes)
          && this.enumTypes.equals(that.enumTypes);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.storageIds.hashCode() ^ this.fieldTypes.hashCode() ^ this.enumTypes.hashCode();
    }
}

