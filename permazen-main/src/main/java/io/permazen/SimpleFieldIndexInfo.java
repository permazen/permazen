
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;

import io.permazen.core.FieldType;

import java.util.Objects;

/**
 * Support superclass for simple indexes on simple fields.
 *
 * <p>
 * An instance of this class exists for each simple field storage ID for which at least one {@link JSimpleField} is indexed.
 */
abstract class SimpleFieldIndexInfo extends IndexInfo implements ConverterProvider {

    private final FieldType<?> fieldType;
    private final Class<? extends Enum<?>> enumType;                            // see below
    private final ConverterProvider converterProvider;

/*
    A Note About 'enumType'
    -----------------------

    The core API defines Enum fields only by their identifier lists. That means two different Enum<?> classes
    that have the same identifier lists can be interchangeable at the core API level. However, we can't support
    that at the Java layer when the fields are indexed, because we wouldn't know which Enum<?> type to use to
    represent them. Therefore, an indexed Enum<?> field at the Java layer must have a consistent Enum<?> type.
    Same thing applies to Enum<?> fields that are part of composite indexes.
*/

    SimpleFieldIndexInfo(JSimpleField jfield) {
        super(jfield.storageId);
        assert jfield.indexed;
        this.fieldType = jfield.fieldType.genericizeForIndex();
        this.enumType = jfield.getEnumType();
        this.converterProvider = ConverterProvider.identityForNull(jfield::getConverter);
    }

    /**
     * Get associated {@link FieldType}.
     *
     * @return indexed {@link FieldType}
     */
    public FieldType<?> getFieldType() {
        return this.fieldType;
    }

    /**
     * Get associated {@link Enum} type (for {@link Enum} fields only).
     *
     * @return associated {@link Enum} type, or null if this does not represent an {@link Enum} field
     */
    public Class<? extends Enum<?>> getEnumType() {
        return this.enumType;
    }

    /**
     * Get a {@link Converter} that converts this indexed field's type from what the core database returns
     * to what the Java application expects.
     *
     * @param jtx transaction
     * @return {@link Converter} from core API to Java, never null.
     */
    @Override
    public Converter<?, ?> getConverter(JTransaction jtx) {
        return this.converterProvider.getConverter(jtx);
    }

    /**
     * Read this index.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Object toIndex(JTransaction jtx) {
        return new ConvertedIndex(jtx.tx.queryIndex(this.storageId), this.getConverter(jtx), jtx.referenceConverter);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final SimpleFieldIndexInfo that = (SimpleFieldIndexInfo)obj;
        return this.fieldType.equals(that.fieldType) && Objects.equals(this.enumType, that.enumType);
    }

    @Override
    public String toString() {
        return this.toStringPrefix() + "]";
    }

    protected String toStringPrefix() {
        return this.getClass().getSimpleName()
          + "[storageId=" + this.getStorageId()
          + ",fieldType=" + this.getFieldType()
          + (this.enumType != null ? ",enumType=" + this.enumType : "");
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.fieldType.hashCode() ^ Objects.hashCode(this.enumType);
    }
}
