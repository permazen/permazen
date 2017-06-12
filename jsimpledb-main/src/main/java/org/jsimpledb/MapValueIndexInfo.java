
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;

import java.util.Objects;

import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.core.type.ReferenceFieldType;

/**
 * Represents an index on the {@code value} sub-field of a {@link JMapField}.
 */
class MapValueIndexInfo extends ComplexSubFieldIndexInfo {

    private final int keyFieldStorageId;
    private final FieldType<?> keyFieldType;
    private final Class<? extends Enum<?>> keyEnumType;
    private final ConverterProvider keyConverterProvider;

    @SuppressWarnings("unchecked")
    MapValueIndexInfo(JMapField jfield) {
        super(jfield.valueField);
        this.keyFieldStorageId = jfield.keyField.storageId;
        this.keyFieldType = jfield.keyField.fieldType.genericizeForIndex();
        this.keyEnumType = jfield.keyField instanceof JEnumField ?
          (Class<? extends Enum<?>>)((JEnumField)jfield.keyField).getTypeToken().getRawType() : null;
        this.keyConverterProvider = ConverterProvider.identityForNull(jfield.keyField::getConverter);
    }

    /**
     * Get the associated key field storage ID.
     *
     * @return key field storage ID
     */
    public int getKeyFieldStorageId() {
        return this.keyFieldStorageId;
    }

    /**
     * Get the associated key field {@link FieldType}.
     *
     * @return key field {@link FieldType}
     */
    public FieldType<?> getKeyFieldType() {
        return this.keyFieldType;
    }

    /**
     * Get a {@link Converter} that converts the map field's key type from what the core database returns
     * to what the Java application expects, or null if no conversion is needed.
     *
     * @param jtx transaction
     * @return key field {@link Converter} from core API to Java, or null if no conversion is required
     */
    public Converter<?, ?> getKeyConverter(JTransaction jtx) {
        return this.keyConverterProvider.getConverter(jtx);
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Object toIndex(JTransaction jtx) {
        return new ConvertedIndex2(jtx.tx.queryMapValueIndex(this.storageId),
          this.getConverter(jtx), jtx.referenceConverter, this.getKeyConverter(jtx));
    }

    @Override
    protected Iterable<?> iterateReferences(Transaction tx, ObjId id) {
        assert this.getFieldType() instanceof ReferenceFieldType;
        return tx.readMapField(id, this.getParentStorageId(), false).values();
    }

// Object

    @Override
    protected String toStringPrefix() {
        return super.toStringPrefix()
          + ",keyFieldStorageId=" + this.keyFieldStorageId
          + ",keyFieldType=" + this.keyFieldType
          + (this.keyEnumType != null ? ",keyEnumType=" + this.keyEnumType : "");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final MapValueIndexInfo that = (MapValueIndexInfo)obj;
        return this.keyFieldStorageId == that.keyFieldStorageId
          && this.keyFieldType.equals(that.keyFieldType)
          && Objects.equals(this.keyEnumType, that.keyEnumType);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.keyFieldStorageId ^ this.keyFieldType.hashCode() ^ Objects.hashCode(this.keyEnumType);
    }
}
