
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;

import io.permazen.core.Encoding;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;
import io.permazen.core.type.ReferenceEncoding;

import java.util.Objects;

/**
 * Represents an index on the {@code value} sub-field of a {@link JMapField}.
 */
class MapValueIndexInfo extends ComplexSubFieldIndexInfo {

    private final int keyFieldStorageId;
    private final Encoding<?> keyEncoding;
    private final Class<? extends Enum<?>> keyEnumType;         // see "A Note About 'enumType'" in SimpleFieldIndexInfo.java
    private final ConverterProvider keyConverterProvider;

    MapValueIndexInfo(JMapField jfield) {
        super(jfield.valueField);
        this.keyFieldStorageId = jfield.keyField.storageId;
        this.keyEncoding = jfield.keyField.encoding.genericizeForIndex();
        this.keyEnumType = jfield.keyField.getEnumType();
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
     * Get the associated key field {@link Encoding}.
     *
     * @return key field {@link Encoding}
     */
    public Encoding<?> getKeyEncoding() {
        return this.keyEncoding;
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
        assert this.getEncoding() instanceof ReferenceEncoding;
        return tx.readMapField(id, this.getParentStorageId(), false).values();
    }

// Object

    @Override
    protected String toStringPrefix() {
        return super.toStringPrefix()
          + ",keyFieldStorageId=" + this.keyFieldStorageId
          + ",keyEncoding=" + this.keyEncoding
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
          && this.keyEncoding.equals(that.keyEncoding)
          && Objects.equals(this.keyEnumType, that.keyEnumType);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.keyFieldStorageId ^ this.keyEncoding.hashCode() ^ Objects.hashCode(this.keyEnumType);
    }
}
