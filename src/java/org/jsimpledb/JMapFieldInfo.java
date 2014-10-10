
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Converter;

import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.NavigableMap;

import org.jsimpledb.core.MapField;
import org.jsimpledb.core.ObjId;

class JMapFieldInfo extends JComplexFieldInfo {

    final JSimpleFieldInfo keyFieldInfo;
    final JSimpleFieldInfo valueFieldInfo;

    JMapFieldInfo(JMapField jfield) {
        super(jfield);
        this.keyFieldInfo = jfield.getKeyField().toJFieldInfo();
        this.valueFieldInfo = jfield.getValueField().toJFieldInfo();
    }

    /**
     * Get the key sub-field info.
     */
    public JSimpleFieldInfo getKeyFieldInfo() {
        return this.keyFieldInfo;
    }

    /**
     * Get the value sub-field info.
     */
    public JSimpleFieldInfo getValueFieldInfo() {
        return this.valueFieldInfo;
    }

    @Override
    public List<JSimpleFieldInfo> getSubFieldInfos() {
        return Arrays.asList(this.keyFieldInfo, this.valueFieldInfo);
    }

    @Override
    public String getSubFieldInfoName(JSimpleFieldInfo subFieldInfo) {
        if (subFieldInfo.storageId == this.keyFieldInfo.storageId)
            return MapField.KEY_FIELD_NAME;
        if (subFieldInfo.storageId == this.valueFieldInfo.storageId)
            return MapField.VALUE_FIELD_NAME;
        throw new RuntimeException("internal error");
    }

    @Override
    public NavigableMapConverter<?, ?, ?, ?> getConverter(JTransaction jtx) {
        final Converter<?, ?> keyConverter = this.keyFieldInfo.getConverter(jtx);
        final Converter<?, ?> valueConverter = this.valueFieldInfo.getConverter(jtx);
        return keyConverter != null || valueConverter != null ?
          this.createConverter(
           keyConverter != null ? keyConverter : Converter.identity(),
           valueConverter != null ? valueConverter : Converter.identity()) :
          null;
    }

    // This method exists solely to bind the generic type parameters
    private <K, V, WK, WV> NavigableMapConverter<K, V, WK, WV> createConverter(
      Converter<K, WK> keyConverter, Converter<V, WV> valueConverter) {
        return new NavigableMapConverter<K, V, WK, WV>(keyConverter, valueConverter);
    }

    @Override
    public void copyRecurse(ObjIdSet seen, JTransaction srcTx, JTransaction dstTx,
      ObjId id, int storageId, Deque<Integer> nextFields) {
        final NavigableMap<?, ?> map = srcTx.tx.readMapField(id, this.storageId, false);
        if (storageId == this.keyFieldInfo.storageId)
            this.copyRecurse(seen, srcTx, dstTx, map.keySet(), nextFields);
        else if (storageId == this.valueFieldInfo.storageId)
            this.copyRecurse(seen, srcTx, dstTx, map.values(), nextFields);
        else
            throw new RuntimeException("internal error");
    }

// Object

    @Override
    public String toString() {
        return "map " + super.toString() + ", key " + this.keyFieldInfo + ", and value " + this.valueFieldInfo;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final JMapFieldInfo that = (JMapFieldInfo)obj;
        return this.keyFieldInfo.equals(that.keyFieldInfo) && this.valueFieldInfo.equals(that.valueFieldInfo);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.keyFieldInfo.hashCode() ^ this.valueFieldInfo.hashCode();
    }
}

