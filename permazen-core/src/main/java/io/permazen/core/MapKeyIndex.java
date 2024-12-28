
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.schema.SimpleSchemaField;
import io.permazen.util.ByteData;

import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Predicate;

/**
 * An index on the keys of a map field.
 *
 * @param <K> map key type
 * @param <V> map value type
 */
public class MapKeyIndex<K, V> extends ComplexSubFieldIndex<NavigableMap<K, V>, K> {

// Constructor

    MapKeyIndex(Schema schema, SimpleSchemaField schemaField, ObjType objType, MapField<K, V> field) {
        super(schema, schemaField, objType, field, field.getKeyField());
    }

// IndexSwitch

    @Override
    public <R> R visit(IndexSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.caseMapKeyIndex(this);
    }

// Package methods

    @Override
    boolean isPrefixModeForIndex() {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    void unreference(Transaction tx, boolean remove, ObjId target, ObjId referrer, ByteData prefix) {
        final NavigableMap<?, ?> map = tx.readMapField(referrer, this.getField().parent.name, false);
        final Object value = map.remove(target);
        if (!remove)
            ((NavigableMap<?, Object>)map).put(null, value);
    }

    @Override
    void readAllNonNull(Transaction tx, ObjId target, Set<K> values, Predicate<? super K> filter) {
        for (K key : this.parentRepresentative.getValueInternal(tx, target).keySet()) {
            if (key != null && (filter == null || filter.test(key)))
                values.add(key);
        }
    }
}
