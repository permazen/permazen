
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Visitor pattern interface for {@link Field}s.
 *
 * @param <R> switch method return type
 * @see Field#visit Field.visit()
 */
public interface FieldSwitch<R> {

    /**
     * Handle a {@link SetField}.
     *
     * @param field visiting field
     * @param <E> set element type
     * @return visitor return value
     */
    <E> R caseSetField(SetField<E> field);

    /**
     * Handle a {@link ListField}.
     *
     * @param field visiting field
     * @param <E> list element type
     * @return visitor return value
     */
    <E> R caseListField(ListField<E> field);

    /**
     * Handle a {@link MapField}.
     *
     * @param field visiting field
     * @param <K> map key type
     * @param <V> map value type
     * @return visitor return value
     */
    <K, V> R caseMapField(MapField<K, V> field);

    /**
     * Handle a {@link SimpleField}.
     *
     * @param field visiting field
     * @param <T> field type
     * @return visitor return value
     */
    <T> R caseSimpleField(SimpleField<T> field);

    /**
     * Handle a {@link ReferenceField}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    R caseReferenceField(ReferenceField field);

    /**
     * Handle a {@link CounterField}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    R caseCounterField(CounterField field);

    /**
     * Handle an {@link EnumField}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    R caseEnumField(EnumField field);
}

