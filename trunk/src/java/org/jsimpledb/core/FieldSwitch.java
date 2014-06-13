
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

/**
 * Visitor pattern interface for {@link Field}s.
 *
 * @param <R> switch method return type
 */
public interface FieldSwitch<R> {

    /**
     * Handle a {@link SetField}.
     */
    <E> R caseSetField(SetField<E> field);

    /**
     * Handle a {@link ListField}.
     */
    <E> R caseListField(ListField<E> field);

    /**
     * Handle a {@link MapField}.
     */
    <K, V> R caseMapField(MapField<K, V> field);

    /**
     * Handle a {@link SimpleField}.
     */
    <T> R caseSimpleField(SimpleField<T> field);

    /**
     * Handle a {@link ReferenceField}.
     */
    R caseReferenceField(ReferenceField field);

    /**
     * Handle a {@link CounterField}.
     */
    R caseCounterField(CounterField field);
}

