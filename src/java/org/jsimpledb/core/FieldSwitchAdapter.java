
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.Collection;

/**
 * Adapter class for {@link FieldSwitch}.
 *
 * @param <R> switch method return type
 */
public class FieldSwitchAdapter<R> implements FieldSwitch<R> {

    /**
     * Handle a {@link SetField}.
     *
     * <p>
     * The implementation in {@link FieldSwitchAdapter} delegates to {@link #caseCollectionField caseCollectionField()}.
     * </p>
     */
    @Override
    public <E> R caseSetField(SetField<E> field) {
        return this.caseCollectionField(field);
    }

    /**
     * Handle a {@link ListField}.
     *
     * <p>
     * The implementation in {@link FieldSwitchAdapter} delegates to {@link #caseCollectionField caseCollectionField()}.
     * </p>
     */
    @Override
    public <E> R caseListField(ListField<E> field) {
        return this.caseCollectionField(field);
    }

    /**
     * Handle a {@link MapField}.
     *
     * <p>
     * The implementation in {@link FieldSwitchAdapter} delegates to {@link #caseComplexField caseComplexField()}.
     * </p>
     */
    @Override
    public <K, V> R caseMapField(MapField<K, V> field) {
        return this.caseComplexField(field);
    }

    /**
     * Handle a {@link SimpleField}.
     *
     * <p>
     * The implementation in {@link FieldSwitchAdapter} delegates to {@link #caseField caseField()}.
     * </p>
     */
    @Override
    public <T> R caseSimpleField(SimpleField<T> field) {
        return this.caseField(field);
    }

    /**
     * Handle a {@link ReferenceField}.
     *
     * <p>
     * The implementation in {@link FieldSwitchAdapter} delegates to {@link #caseSimpleField caseSimpleField()}.
     * </p>
     */
    @Override
    public R caseReferenceField(ReferenceField field) {
        return this.caseSimpleField(field);
    }

    /**
     * Handle a {@link CounterField}.
     *
     * <p>
     * The implementation in {@link FieldSwitchAdapter} delegates to {@link #caseField caseField()}.
     * </p>
     * </p>
     */
    @Override
    public R caseCounterField(CounterField field) {
        return this.caseField(field);
    }

    /**
     * Adapter class roll-up method.
     *
     * <p>
     * The implementation in {@link FieldSwitchAdapter} delegates to {@link #caseComplexField caseComplexField()}.
     * </p>
     */
    protected <C extends Collection<E>, E> R caseCollectionField(CollectionField<C, E> field) {
        return this.caseComplexField(field);
    }

    /**
     * Adapter class roll-up method.
     *
     * <p>
     * The implementation in {@link FieldSwitchAdapter} delegates to {@link #caseField caseField()}.
     * </p>
     */
    protected <T> R caseComplexField(ComplexField<T> field) {
        return this.caseField(field);
    }

    /**
     * Adapter class roll-up method.
     *
     * <p>
     * The implementation in {@link FieldSwitchAdapter} always throws {@link UnsupportedOperationException}.
     * </p>
     */
    protected <T> R caseField(Field<T> field) {
        throw new UnsupportedOperationException("field type not handled: " + field);
    }
}

