
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

/**
 * Adapter class for {@link SchemaFieldSwitch}.
 *
 * @param <R> switch method return type
 */
public class SchemaFieldSwitchAdapter<R> implements SchemaFieldSwitch<R> {

    /**
     * Handle a {@link SetSchemaField}.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitchAdapter} delegates to
     * {@link #caseCollectionSchemaField caseCollectionSchemaField()}.
     * </p>
     */
    @Override
    public R caseSetSchemaField(SetSchemaField field) {
        return this.caseCollectionSchemaField(field);
    }

    /**
     * Handle a {@link ListSchemaField}.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitchAdapter} delegates to
     * {@link #caseCollectionSchemaField caseCollectionSchemaField()}.
     * </p>
     */
    @Override
    public R caseListSchemaField(ListSchemaField field) {
        return this.caseCollectionSchemaField(field);
    }

    /**
     * Handle a {@link MapSchemaField}.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitchAdapter} delegates to
     * {@link #caseComplexSchemaField caseComplexSchemaField()}.
     * </p>
     */
    @Override
    public R caseMapSchemaField(MapSchemaField field) {
        return this.caseComplexSchemaField(field);
    }

    /**
     * Handle a {@link SimpleSchemaField}.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitchAdapter} delegates to
     * {@link #caseDefault caseDefault()}.
     * </p>
     */
    @Override
    public R caseSimpleSchemaField(SimpleSchemaField field) {
        return this.caseDefault(field);
    }

    /**
     * Handle a {@link ReferenceSchemaField}.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitchAdapter} delegates to
     * {@link #caseSimpleSchemaField caseSimpleSchemaField()}.
     * </p>
     */
    @Override
    public R caseReferenceSchemaField(ReferenceSchemaField field) {
        return this.caseSimpleSchemaField(field);
    }

    /**
     * Adapter class roll-up method.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitchAdapter} delegates to {@link #caseComplexSchemaField caseComplexSchemaField()}.
     * </p>
     */
    protected R caseCollectionSchemaField(CollectionSchemaField field) {
        return this.caseComplexSchemaField(field);
    }

    /**
     * Adapter class roll-up method.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitchAdapter} delegates to {@link #caseDefault caseDefault()}.
     * </p>
     */
    protected R caseComplexSchemaField(ComplexSchemaField field) {
        return this.caseDefault(field);
    }

    /**
     * Adapter class roll-up method.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitchAdapter} always throws {@link UnsupportedOperationException}.
     * </p>
     */
    protected R caseDefault(SchemaField field) {
        throw new UnsupportedOperationException("field type not handled: " + field);
    }
}

