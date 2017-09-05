
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

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
     */
    @Override
    public R caseReferenceSchemaField(ReferenceSchemaField field) {
        return this.caseSimpleSchemaField(field);
    }

    /**
     * Handle a {@link EnumSchemaField}.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitchAdapter} delegates to
     * {@link #caseSimpleSchemaField caseSimpleSchemaField()}.
     */
    @Override
    public R caseEnumSchemaField(EnumSchemaField field) {
        return this.caseSimpleSchemaField(field);
    }

    /**
     * Handle a {@link CounterSchemaField}.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitchAdapter} delegates to {@link #caseDefault caseDefault()}.
     */
    @Override
    public R caseCounterSchemaField(CounterSchemaField field) {
        return this.caseDefault(field);
    }

    /**
     * Adapter class roll-up method.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitchAdapter} delegates to {@link #caseComplexSchemaField caseComplexSchemaField()}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    protected R caseCollectionSchemaField(CollectionSchemaField field) {
        return this.caseComplexSchemaField(field);
    }

    /**
     * Adapter class roll-up method.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitchAdapter} delegates to {@link #caseDefault caseDefault()}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    protected R caseComplexSchemaField(ComplexSchemaField field) {
        return this.caseDefault(field);
    }

    /**
     * Adapter class roll-up method.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitchAdapter} always throws {@link UnsupportedOperationException}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    protected R caseDefault(SchemaField field) {
        throw new UnsupportedOperationException("field type not handled: " + field);
    }
}

