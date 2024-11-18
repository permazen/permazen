
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

/**
 * Visitor pattern interface for {@link SchemaField}s.
 *
 * @param <R> switch method return type
 * @see SchemaField#visit
 */
public interface SchemaFieldSwitch<R> {

    /**
     * Handle a {@link SetSchemaField}.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitch} delegates to {@link #caseCollectionSchemaField caseCollectionSchemaField()}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    default R caseSetSchemaField(SetSchemaField field) {
        return this.caseCollectionSchemaField(field);
    }

    /**
     * Handle a {@link ListSchemaField}.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitch} delegates to {@link #caseCollectionSchemaField caseCollectionSchemaField()}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    default R caseListSchemaField(ListSchemaField field) {
        return this.caseCollectionSchemaField(field);
    }

    /**
     * Handle a {@link MapSchemaField}.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitch} delegates to {@link #caseComplexSchemaField caseComplexSchemaField()}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    default R caseMapSchemaField(MapSchemaField field) {
        return this.caseComplexSchemaField(field);
    }

    /**
     * Handle a {@link SimpleSchemaField}.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitch} delegates to {@link #caseDefault caseDefault()}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    default R caseSimpleSchemaField(SimpleSchemaField field) {
        return this.caseDefault(field);
    }

    /**
     * Handle a {@link ReferenceSchemaField}.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitch} delegates to {@link #caseSimpleSchemaField caseSimpleSchemaField()}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    default R caseReferenceSchemaField(ReferenceSchemaField field) {
        return this.caseSimpleSchemaField(field);
    }

    /**
     * Handle a {@link EnumSchemaField}.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitch} delegates to {@link #caseSimpleSchemaField caseSimpleSchemaField()}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    default R caseEnumSchemaField(EnumSchemaField field) {
        return this.caseSimpleSchemaField(field);
    }

    /**
     * Handle a {@link EnumArraySchemaField}.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitch} delegates to {@link #caseSimpleSchemaField caseSimpleSchemaField()}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    default R caseEnumArraySchemaField(EnumArraySchemaField field) {
        return this.caseSimpleSchemaField(field);
    }

    /**
     * Handle a {@link CounterSchemaField}.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitch} delegates to {@link #caseDefault caseDefault()}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    default R caseCounterSchemaField(CounterSchemaField field) {
        return this.caseDefault(field);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitch} delegates to {@link #caseComplexSchemaField caseComplexSchemaField()}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    default R caseCollectionSchemaField(CollectionSchemaField field) {
        return this.caseComplexSchemaField(field);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitch} delegates to {@link #caseDefault caseDefault()}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    default R caseComplexSchemaField(ComplexSchemaField field) {
        return this.caseDefault(field);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link SchemaFieldSwitch} always throws {@link UnsupportedOperationException}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    default R caseDefault(SchemaField field) {
        throw new UnsupportedOperationException(String.format("field type not handled: %s", field));
    }
}
