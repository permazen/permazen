
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
     * @param field visiting field
     * @return visitor return value
     */
    R caseSetSchemaField(SetSchemaField field);

    /**
     * Handle a {@link ListSchemaField}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    R caseListSchemaField(ListSchemaField field);

    /**
     * Handle a {@link MapSchemaField}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    R caseMapSchemaField(MapSchemaField field);

    /**
     * Handle a {@link SimpleSchemaField}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    R caseSimpleSchemaField(SimpleSchemaField field);

    /**
     * Handle a {@link ReferenceSchemaField}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    R caseReferenceSchemaField(ReferenceSchemaField field);

    /**
     * Handle a {@link EnumSchemaField}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    R caseEnumSchemaField(EnumSchemaField field);

    /**
     * Handle a {@link EnumArraySchemaField}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    R caseEnumArraySchemaField(EnumArraySchemaField field);

    /**
     * Handle a {@link CounterSchemaField}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    R caseCounterSchemaField(CounterSchemaField field);
}

