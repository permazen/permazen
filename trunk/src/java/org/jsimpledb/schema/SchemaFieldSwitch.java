
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.schema;

/**
 * Visitor pattern interface for {@link SchemaField}s.
 *
 * @param <R> switch method return type
 */
public interface SchemaFieldSwitch<R> {

    /**
     * Handle a {@link SetSchemaField}.
     */
    R caseSetSchemaField(SetSchemaField field);

    /**
     * Handle a {@link ListSchemaField}.
     */
    R caseListSchemaField(ListSchemaField field);

    /**
     * Handle a {@link MapSchemaField}.
     */
    R caseMapSchemaField(MapSchemaField field);

    /**
     * Handle a {@link SimpleSchemaField}.
     */
    R caseSimpleSchemaField(SimpleSchemaField field);

    /**
     * Handle a {@link ReferenceSchemaField}.
     */
    R caseReferenceSchemaField(ReferenceSchemaField field);

    /**
     * Handle a {@link CounterSchemaField}.
     */
    R caseCounterSchemaField(CounterSchemaField field);
}

