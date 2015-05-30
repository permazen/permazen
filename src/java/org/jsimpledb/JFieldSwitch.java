
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

/**
 * Visitor pattern interface for {@link JField}s.
 *
 * @param <R> switch method return type
 * @see JField#visit
 */
public interface JFieldSwitch<R> {

    /**
     * Handle a {@link JSetField}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    R caseJSetField(JSetField field);

    /**
     * Handle a {@link JListField}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    R caseJListField(JListField field);

    /**
     * Handle a {@link JMapField}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    R caseJMapField(JMapField field);

    /**
     * Handle a {@link JSimpleField}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    R caseJSimpleField(JSimpleField field);

    /**
     * Handle a {@link JReferenceField}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    R caseJReferenceField(JReferenceField field);

    /**
     * Handle a {@link JEnumField}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    R caseJEnumField(JEnumField field);

    /**
     * Handle a {@link JCounterField}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    R caseJCounterField(JCounterField field);
}

