
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

/**
 * Visitor pattern interface for {@link JField}s.
 *
 * @param <R> switch method return type
 */
public interface JFieldSwitch<R> {

    /**
     * Handle a {@link JSetField}.
     */
    R caseJSetField(JSetField field);

    /**
     * Handle a {@link JListField}.
     */
    R caseJListField(JListField field);

    /**
     * Handle a {@link JMapField}.
     */
    R caseJMapField(JMapField field);

    /**
     * Handle a {@link JSimpleField}.
     */
    R caseJSimpleField(JSimpleField field);

    /**
     * Handle a {@link JReferenceField}.
     */
    R caseJReferenceField(JReferenceField field);

    /**
     * Handle a {@link JEnumField}.
     */
    R caseJEnumField(JEnumField field);

    /**
     * Handle a {@link JCounterField}.
     */
    R caseJCounterField(JCounterField field);
}

