
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

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
     * <p>
     * The implementation in {@link JFieldSwitch} delegates to {@link #caseJCollectionField caseJCollectionField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R caseJSetField(JSetField field) {
        return this.caseJCollectionField(field);
    }

    /**
     * Handle a {@link JListField}.
     *
     * <p>
     * The implementation in {@link JFieldSwitch} delegates to {@link #caseJCollectionField caseJCollectionField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R caseJListField(JListField field) {
        return this.caseJCollectionField(field);
    }

    /**
     * Handle a {@link JMapField}.
     *
     * <p>
     * The implementation in {@link JFieldSwitch} delegates to {@link #caseJComplexField caseJComplexField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R caseJMapField(JMapField field) {
        return this.caseJComplexField(field);
    }

    /**
     * Handle a {@link JSimpleField}.
     *
     * <p>
     * The implementation in {@link JFieldSwitch} delegates to {@link #caseJField caseJField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R caseJSimpleField(JSimpleField field) {
        return this.caseJField(field);
    }

    /**
     * Handle a {@link JReferenceField}.
     *
     * <p>
     * The implementation in {@link JFieldSwitch} delegates to {@link #caseJSimpleField caseJSimpleField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R caseJReferenceField(JReferenceField field) {
        return this.caseJSimpleField(field);
    }

    /**
     * Handle a {@link JEnumField}.
     *
     * <p>
     * The implementation in {@link JFieldSwitch} delegates to {@link #caseJSimpleField caseJSimpleField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R caseJEnumField(JEnumField field) {
        return this.caseJSimpleField(field);
    }

    /**
     * Handle a {@link JEnumArrayField}.
     *
     * <p>
     * The implementation in {@link JFieldSwitch} delegates to {@link #caseJSimpleField caseJSimpleField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R caseJEnumArrayField(JEnumArrayField field) {
        return this.caseJSimpleField(field);
    }

    /**
     * Handle a {@link JCounterField}.
     *
     * <p>
     * The implementation in {@link JFieldSwitch} delegates to {@link #caseJField caseJField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R caseJCounterField(JCounterField field) {
        return this.caseJField(field);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link JFieldSwitch} delegates to {@link #caseJComplexField caseJComplexField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R caseJCollectionField(JCollectionField field) {
        return this.caseJComplexField(field);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link JFieldSwitch} delegates to {@link #caseJField caseJField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R caseJComplexField(JComplexField field) {
        return this.caseJField(field);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link JFieldSwitch} always throws {@link UnsupportedOperationException}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R caseJField(JField field) {
        throw new UnsupportedOperationException("field type not handled: " + field);
    }
}
