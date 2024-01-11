
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

/**
 * Visitor pattern interface for {@link PermazenField}s.
 *
 * @param <R> switch method return type
 * @see PermazenField#visit
 */
public interface PermazenFieldSwitch<R> {

    /**
     * Handle a {@link PermazenSetField}.
     *
     * <p>
     * The implementation in {@link PermazenFieldSwitch} delegates to
     * {@link #casePermazenCollectionField casePermazenCollectionField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R casePermazenSetField(PermazenSetField field) {
        return this.casePermazenCollectionField(field);
    }

    /**
     * Handle a {@link PermazenListField}.
     *
     * <p>
     * The implementation in {@link PermazenFieldSwitch} delegates to
     * {@link #casePermazenCollectionField casePermazenCollectionField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R casePermazenListField(PermazenListField field) {
        return this.casePermazenCollectionField(field);
    }

    /**
     * Handle a {@link PermazenMapField}.
     *
     * <p>
     * The implementation in {@link PermazenFieldSwitch} delegates to {@link #casePermazenComplexField casePermazenComplexField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R casePermazenMapField(PermazenMapField field) {
        return this.casePermazenComplexField(field);
    }

    /**
     * Handle a {@link PermazenSimpleField}.
     *
     * <p>
     * The implementation in {@link PermazenFieldSwitch} delegates to {@link #casePermazenField casePermazenField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R casePermazenSimpleField(PermazenSimpleField field) {
        return this.casePermazenField(field);
    }

    /**
     * Handle a {@link PermazenReferenceField}.
     *
     * <p>
     * The implementation in {@link PermazenFieldSwitch} delegates to {@link #casePermazenSimpleField casePermazenSimpleField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R casePermazenReferenceField(PermazenReferenceField field) {
        return this.casePermazenSimpleField(field);
    }

    /**
     * Handle a {@link PermazenEnumField}.
     *
     * <p>
     * The implementation in {@link PermazenFieldSwitch} delegates to {@link #casePermazenSimpleField casePermazenSimpleField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R casePermazenEnumField(PermazenEnumField field) {
        return this.casePermazenSimpleField(field);
    }

    /**
     * Handle a {@link PermazenEnumArrayField}.
     *
     * <p>
     * The implementation in {@link PermazenFieldSwitch} delegates to {@link #casePermazenSimpleField casePermazenSimpleField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R casePermazenEnumArrayField(PermazenEnumArrayField field) {
        return this.casePermazenSimpleField(field);
    }

    /**
     * Handle a {@link PermazenCounterField}.
     *
     * <p>
     * The implementation in {@link PermazenFieldSwitch} delegates to {@link #casePermazenField casePermazenField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R casePermazenCounterField(PermazenCounterField field) {
        return this.casePermazenField(field);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link PermazenFieldSwitch} delegates to {@link #casePermazenComplexField casePermazenComplexField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R casePermazenCollectionField(PermazenCollectionField field) {
        return this.casePermazenComplexField(field);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link PermazenFieldSwitch} delegates to {@link #casePermazenField casePermazenField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R casePermazenComplexField(PermazenComplexField field) {
        return this.casePermazenField(field);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link PermazenFieldSwitch} always throws {@link UnsupportedOperationException}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    default R casePermazenField(PermazenField field) {
        throw new UnsupportedOperationException("field type not handled: " + field);
    }
}
