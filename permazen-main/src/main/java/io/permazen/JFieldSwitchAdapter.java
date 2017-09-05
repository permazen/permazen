
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

/**
 * Adapter class for {@link JFieldSwitch}.
 *
 * @param <R> switch method return type
 */
public class JFieldSwitchAdapter<R> implements JFieldSwitch<R> {

    /**
     * Handle a {@link JSetField}.
     *
     * <p>
     * The implementation in {@link JFieldSwitchAdapter} delegates to {@link #caseJCollectionField caseJCollectionField()}.
     */
    @Override
    public R caseJSetField(JSetField field) {
        return this.caseJCollectionField(field);
    }

    /**
     * Handle a {@link JListField}.
     *
     * <p>
     * The implementation in {@link JFieldSwitchAdapter} delegates to {@link #caseJCollectionField caseJCollectionField()}.
     */
    @Override
    public R caseJListField(JListField field) {
        return this.caseJCollectionField(field);
    }

    /**
     * Handle a {@link JMapField}.
     *
     * <p>
     * The implementation in {@link JFieldSwitchAdapter} delegates to {@link #caseJComplexField caseJComplexField()}.
     */
    @Override
    public R caseJMapField(JMapField field) {
        return this.caseJComplexField(field);
    }

    /**
     * Handle a {@link JSimpleField}.
     *
     * <p>
     * The implementation in {@link JFieldSwitchAdapter} delegates to {@link #caseJField caseJField()}.
     */
    @Override
    public R caseJSimpleField(JSimpleField field) {
        return this.caseJField(field);
    }

    /**
     * Handle a {@link JReferenceField}.
     *
     * <p>
     * The implementation in {@link JFieldSwitchAdapter} delegates to {@link #caseJSimpleField caseJSimpleField()}.
     */
    @Override
    public R caseJReferenceField(JReferenceField field) {
        return this.caseJSimpleField(field);
    }

    /**
     * Handle a {@link JEnumField}.
     *
     * <p>
     * The implementation in {@link JFieldSwitchAdapter} delegates to {@link #caseJSimpleField caseJSimpleField()}.
     */
    @Override
    public R caseJEnumField(JEnumField field) {
        return this.caseJSimpleField(field);
    }

    /**
     * Handle a {@link JCounterField}.
     *
     * <p>
     * The implementation in {@link JFieldSwitchAdapter} delegates to {@link #caseJField caseJField()}.
     */
    @Override
    public R caseJCounterField(JCounterField field) {
        return this.caseJField(field);
    }

    /**
     * Adapter class roll-up method.
     *
     * <p>
     * The implementation in {@link JFieldSwitchAdapter} delegates to {@link #caseJComplexField caseJComplexField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    protected R caseJCollectionField(JCollectionField field) {
        return this.caseJComplexField(field);
    }

    /**
     * Adapter class roll-up method.
     *
     * <p>
     * The implementation in {@link JFieldSwitchAdapter} delegates to {@link #caseJField caseJField()}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    protected R caseJComplexField(JComplexField field) {
        return this.caseJField(field);
    }

    /**
     * Adapter class roll-up method.
     *
     * <p>
     * The implementation in {@link JFieldSwitchAdapter} always throws {@link UnsupportedOperationException}.
     *
     * @param field the visiting field
     * @return visitor return value
     */
    protected R caseJField(JField field) {
        throw new UnsupportedOperationException("field type not handled: " + field);
    }
}

