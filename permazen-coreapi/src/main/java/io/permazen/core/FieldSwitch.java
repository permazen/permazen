
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.Collection;

/**
 * Visitor pattern interface for {@link Field}s.
 *
 * @param <R> switch method return type
 * @see Field#visit Field.visit()
 */
public interface FieldSwitch<R> {

    /**
     * Handle a {@link SetField}.
     *
     * <p>
     * The implementation in {@link FieldSwitch} delegates to {@link #caseCollectionField caseCollectionField()}.
     *
     * @param field visiting field
     * @param <E> set element type
     * @return visitor return value
     */
    default <E> R caseSetField(SetField<E> field) {
        return this.caseCollectionField(field);
    }

    /**
     * Handle a {@link ListField}.
     *
     * <p>
     * The implementation in {@link FieldSwitch} delegates to {@link #caseCollectionField caseCollectionField()}.
     *
     * @param field visiting field
     * @param <E> list element type
     * @return visitor return value
     */
    default <E> R caseListField(ListField<E> field) {
        return this.caseCollectionField(field);
    }

    /**
     * Handle a {@link MapField}.
     *
     * <p>
     * The implementation in {@link FieldSwitch} delegates to {@link #caseComplexField caseComplexField()}.
     *
     * @param field visiting field
     * @param <K> map key type
     * @param <V> map value type
     * @return visitor return value
     */
    default <K, V> R caseMapField(MapField<K, V> field) {
        return this.caseComplexField(field);
    }

    /**
     * Handle a {@link SimpleField}.
     *
     * <p>
     * The implementation in {@link FieldSwitch} delegates to {@link #caseField caseField()}.
     *
     * @param field visiting field
     * @param <T> field type
     * @return visitor return value
     */
    default <T> R caseSimpleField(SimpleField<T> field) {
        return this.caseField(field);
    }

    /**
     * Handle a {@link ReferenceField}.
     *
     * <p>
     * The implementation in {@link FieldSwitch} delegates to {@link #caseSimpleField caseSimpleField()}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    default R caseReferenceField(ReferenceField field) {
        return this.caseSimpleField(field);
    }

    /**
     * Handle a {@link CounterField}.
     *
     * <p>
     * The implementation in {@link FieldSwitch} delegates to {@link #caseField caseField()}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    default R caseCounterField(CounterField field) {
        return this.caseField(field);
    }

    /**
     * Handle an {@link EnumField}.
     *
     * <p>
     * The implementation in {@link FieldSwitch} delegates to {@link #caseSimpleField caseSimpleField()}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    default R caseEnumField(EnumField field) {
        return this.caseSimpleField(field);
    }

    /**
     * Handle an {@link EnumArrayField}.
     *
     * <p>
     * The implementation in {@link FieldSwitch} delegates to {@link #caseSimpleField caseSimpleField()}.
     *
     * @param field visiting field
     * @return visitor return value
     */
    default R caseEnumArrayField(EnumArrayField field) {
        return this.caseSimpleField(field);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link FieldSwitch} delegates to {@link #caseComplexField caseComplexField()}.
     *
     * @param field visiting field
     * @param <C> visiting field type
     * @param <E> collection element type
     * @return visitor return value
     */
    default <C extends Collection<E>, E> R caseCollectionField(CollectionField<C, E> field) {
        return this.caseComplexField(field);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link FieldSwitch} delegates to {@link #caseField caseField()}.
     *
     * @param field visiting field
     * @param <T> visiting field type
     * @return visitor return value
     */
    default <T> R caseComplexField(ComplexField<T> field) {
        return this.caseField(field);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link FieldSwitch} always throws {@link UnsupportedOperationException}.
     *
     * @param field visiting field
     * @param <T> visiting field type
     * @return visitor return value
     */
    default <T> R caseField(Field<T> field) {
        throw new UnsupportedOperationException("field type not handled: " + field);
    }
}
