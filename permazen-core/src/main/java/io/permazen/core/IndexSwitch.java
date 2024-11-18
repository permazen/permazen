
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.Collection;

/**
 * Visitor pattern interface for {@link Index}es.
 *
 * @param <R> switch method return type
 * @see Index#visit Index.visit()
 */
public interface IndexSwitch<R> {

    /**
     * Handle a {@link SimpleFieldIndex}.
     *
     * <p>
     * The implementation in {@link IndexSwitch} delegates to {@link #caseSimpleIndex caseSimpleIndex()}.
     *
     * @param index visiting index
     * @param <T> field value type
     * @return visitor return value
     */
    default <T> R caseSimpleFieldIndex(SimpleFieldIndex<T> index) {
        return this.caseSimpleIndex(index);
    }

    /**
     * Handle a {@link ListElementIndex}.
     *
     * <p>
     * The implementation in {@link IndexSwitch} delegates to {@link #caseCollectionElementIndex caseCollectionElementIndex()}.
     *
     * @param index visiting index
     * @param <E> list element type
     * @return visitor return value
     */
    default <E> R caseListElementIndex(ListElementIndex<E> index) {
        return this.caseCollectionElementIndex(index);
    }

    /**
     * Handle a {@link SetElementIndex}.
     *
     * <p>
     * The implementation in {@link IndexSwitch} delegates to {@link #caseCollectionElementIndex caseCollectionElementIndex()}.
     *
     * @param index visiting index
     * @param <E> set element type
     * @return visitor return value
     */
    default <E> R caseSetElementIndex(SetElementIndex<E> index) {
        return this.caseCollectionElementIndex(index);
    }

    /**
     * Handle a {@link MapKeyIndex}.
     *
     * <p>
     * The implementation in {@link IndexSwitch} delegates to {@link #caseComplexSubFieldIndex caseComplexSubFieldIndex()}.
     *
     * @param index visiting index
     * @param <K> map key type
     * @param <V> map value type
     * @return visitor return value
     */
    default <K, V> R caseMapKeyIndex(MapKeyIndex<K, V> index) {
        return this.caseComplexSubFieldIndex(index);
    }

    /**
     * Handle a {@link MapValueIndex}.
     *
     * <p>
     * The implementation in {@link IndexSwitch} delegates to {@link #caseComplexSubFieldIndex caseComplexSubFieldIndex()}.
     *
     * @param index visiting index
     * @param <K> map key type
     * @param <V> map value type
     * @return visitor return value
     */
    default <K, V> R caseMapValueIndex(MapValueIndex<K, V> index) {
        return this.caseComplexSubFieldIndex(index);
    }

    /**
     * Handle a {@link CompositeIndex}.
     *
     * <p>
     * The implementation in {@link IndexSwitch} delegates to {@link #caseIndex caseIndex()}.
     *
     * @param index visiting index
     * @return visitor return value
     */
    default R caseCompositeIndex(CompositeIndex index) {
        return this.caseIndex(index);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link IndexSwitch} delegates to {@link #caseComplexSubFieldIndex caseComplexSubFieldIndex()}.
     *
     * @param index visiting index
     * @param <C> field value type
     * @param <E> collection element type
     * @return visitor return value
     */
    default <C extends Collection<E>, E> R caseCollectionElementIndex(CollectionElementIndex<C, E> index) {
        return this.caseComplexSubFieldIndex(index);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link IndexSwitch} delegates to {@link #caseIndex caseSimpleIndex()}.
     *
     * @param index visiting index
     * @param <T> field value type
     * @return visitor return value
     */
    default <C, T> R caseComplexSubFieldIndex(ComplexSubFieldIndex<C, T> index) {
        return this.caseSimpleIndex(index);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link IndexSwitch} delegates to {@link #caseIndex caseIndex()}.
     *
     * @param index visiting index
     * @param <T> field value type
     * @return visitor return value
     */
    default <T> R caseSimpleIndex(SimpleIndex<T> index) {
        return this.caseIndex(index);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link IndexSwitch} always throws {@link UnsupportedOperationException}.
     *
     * @param index visiting index
     * @return visitor return value
     */
    default R caseIndex(Index index) {
        throw new UnsupportedOperationException(String.format("index type not handled: %s", index));
    }
}
