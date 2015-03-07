
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

/**
 * Visitor pattern interface for the {@link Change} class hierarchy.
 *
 * @param <R> method return type
 * @see Change#visit
 */
public interface ChangeSwitch<R> {

    /**
     * Handle an {@link ObjectCreate} event.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @return visitor return value
     */
    <T> R caseObjectCreate(ObjectCreate<T> change);

    /**
     * Handle an {@link ObjectDelete} event.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @return visitor return value
     */
    <T> R caseObjectDelete(ObjectDelete<T> change);

    /**
     * Handle a {@link ListFieldAdd} event.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <E> changed list element type
     * @return visitor return value
     */
    <T, E> R caseListFieldAdd(ListFieldAdd<T, E> change);

    /**
     * Handle a {@link ListFieldClear} event.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @return visitor return value
     */
    <T> R caseListFieldClear(ListFieldClear<T> change);

    /**
     * Handle a {@link ListFieldRemove} event.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <E> changed list element type
     * @return visitor return value
     */
    <T, E> R caseListFieldRemove(ListFieldRemove<T, E> change);

    /**
     * Handle a {@link ListFieldReplace} event.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <E> changed list element type
     * @return visitor return value
     */
    <T, E> R caseListFieldReplace(ListFieldReplace<T, E> change);

    /**
     * Handle a {@link MapFieldAdd} event.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <K> changed map key type
     * @param <V> changed map value type
     * @return visitor return value
     */
    <T, K, V> R caseMapFieldAdd(MapFieldAdd<T, K, V> change);

    /**
     * Handle a {@link MapFieldClear} event.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @return visitor return value
     */
    <T> R caseMapFieldClear(MapFieldClear<T> change);

    /**
     * Handle a {@link MapFieldRemove} event.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <K> changed map key type
     * @param <V> changed map value type
     * @return visitor return value
     */
    <T, K, V> R caseMapFieldRemove(MapFieldRemove<T, K, V> change);

    /**
     * Handle a {@link MapFieldReplace} event.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <K> changed map key type
     * @param <V> changed map value type
     * @return visitor return value
     */
    <T, K, V> R caseMapFieldReplace(MapFieldReplace<T, K, V> change);

    /**
     * Handle a {@link SetFieldAdd} event.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <E> changed set element type
     * @return visitor return value
     */
    <T, E> R caseSetFieldAdd(SetFieldAdd<T, E> change);

    /**
     * Handle a {@link SetFieldClear} event.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @return visitor return value
     */
    <T> R caseSetFieldClear(SetFieldClear<T> change);

    /**
     * Handle a {@link SetFieldRemove} event.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <E> changed set element type
     * @return visitor return value
     */
    <T, E> R caseSetFieldRemove(SetFieldRemove<T, E> change);

    /**
     * Handle a {@link SimpleFieldChange} event.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <V> changed field type
     * @return visitor return value
     */
    <T, V> R caseSimpleFieldChange(SimpleFieldChange<T, V> change);
}

