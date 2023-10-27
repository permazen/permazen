
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.change;

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
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseChange caseChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @return visitor return value
     */
    default <T> R caseObjectCreate(ObjectCreate<T> change) {
        return this.caseChange(change);
    }

    /**
     * Handle an {@link ObjectDelete} event.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseChange caseChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @return visitor return value
     */
    default <T> R caseObjectDelete(ObjectDelete<T> change) {
        return this.caseChange(change);
    }

    /**
     * Handle a {@link ListFieldAdd} event.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseListFieldChange caseListFieldChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <E> changed list element type
     * @return visitor return value
     */
    default <T, E> R caseListFieldAdd(ListFieldAdd<T, E> change) {
        return this.caseListFieldChange(change);
    }

    /**
     * Handle a {@link ListFieldClear} event.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseListFieldChange caseListFieldChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @return visitor return value
     */
    default <T> R caseListFieldClear(ListFieldClear<T> change) {
        return this.caseListFieldChange(change);
    }

    /**
     * Handle a {@link ListFieldRemove} event.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseListFieldChange caseListFieldChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <E> changed list element type
     * @return visitor return value
     */
    default <T, E> R caseListFieldRemove(ListFieldRemove<T, E> change) {
        return this.caseListFieldChange(change);
    }

    /**
     * Handle a {@link ListFieldReplace} event.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseListFieldChange caseListFieldChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <E> changed list element type
     * @return visitor return value
     */
    default <T, E> R caseListFieldReplace(ListFieldReplace<T, E> change) {
        return this.caseListFieldChange(change);
    }

    /**
     * Handle a {@link MapFieldAdd} event.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseMapFieldChange caseMapFieldChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <K> changed map key type
     * @param <V> changed map value type
     * @return visitor return value
     */
    default <T, K, V> R caseMapFieldAdd(MapFieldAdd<T, K, V> change) {
        return this.caseMapFieldChange(change);
    }

    /**
     * Handle a {@link MapFieldClear} event.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseMapFieldChange caseMapFieldChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @return visitor return value
     */
    default <T> R caseMapFieldClear(MapFieldClear<T> change) {
        return this.caseMapFieldChange(change);
    }

    /**
     * Handle a {@link MapFieldRemove} event.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseMapFieldChange caseMapFieldChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <K> changed map key type
     * @param <V> changed map value type
     * @return visitor return value
     */
    default <T, K, V> R caseMapFieldRemove(MapFieldRemove<T, K, V> change) {
        return this.caseMapFieldChange(change);
    }

    /**
     * Handle a {@link MapFieldReplace} event.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseMapFieldChange caseMapFieldChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <K> changed map key type
     * @param <V> changed map value type
     * @return visitor return value
     */
    default <T, K, V> R caseMapFieldReplace(MapFieldReplace<T, K, V> change) {
        return this.caseMapFieldChange(change);
    }

    /**
     * Handle a {@link SetFieldAdd} event.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseSetFieldChange caseSetFieldChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <E> changed set element type
     * @return visitor return value
     */
    default <T, E> R caseSetFieldAdd(SetFieldAdd<T, E> change) {
        return this.caseSetFieldChange(change);
    }

    /**
     * Handle a {@link SetFieldClear} event.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseSetFieldChange caseSetFieldChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @return visitor return value
     */
    default <T> R caseSetFieldClear(SetFieldClear<T> change) {
        return this.caseSetFieldChange(change);
    }

    /**
     * Handle a {@link SetFieldRemove} event.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseSetFieldChange caseSetFieldChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <E> changed set element type
     * @return visitor return value
     */
    default <T, E> R caseSetFieldRemove(SetFieldRemove<T, E> change) {
        return this.caseSetFieldChange(change);
    }

    /**
     * Handle a {@link SimpleFieldChange} event.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseSetFieldChange caseFieldChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @param <V> changed encoding
     * @return visitor return value
     */
    default <T, V> R caseSimpleFieldChange(SimpleFieldChange<T, V> change) {
        return this.caseFieldChange(change);
    }

// Roll-Up Methods

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseFieldChange caseFieldChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @return visitor return value
     */
    default <T> R caseListFieldChange(ListFieldChange<T> change) {
        return this.caseFieldChange(change);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseFieldChange caseFieldChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @return visitor return value
     */
    default <T> R caseMapFieldChange(MapFieldChange<T> change) {
        return this.caseFieldChange(change);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseFieldChange caseFieldChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @return visitor return value
     */
    default <T> R caseSetFieldChange(SetFieldChange<T> change) {
        return this.caseFieldChange(change);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} delegates to {@link #caseChange caseChange()}.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @return visitor return value
     */
    default <T> R caseFieldChange(FieldChange<T> change) {
        return this.caseChange(change);
    }

    /**
     * Visitor pattern roll-up method.
     *
     * <p>
     * The implementation in {@link ChangeSwitch} returns null.
     *
     * @param change visiting change
     * @param <T> changed object type
     * @return visitor return value
     */
    default <T> R caseChange(Change<T> change) {
        return null;
    }
}
