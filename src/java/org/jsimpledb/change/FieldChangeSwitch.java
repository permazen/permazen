
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.change;

/**
 * Visitor pattern interface for the {@link FieldChange} class hierarchy.
 *
 * @param <R> method return type
 * @see FieldChange#visit
 */
public interface FieldChangeSwitch<R> {

    <T, E> R caseListFieldAdd(ListFieldAdd<T, E> change);
    <T> R caseListFieldClear(ListFieldClear<T> change);
    <T, E> R caseListFieldRemove(ListFieldRemove<T, E> change);
    <T, E> R caseListFieldReplace(ListFieldReplace<T, E> change);
    <T, K, V> R caseMapFieldAdd(MapFieldAdd<T, K, V> change);
    <T> R caseMapFieldClear(MapFieldClear<T> change);
    <T, K, V> R caseMapFieldRemove(MapFieldRemove<T, K, V> change);
    <T, K, V> R caseMapFieldReplace(MapFieldReplace<T, K, V> change);
    <T, E> R caseSetFieldAdd(SetFieldAdd<T, E> change);
    <T> R caseSetFieldClear(SetFieldClear<T> change);
    <T, E> R caseSetFieldRemove(SetFieldRemove<T, E> change);
    <T, V> R caseSimpleFieldChange(SimpleFieldChange<T, V> change);
}

