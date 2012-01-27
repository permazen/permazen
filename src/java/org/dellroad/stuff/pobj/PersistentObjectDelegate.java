
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.io.IOException;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.xml.transform.Result;
import javax.xml.transform.Source;

/**
 * Delegate interface required for {@link PersistentObject}s.
 * Instances provide methods for converting to/from XML, validation, etc.
 *
 * @param <T> type of the root persistent object
 * @see PersistentObject
 */
public interface PersistentObjectDelegate<T> {

    /**
     * Serialize a root object into XML.
     *
     * <p>
     * Note that this method effectively defines what is contained in the object graph
     * rooted at {@code obj}.
     *
     * <p>
     * This method must not modify {@code obj} or any other object in its object graph.
     *
     * @param obj object to serialize; must not be modified
     * @param result XML destination
     * @throws PersistentObjectException if an error occurs
     */
    void serialize(T obj, Result result) throws IOException;

    /**
     * Deserialize a root object from XML.
     *
     * @param source XML source
     * @return deserialized object
     * @throws PersistentObjectException if an error occurs
     */
    T deserialize(Source source) throws IOException;

    /**
     * Make a deep copy of the given object.
     *
     * <p>
     * For correct behavior, this behavior of this method should be equivalent to a
     * {@linkplain #serialize serialization} followed by a {@linkplain #deserialize deserialization}.
     *
     * <p>
     * This method must not modify {@code original} or any other object in its object graph.
     *
     * @throws IllegalArgumentException if {@code original} is null
     * @throws PersistentObjectException if an error occurs
     */
    T copy(T original);

    /**
     * Attempt to determine whether two object graphs are identical.
     *
     * <p>
     * This optional method is an optimization to detect invocations to {@link PersistentObject#setRoot PersistentObject.setRoot()}
     * where the new object graph and the old object graph are identical. In such cases, no change is applied,
     * the version number does not increase, and no notifications are sent.
     *
     * <p>
     * It is always safe and correct for this method to return false. If it returns true, then it must be the case
     * that the two object graphs are identical.
     *
     * <p>
     * This method must not modify {@code oldRoot} or {@code newRoot} or any other object in their object respective graphs.
     *
     * @param root1 root of first object graph
     * @param root2 root of second object graph
     * @throws IllegalArgumentException if {@code oldRoot} or {@code newRoot} is null
     * @throws PersistentObjectException if an error occurs
     */
    boolean isSameGraph(T root1, T root2);

    /**
     * Validate the given object.
     *
     * <p>
     * This method must not modify {@code obj} or any other object in its object graph.
     *
     * @throws IllegalArgumentException if {@code obj} is null
     * @return set of zero or more constraint violations
     */
    Set<ConstraintViolation<T>> validate(T obj);

    /**
     * Handle an exception thrown during a delayed write-back attempt. {@link ThreadDeath} exceptions are not
     * passed to this method, but all others are.
     *
     * @param pobj the instance that encountered the exception
     * @param t the exception thrown
     */
    void handleWritebackException(PersistentObject<T> pobj, Throwable t);
}

