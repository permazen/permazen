
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
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
     * @throws IllegalArgumentException if {@code original} is null
     * @throws PersistentObjectException if an error occurs
     */
    T copy(T original);

    /**
     * Validate the given object.
     *
     * @throws IllegalArgumentException if {@code obj} is null
     * @throws PersistentObjectException if validation fails
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

