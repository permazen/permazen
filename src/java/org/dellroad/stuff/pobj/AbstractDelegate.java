
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.dellroad.stuff.validation.ValidationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support superclass for {@link PersistentObjectDelegate} classes, with implementations of all methods
 * other than {@link #serialize serialize()} and {@link #deserialize deserialize()}.
 *
 * @param <T> type of the root persistent object
 * @see PersistentObject
 */
public abstract class AbstractDelegate<T> implements PersistentObjectDelegate<T> {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Make a deep copy of the given object.
     *
     * <p>
     * The implementation in {@link AbstractDelegate} does this by {@linkplain #serialize serializing}
     * and then {@linkplain #deserialize deserializing} the object graph.
     * Subclasses are encouraged to provide a more efficient implementation.
     *
     * @throws IllegalArgumentException if {@code original} is null
     * @throws PersistentObjectException if an error occurs
     */
    public T copy(T original) {
        if (original == null)
            throw new IllegalArgumentException("null original");
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(32 * 1024 - 32);
        try {
            this.serialize(original, new StreamResult(buffer));
        } catch (IOException e) {
            throw new PersistentObjectException("exception during serialize()");
        }
        StreamSource source = new StreamSource(new ByteArrayInputStream(buffer.toByteArray()));
        T copy;
        try {
            copy = this.deserialize(source);
        } catch (IOException e) {
            throw new PersistentObjectException("exception during deserialize()");
        }
        if (copy == null)
            throw new PersistentObjectException("null object returned by deserialize()");
        return copy;
    }

    /**
     * Validate the given instance.
     *
     * <p>
     * The implementation in {@link AbstractDelegate} performs validation using {@link ValidationContext#validate()}.
     *
     * @throws IllegalArgumentException if {@code obj} is null
     * @throws PersistentObjectException if validation fails
     */
    public Set<ConstraintViolation<T>> validate(T obj) {
        return new ValidationContext<T>(obj).validate();
    }

    /**
     * Handle an exception thrown during a delayed write-back attempt. {@link ThreadDeath} exceptions are not
     * passed to this method, but all others are.
     *
     * <p>
     * The implementation in {@link AbstractDelegate} simply logs an error to {@link #log}.
     *
     * @param pobj the instance that encountered the exception
     * @param t the exception thrown
     */
    public void handleWritebackException(PersistentObject<T> pobj, Throwable t) {
        this.log.error(pobj + ": error during write-back", t);
    }
}

