
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
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

    private static final int BUFFER_SIZE = 16 * 1024 - 32;

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Make a deep copy of the given object.
     *
     * <p>
     * The implementation in {@link AbstractDelegate} does this by {@linkplain #serialize serializing}
     * and then {@linkplain #deserialize deserializing} the object graph.
     * Subclasses are encouraged to provide a more efficient implementation, for example, by implementing
     * {@link org.dellroad.stuff.java.GraphCloneable}.
     * </p>
     *
     * @throws IllegalArgumentException if {@code original} is null
     * @throws PersistentObjectException if an error occurs
     */
    public T copy(T original) {
        if (original == null)
            throw new IllegalArgumentException("null original");
        StringWriter buffer = new StringWriter(BUFFER_SIZE);
        try {
            this.serialize(original, new StreamResult(buffer));
        } catch (IOException e) {
            throw new PersistentObjectException("exception during serialize()");
        }
        StreamSource source = new StreamSource(new StringReader(buffer.toString()));
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
     * Compare two object graphs.
     *
     * <p>
     * The implementation in {@link AbstractDelegate} only returns true if {@code root1 == root2}.
     * This is a very conservative implementation. If your root object graph correctly implements
     * {@link Object#equals equals()}, then {@code root1.equals(root2)} would be a more appropriate test.
     *
     * @param root1 first object graph (never null)
     * @param root2 second object graph (never null)
     */
    @Override
    public boolean isSameGraph(T root1, T root2) {
        return root1 == root2;
    }

    /**
     * Validate the given instance.
     *
     * <p>
     * The implementation in {@link AbstractDelegate} performs validation using {@link ValidationContext#validate()}.
     *
     * @throws IllegalArgumentException if {@code obj} is null
     * @return set of zero or more constraint violations
     */
    @Override
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
    @Override
    public void handleWritebackException(PersistentObject<T> pobj, Throwable t) {
        this.log.error(pobj + ": error during write-back", t);
    }

    /**
     * Get the default value for the root object graph. This method is invoked at startup when the persistent file does not exist.
     * If this method returns null, then an {@linkplain PersistentObject#isAllowEmptyStart empty start} occurs unless the
     * {@link PersistentObject} object is configured to disallow them, in which case an exception is thrown.
     *
     * <p>
     * The implementation in {@link AbstractDelegate} returns null
     *
     * @return root object initial value, or null if there is no default value
     */
    @Override
    public T getDefaultValue() {
        return null;
    }
}

