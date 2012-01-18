
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
 * Adapter class for {@link PersistentObjectDelegate} implementations that wrap a nested delegate.
 * All methods in this class forward to the nested delegate.
 *
 * @param <T> type of the root persistent object
 */
public class FilterDelegate<T> implements PersistentObjectDelegate<T> {

    protected final PersistentObjectDelegate<T> nested;

    /**
     * Constructor.
     *
     * @param nested nested delegate to wrap
     * @throws IllegalArgumentException if {@code nested} is null
     */
    public FilterDelegate(PersistentObjectDelegate<T> nested) {
        if (nested == null)
            throw new IllegalArgumentException("null nested");
        this.nested = nested;
    }

    @Override
    public void serialize(T obj, Result result) throws IOException {
        this.nested.serialize(obj, result);
    }

    @Override
    public T deserialize(Source source) throws IOException {
        return this.nested.deserialize(source);
    }

    @Override
    public T copy(T original) {
        return this.nested.copy(original);
    }

    @Override
    public Set<ConstraintViolation<T>> validate(T obj) {
        return this.nested.validate(obj);
    }

    @Override
    public void handleWritebackException(PersistentObject<T> pobj, Throwable t) {
        this.nested.handleWritebackException(pobj, t);
    }
}

