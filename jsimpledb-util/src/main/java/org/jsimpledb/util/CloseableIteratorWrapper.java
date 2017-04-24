
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import com.google.common.collect.ForwardingIterator;

import java.util.Iterator;

class CloseableIteratorWrapper<E> extends ForwardingIterator<E> implements CloseableIterator<E> {

    private final Iterator<E> iterator;
    private final AutoCloseable resource;

    CloseableIteratorWrapper(Iterator<E> iterator, AutoCloseable resource) {
        this.iterator = iterator;
        this.resource = resource;
    }

    @Override
    protected Iterator<E> delegate() {
        return this.iterator;
    }

    @Override
    public void close() {
        if (resource != null) {
            try {
                this.resource.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
