
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.collect.ForwardingIterator;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

class CloseableIteratorWrapper<E> extends ForwardingIterator<E> implements CloseableIterator<E> {

    private final Iterator<E> iterator;
    private final AutoCloseable resource;
    private final AtomicBoolean closed = new AtomicBoolean();

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
        if (this.closed.compareAndSet(false, true) && resource != null) {
            try {
                this.resource.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
