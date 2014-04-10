
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer.change;

/**
 * Notification object that gets passed to {@link org.jsimpledb.annotation.OnChange &#64;OnChange}-annotated methods
 * when an element is added to a set field.
 *
 * @param <T> the type of the object containing the changed field
 * @param <E> the type of the changed set's elements
 */
public class SetFieldAdd<T, E> extends SetFieldChange<T, E> {

    private final E element;

    /**
     * Constructor.
     *
     * @param jobj Java object containing the set field that changed
     * @param element the element that was added
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public SetFieldAdd(T jobj, E element) {
        super(jobj);
        this.element = element;
    }

    /**
     * Get the element that was added to the set.
     */
    public E getElement() {
        return this.element;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final SetFieldAdd<?, ?> that = (SetFieldAdd<?, ?>)obj;
        return this.element != null ? this.element.equals(that.element) : that.element == null;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ (this.element != null ? this.element.hashCode() : 0);
    }

    @Override
    public String toString() {
        return "SetFieldAdd[object=" + this.getObject() + ",element=" + this.element + "]";
    }
}

