
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.change;

import com.google.common.base.Preconditions;

import io.permazen.PermazenObject;
import io.permazen.PermazenTransaction;
import io.permazen.annotation.OnChange;

import java.util.Objects;
import java.util.Set;

/**
 * Notification object that gets passed to {@link OnChange &#64;OnChange}-annotated methods
 * when an element is added to a set field.
 *
 * @param <T> the type of the object containing the changed field
 * @param <E> the type of the changed set's elements
 */
public class SetFieldAdd<T, E> extends SetFieldChange<T> {

    private final E element;

    /**
     * Constructor.
     *
     * @param jobj Java object containing the set field that changed
     * @param fieldName the name of the field that changed
     * @param element the element that was added
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    public SetFieldAdd(T jobj, String fieldName, E element) {
        super(jobj, fieldName);
        this.element = element;
    }

    @Override
    public <R> R visit(ChangeSwitch<R> target) {
        return target.caseSetFieldAdd(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void apply(PermazenTransaction jtx, PermazenObject jobj) {
        Preconditions.checkArgument(jtx != null, "null jtx");
        Preconditions.checkArgument(jobj != null, "null jobj");
        ((Set<E>)jtx.readSetField(jobj.getObjId(), this.getFieldName(), false)).add(this.element);
    }

    /**
     * Get the element that was added to the set.
     *
     * @return the newly added value
     */
    public E getElement() {
        return this.element;
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final SetFieldAdd<?, ?> that = (SetFieldAdd<?, ?>)obj;
        return Objects.equals(this.element, that.element);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(this.element);
    }

    @Override
    public String toString() {
        return "SetFieldAdd[object=" + this.getObject() + ",field=\"" + this.getFieldName() + "\",element=" + this.element + "]";
    }
}
