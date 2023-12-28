
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.change;

import com.google.common.base.Preconditions;

import io.permazen.JObject;
import io.permazen.JTransaction;
import io.permazen.annotation.OnChange;

import java.util.Objects;

/**
 * Notification object that gets passed to {@link OnChange &#64;OnChange}-annotated methods
 * when an element is removed from a set field.
 *
 * @param <T> the type of the object containing the changed field
 * @param <E> the type of the changed set's elements
 */
public class SetFieldRemove<T, E> extends SetFieldChange<T> {

    private final E element;

    /**
     * Constructor.
     *
     * @param jobj Java object containing the set field that changed
     * @param fieldName the name of the field that changed
     * @param element the element that was removed
     * @throws IllegalArgumentException if {@code jobj} or {@code fieldName} is null
     */
    public SetFieldRemove(T jobj, String fieldName, E element) {
        super(jobj, fieldName);
        this.element = element;
    }

    @Override
    public <R> R visit(ChangeSwitch<R> target) {
        return target.caseSetFieldRemove(this);
    }

    @Override
    public void apply(JTransaction jtx, JObject jobj) {
        Preconditions.checkArgument(jtx != null, "null jtx");
        Preconditions.checkArgument(jobj != null, "null jobj");
        jtx.readSetField(jobj.getObjId(), this.getFieldName(), false).remove(this.element);
    }

    /**
     * Get the element that was removed from the set.
     *
     * @return the value removed from the set
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
        final SetFieldRemove<?, ?> that = (SetFieldRemove<?, ?>)obj;
        return Objects.equals(this.element, that.element);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(this.element);
    }

    @Override
    public String toString() {
        return "SetFieldRemove[object=" + this.getObject() + ",field=\"" + this.getFieldName() + "\",element=" + this.element + "]";
    }
}
