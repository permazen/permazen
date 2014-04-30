
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.data.Property;

import java.util.EventObject;

/**
 * Support superclass for {@link Property}s with read-only values.
 * Provides a somewhat more space-efficient implementation than Vaadin's {@link com.vaadin.data.util.AbstractProperty},
 * which is useful when there are many instances in memory.
 *
 * @param <V> the type of the property
 */
@SuppressWarnings("serial")
public abstract class ReadOnlyProperty<V> implements
  Property<V>, Property.ValueChangeNotifier, Property.ReadOnlyStatusChangeNotifier {

    private Object listeners;                   // space efficient listener hackery

    /**
     * Change this instance's value.
     *
     * <p>
     * The implementation in {@link ReadOnlyProperty} always throws {@link com.vaadin.data.Property.ReadOnlyException}.
     * </p>
     */
    @Override
    public void setValue(V value) {
        throw new Property.ReadOnlyException();
    }

    /**
     * Determine if this instance is read-only.
     *
     * <p>
     * The implementation in {@link ReadOnlyProperty} always returns true.
     * </p>
     */
    @Override
    public boolean isReadOnly() {
        return true;
    }

    /**
     * Change this instance's read-only setting.
     *
     * <p>
     * The implementation in {@link ReadOnlyProperty} throws {@link UnsupportedOperationException}
     * if {@code readOnly} is false.
     * </p>
     */
    @Override
    public void setReadOnly(boolean readOnly) {
        if (!readOnly)
            throw new UnsupportedOperationException();
    }

    /**
     * Issue a {@link com.vaadin.data.Property.ValueChangeEvent} to all registered
     * {@link com.vaadin.data.Property.ValueChangeListener}s.
     */
    protected void fireValueChange() {
        if (this.listeners == null)
            return;
        final Property.ValueChangeEvent event = new ValueChangeEvent();
        if (this.listeners instanceof Property.ValueChangeListener)
            ((Property.ValueChangeListener)this.listeners).valueChange(event);
        else {
            for (Property.ValueChangeListener listener : (Property.ValueChangeListener[])this.listeners)
                listener.valueChange(event);
        }
    }

    /**
     * Invoked when the first {@link com.vaadin.data.Property.ValueChangeListener} has been added.
     *
     * <p>
     * The implementation in {@link ReadOnlyProperty} does nothing.
     * </p>
     */
    protected void firstListenerAdded() {
    }

    /**
     * Invoked when the last {@link com.vaadin.data.Property.ValueChangeListener} has been removed.
     *
     * <p>
     * The implementation in {@link ReadOnlyProperty} does nothing.
     * </p>
     */
    protected void lastListenerRemoved() {
    }

// Property.ValueChangeNotifier

    @Override
    public void addValueChangeListener(Property.ValueChangeListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("null listener");
        if (this.listeners == null) {
            this.listeners = listener;
            this.firstListenerAdded();
        } else if (this.listeners instanceof Property.ValueChangeListener)
            this.listeners = new Property.ValueChangeListener[] { (Property.ValueChangeListener)this.listeners, listener };
        else {
            final Property.ValueChangeListener[] oldArray = (Property.ValueChangeListener[])this.listeners;
            final Property.ValueChangeListener[] newArray = new Property.ValueChangeListener[oldArray.length + 1];
            System.arraycopy(oldArray, 0, newArray, 0, oldArray.length);
            newArray[oldArray.length] = listener;
            this.listeners = newArray;
        }
    }

    @Override
    public void removeValueChangeListener(Property.ValueChangeListener listener) {
        if (listener == null || this.listeners == null)
            return;
        if (this.listeners instanceof Property.ValueChangeListener) {
            if (!this.listeners.equals(listener))
                return;
            this.listeners = null;
            this.lastListenerRemoved();
            return;
        }
        final Property.ValueChangeListener[] oldArray = (Property.ValueChangeListener[])this.listeners;
        int index = -1;
        for (int i = 0; i < oldArray.length; i++) {
            if (oldArray[i].equals(listener)) {
                index = i;
                break;
            }
        }
        if (index == -1)
            return;
        switch (oldArray.length) {
        case 0:
        case 1:
            throw new RuntimeException("internal error");
        case 2:
            this.listeners = oldArray[1 - index];
            break;
        default:
            final Property.ValueChangeListener[] newArray = new Property.ValueChangeListener[oldArray.length - 1];
            System.arraycopy(oldArray, 0, newArray, 0, index);
            System.arraycopy(oldArray, index + 1, newArray, index, oldArray.length - index - 1);
            this.listeners = newArray;
            break;
        }
    }

    @Override
    @SuppressWarnings("deprecated")
    public void addListener(Property.ValueChangeListener listener) {
        this.addValueChangeListener(listener);
    }

    @Override
    @SuppressWarnings("deprecated")
    public void removeListener(Property.ValueChangeListener listener) {
        this.removeValueChangeListener(listener);
    }

// Property.ReadOnlyStatusChangeNotifier

    @Override
    public void addReadOnlyStatusChangeListener(Property.ReadOnlyStatusChangeListener listener) {
        // nothing to do - read-only status never changes
    }

    @Override
    public void removeReadOnlyStatusChangeListener(Property.ReadOnlyStatusChangeListener listener) {
        // nothing to do - read-only status never changes
    }

    @Override
    @SuppressWarnings("deprecated")
    public void addListener(Property.ReadOnlyStatusChangeListener listener) {
        this.addReadOnlyStatusChangeListener(listener);
    }

    @Override
    @SuppressWarnings("deprecated")
    public void removeListener(Property.ReadOnlyStatusChangeListener listener) {
        this.removeReadOnlyStatusChangeListener(listener);
    }

// ValueChangeEvent

    private class ValueChangeEvent extends EventObject implements Property.ValueChangeEvent {

        ValueChangeEvent() {
            super(ReadOnlyProperty.this);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Property/*<?>*/ getProperty() {
            return (Property)this.getSource();
        }
    }
}

