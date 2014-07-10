
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import java.util.List;

import org.dellroad.stuff.vaadin7.BackedItem;
import org.dellroad.stuff.vaadin7.SimpleKeyedContainer;

/**
 * Vaadin {@link com.vaadin.event.Action.Handler} implementation that builds a list
 * of {@link Action}s using a configured {@link ActionListBuilder}.
 */
@SuppressWarnings("serial")
public class DefaultActionHandler<T> implements com.vaadin.event.Action.Handler {

    private final SimpleKeyedContainer<?, T> container;
    private final ActionListBuilder<T> actionListBuilder;

    public DefaultActionHandler(SimpleKeyedContainer<?, T> container, ActionListBuilder<T> actionListBuilder) {
        if (container == null)
            throw new IllegalArgumentException("null container");
        if (actionListBuilder == null)
            throw new IllegalArgumentException("null actionListBuilder");
        this.container = container;
        this.actionListBuilder = actionListBuilder;
    }

    /**
     * Get the backing object from the target provided by the framework.
     */
    protected T getTarget(Object object) {
        final BackedItem<T> item = this.container.getItem(object);
        return item != null ? item.getObject() : null;
    }

// Action.Handler

    @Override
    public Action[] getActions(Object guiTarget, Object sender) {
        final List<? extends Action> list = this.actionListBuilder.buildActionList(this.getTarget(guiTarget));
        return list == null || list.isEmpty() ? new Action[0] : list.toArray(new Action[list.size()]);
    }

    @Override
    public void handleAction(com.vaadin.event.Action action, Object target, Object sender) {
        ((Action)action).execute();
    }
}

