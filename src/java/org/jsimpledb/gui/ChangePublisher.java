
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import org.dellroad.stuff.vaadin7.VaadinConfigurable;
import org.jsimpledb.JObject;
import org.jsimpledb.JTransaction;
import org.jsimpledb.change.Change;
import org.jsimpledb.change.SimpleFieldChange;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.stereotype.Component;

/**
 * Publishes change notifications.
 */
@Component
@VaadinConfigurable
public class ChangePublisher {

    @Autowired
    @Qualifier("jsimpledbGuiEventMulticaster")
    private ApplicationEventMulticaster eventMulticaster;

    /**
     * Publish the given {@link Change} if the current transaction is successful.
     */
    public void publishChangeOnCommit(Change<?> change) {
        JTransaction.getCurrent().getTransaction().addCallback(
          new PublishChangeCallback(this.eventMulticaster, new DataChangeEvent(this, change)));
    }

    /**
     * Publish that the specified object has changed if the current transaction is successful.
     */
    public void publishChangeOnCommit(JObject jobj) {

        // We rely here on the fact that publishing a change in any property reloads all properties for that object
        this.publishChangeOnCommit(new SimpleFieldChange<Object, ObjId>(jobj,
          Integer.MAX_VALUE, ObjectContainer.OBJECT_ID_PROPERTY, jobj.getObjId(), jobj.getObjId()));
    }

// PublishChangeCallback - notifies the rest of the application when a data instance has been added/removed/changed

    private static class PublishChangeCallback extends Transaction.CallbackAdapter {

        private final ApplicationEventMulticaster eventMulticaster;
        private final DataChangeEvent event;

        PublishChangeCallback(ApplicationEventMulticaster eventMulticaster, DataChangeEvent event) {
            if (eventMulticaster == null)
                throw new IllegalArgumentException("null eventMulticaster");
            if (event == null)
                throw new IllegalArgumentException("null event");
            this.eventMulticaster = eventMulticaster;
            this.event = event;
        }

        @Override
        public void afterCommit() {
            this.eventMulticaster.multicastEvent(this.event);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final PublishChangeCallback that = (PublishChangeCallback)obj;
            return this.event.equals(that.event);
        }

        @Override
        public int hashCode() {
            return this.event.hashCode();
        }
    }
}

