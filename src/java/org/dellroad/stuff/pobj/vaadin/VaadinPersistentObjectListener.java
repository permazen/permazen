
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj.vaadin;

import org.dellroad.stuff.pobj.PersistentObject;
import org.dellroad.stuff.pobj.PersistentObjectEvent;
import org.dellroad.stuff.pobj.PersistentObjectListener;
import org.dellroad.stuff.vaadin7.VaadinExternalListener;

/**
 * Specialization of {@link VaadinExternalListener} customized for listening to {@link PersistentObject}s.
 *
 * @param <T> type of the root persistent object
 */
public abstract class VaadinPersistentObjectListener<T> extends VaadinExternalListener<PersistentObject<T>>
  implements PersistentObjectListener<T> {

    public VaadinPersistentObjectListener(PersistentObject<T> pobj) {
        super(pobj);
    }

    @Override
    public final void handleEvent(final PersistentObjectEvent<T> event) {
        this.handleEvent(new Runnable() {
            @Override
            public void run() {
                VaadinPersistentObjectListener.this.handlePersistentObjectChange(
                  event.getOldRoot(), event.getNewRoot(), event.getVersion());
            }
        });
    }

    /**
     * Handle an update of the {@link PersistentObject} root object.
     * The current thread will already have the Vaadin session locked, so it
     * <i>should not invoke {@link #handleEvent handleEvent()}</i> to handle the event.
     *
     * <p>
     * The caller should not modify {@code oldRoot} or {@code newRoot}.
     * </p>
     *
     * @param oldRoot previous root object; may be null if empty starts or stops are supported
     * @param newRoot new root object; may be null if empty starts or stops are supported
     * @param version version corresponding to {@code newRoot}
     */
    protected abstract void handlePersistentObjectChange(T oldRoot, T newRoot, long version);

    @Override
    protected final void register(PersistentObject<T> pobj) {
        pobj.addListener(this);
    }

    @Override
    protected final void unregister(PersistentObject<T> pobj) {
        pobj.removeListener(this);
    }
}

