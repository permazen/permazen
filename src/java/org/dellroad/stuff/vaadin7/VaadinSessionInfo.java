
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.server.VaadinSession;

/**
 * Represents one {@link VaadinSession} in a {@link VaadinSessionContainer}.
 */
public class VaadinSessionInfo {

    /**
     * The {@link VaadinSession} associated with this instance.
     */
    protected final VaadinSession session;

    /**
     * Constructor.
     *
     * @throws IllegalStateException if there is no current {@link VaadinSession}
     */
    public VaadinSessionInfo() {
        this.session = VaadinUtil.getCurrentSession();
    }

    /**
     * Get the {@link VaadinSession} associated with this instance.
     */
    public VaadinSession getVaadinSession() {
        return this.session;
    }

    /**
     * Determine if the {@link VaadinSession} associated with this instance is also associated with the current thread.
     */
    public boolean isCurrentSession() {
        return this.session == VaadinSession.getCurrent();
    }

    /**
     * Update this instance with information from its corresponding {@link VaadinSession},
     * which will be associated with the current thread and locked.
     *
     * <p>
     * This method is invoked from {@link VaadinSessionContainer#doUpdate}; the implementation in
     * {@link VaadinSessionInfo} does nothing.
     * </p>
     *
     * <p>
     * When this method and {@link #makeUpdatesVisible} are overridden, consider declaring them both {@code synchronized},
     * as they are invoked with different locks held yet will access the same information.
     * </p>
     */
    protected void updateInformation() {
    }

    /**
     * Make the updates previously gathered by {@link #updateInformation} visible in this instance's {@link VaadinSessionContainer}.
     * The  {@link VaadinSession} associated with this instance's {@link VaadinSessionContainer} will be associated with the
     * current thread and locked.
     *
     * <p>
     * This method is invoked from {@link VaadinSessionContainer#doUpdate}; the implementation in
     * {@link VaadinSessionInfo} does nothing.
     * </p>
     *
     * <p>
     * When this method and {@link #updateInformation} are used, consider declaring them both {@code synchronized},
     * as they are invoked with different locks held yet will access the same information.
     * </p>
     */
    protected void makeUpdatesVisible() {
    }
}

