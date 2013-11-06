
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.server.VaadinSession;

/**
 * Represents a session in the {@link VaadinSessionContainer}.
 */
public class VaadinSessionInfo {

    private final VaadinSession session;

    /**
     * Constructor.
     *
     * @throws IllegalStateException if there is no current {@link VaadinSession}
     */
    public VaadinSessionInfo() {
        this.session = VaadinUtil.getCurrentSession();
    }

    /**
     * Get the associated {@link VaadinSession}.
     */
    public VaadinSession getVaadinSession() {
        return this.session;
    }

    /**
     * Determine if the associated session is also the current session.
     */
    public boolean isCurrentSession() {
        return this.session == VaadinSession.getCurrent();
    }
}

