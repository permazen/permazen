
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

/**
 * Generic interface supporting connection and disconnection.
 *
 * <p>
 * Intended for use by any class in a Vaadin session that connects/disconnects to/from non-Vaadin "back-end" resources,
 * typically {@link com.vaadin.data.Container}s. Some type of interface like this is required because for
 * {@link com.vaadin.data.Container}s that connect to back-end resources because there is no equivalent of
 * {@link com.vaadin.ui.Component#attach()}/{@link com.vaadin.ui.Component#detach()} like there is for widgets.
 * Typically, {@link #connect} and {@link #disconnect} (respectively) would be invoked by those methods.
 */
public interface Connectable {

    /**
     * Connect this instance to non-Vaadin resources.
     *
     * @throws IllegalStateException if there is no {@link com.vaadin.server.VaadinSession} associated with the current thread
     */
    void connect();

    /**
     * Disconnect this instance from non-Vaadin resources.
     *
     * @throws IllegalStateException if there is no {@link com.vaadin.server.VaadinSession} associated with the current thread
     */
    void disconnect();
}

