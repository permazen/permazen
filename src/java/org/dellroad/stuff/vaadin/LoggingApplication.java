
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import com.vaadin.Application;
import com.vaadin.terminal.Terminal;
import com.vaadin.ui.Window;

import java.net.SocketException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Application} subclass that provides subclasses with a {@link Logger}
 * and logs and displays any exceptions thrown in an overlay error window.
 *
 * @since 1.0.51
 */
@SuppressWarnings("serial")
public abstract class LoggingApplication extends Application {

    public static final int NOTIFICATION_DELAY = 30000;

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected LoggingApplication() {
    }

    @Override
    public void terminalError(Terminal.ErrorEvent event) {

        // Delegate to superclass
        super.terminalError(event);

        // Get exception; ignore client "hangups"
        final Throwable t = event.getThrowable();
        if (t instanceof SocketException)
            return;

        // Notify user
        this.showError("Internal Error", "" + t);
    }

    /**
     * Display an error message to the user.
     */
    public void showError(String title, String description) {
        Window.Notification notification = new Window.Notification(title, description, Window.Notification.TYPE_ERROR_MESSAGE);
        notification.setStyleName("warning");
        notification.setDelayMsec(NOTIFICATION_DELAY);
        this.getMainWindow().showNotification(notification);
    }

    /**
     * Display an error message to the user caused by an exception.
     */
    public void showError(String title, Throwable t) {
        for (int i = 0; i < 100 && t.getCause() != null; i++)
            t = t.getCause();
        this.showError(title, t.getClass().getSimpleName() + ": " + t.getMessage());
    }
}

