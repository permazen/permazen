
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.ui.Notification;
import com.vaadin.ui.Root;

import java.util.Collection;

/**
 * Vaadin 7 version of {@link org.dellroad.stuff.vaadin.ContextApplication}.
 *
 * @see org.dellroad.stuff.vaadin.ContextApplication
 */
@SuppressWarnings("serial")
public class ContextApplication extends org.dellroad.stuff.vaadin.ContextApplication {

    /**
     * Initialize the application. In Vaadin 7 overriding this method is optional.
     *
     * <p>
     * The implementation in {@link ContextApplication} does nothing.
     */
    @Override
    protected void initApplication() {
    }

// Error handling

    @Override
    public void showError(String title, String description) {
        ContextApplication.showError(this.getRoots(), this.getNotificationDelay(), title, description);
    }

    static void showError(Collection<Root> roots, int notificationDelay, String title, String description) {

        // Get window
        Root root = Root.getCurrentRoot();
        if (root == null) {
            if (roots.isEmpty())
                return;
            root = roots.iterator().next();
        }

        // Show error
        Notification notification = new Notification(title, description, Notification.TYPE_ERROR_MESSAGE);
        notification.setStyleName("warning");
        notification.setDelayMsec(notificationDelay);
        root.showNotification(notification);
    }
}

