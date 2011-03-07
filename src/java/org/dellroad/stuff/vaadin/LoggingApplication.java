
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import com.vaadin.Application;
import com.vaadin.terminal.ErrorMessage;
import com.vaadin.terminal.ParameterHandler;
import com.vaadin.terminal.SystemError;
import com.vaadin.terminal.Terminal;
import com.vaadin.terminal.URIHandler;
import com.vaadin.terminal.VariableOwner;
import com.vaadin.terminal.gwt.server.ChangeVariablesErrorEvent;
import com.vaadin.ui.AbstractComponent;
import com.vaadin.ui.Window;

import java.net.SocketException;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

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

        // Get exception
        final Throwable t = event.getThrowable();

        // Ignore client "hangups"
        if (t instanceof SocketException) {
            HttpServletRequest request = getCurrentRequest();
            log.warn("socket exception talking to " + request.getRemoteAddr() + ":" + request.getRemotePort() + ": " + t);
            return;
        }

        // Log it
        log.error("internal error in " + getClass().getSimpleName() + ": " + event, t);

        // Note: the following code was copied from Application.java...

        // Finds the original source of the error/exception
        Object owner = null;
        if (event instanceof VariableOwner.ErrorEvent)
            owner = ((VariableOwner.ErrorEvent)event).getVariableOwner();
        else if (event instanceof URIHandler.ErrorEvent)
            owner = ((URIHandler.ErrorEvent)event).getURIHandler();
        else if (event instanceof ParameterHandler.ErrorEvent)
            owner = ((ParameterHandler.ErrorEvent)event).getParameterHandler();
        else if (event instanceof ChangeVariablesErrorEvent)
            owner = ((ChangeVariablesErrorEvent)event).getComponent();

        // Shows the error in AbstractComponent
        if (owner instanceof AbstractComponent) {
            AbstractComponent component = (AbstractComponent)owner;
            if (t instanceof ErrorMessage)
                component.setComponentError((ErrorMessage)t);
            else
                component.setComponentError(new SystemError(t));
        }

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

    public HttpServletRequest getCurrentRequest() {
        RequestAttributes requestAttributes = RequestContextHolder.currentRequestAttributes();
        return (HttpServletRequest)requestAttributes.getAttribute(
          RequestAttributes.REFERENCE_REQUEST, RequestAttributes.SCOPE_REQUEST);
    }
}

