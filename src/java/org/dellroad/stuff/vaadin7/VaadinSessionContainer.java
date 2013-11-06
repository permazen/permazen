
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.server.SessionDestroyEvent;
import com.vaadin.server.SessionDestroyListener;
import com.vaadin.server.SessionInitEvent;
import com.vaadin.server.SessionInitListener;
import com.vaadin.server.VaadinService;
import com.vaadin.server.VaadinServlet;
import com.vaadin.server.VaadinServletService;
import com.vaadin.server.VaadinSession;

import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container containing active {@link VaadinSession}s. This class is useful when you need to display
 * sessions in your GUI, for example, a table showing all logged-in users.
 *
 * <p>
 * By subclassing this class and {@link VaadinSessionInfo}, additional session-related properties,
 * such as logged-in user, can be added to the container by annotating the {@link VaadinSessionInfo}
 * subclass with {@link ProvidesProperty &#64;ProvidesProperty} annotations.
 * </p>
 *
 * <p>
 * Note the following:
 * <ul>
 *  <li>The {@link SpringVaadinServlet} must be used and configured with init parameter
 *      {@link SpringVaadinServlet#SESSION_TRACKING_PARAMETER} set to {@code true}.</li>
 *  <li>The {@link #connect} and {@link #disconnect} methods must be invoked before and after (respectively)
 *      this container is used; typically these would be invoked in the {@link com.vaadin.ui.Component#attach}
 *      and {@link com.vaadin.ui.Component#detach} methods of a corresponding widget.
 * </ul>
 * </p>
 *
 * @param <T> the type of the Java objects that back each {@link com.vaadin.data.Item} in the container
 */
@SuppressWarnings("serial")
public abstract class VaadinSessionContainer<T extends VaadinSessionInfo> extends SimpleKeyedContainer<VaadinSession, T> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final SessionEventListener listener = new SessionEventListener();
    private final VaadinSession session;

    /**
     * Constructor.
     *
     * <p>
     * Properties will be determined by the {@link ProvidesProperty &#64;ProvidesProperty} and
     * {@link ProvidesPropertySort &#64;ProvidesPropertySort} annotated methods in the given class.
     * </p>
     *
     * @param type class to introspect for annotated methods
     * @throws IllegalArgumentException if {@code type} is null
     * @throws IllegalArgumentException if {@code type} has two {@link ProvidesProperty &#64;ProvidesProperty}
     *  or {@link ProvidesPropertySort &#64;ProvidesPropertySort} annotated methods for the same property
     * @throws IllegalArgumentException if a {@link ProvidesProperty &#64;ProvidesProperty}-annotated method with no
     *  {@linkplain ProvidesProperty#value property name specified} has a name which cannot be interpreted as a bean
     *  property "getter" method
     * @throws IllegalStateException if there is no {@link VaadinSession} associated with the current thread
     * @throws IllegalStateException if there is no {@link SpringVaadinServlet} associated
     *  with the current thread's {@link VaadinSession}
     * @see ProvidesProperty
     * @see ProvidesPropertySort
     * @see ProvidesPropertyScanner
     */
    protected VaadinSessionContainer(Class<T> type) {
        super(type);
        this.session = VaadinUtil.getCurrentSession();
        this.getSpringVaadinServlet();
    }

    /**
     * Constructor.
     *
     * @param propertyExtractor used to extract properties from the underlying Java objects;
     *  may be null but then container is not usable until one is configured via
     * {@link #setPropertyExtractor setPropertyExtractor()}
     * @param propertyDefs container property definitions; null is treated like the empty set
     */
    protected VaadinSessionContainer(PropertyExtractor<? super T> propertyExtractor,
      Collection<? extends PropertyDef<?>> propertyDefs) {
        super(propertyExtractor, propertyDefs);
        this.session = VaadinUtil.getCurrentSession();
        this.getSpringVaadinServlet();
    }

    @Override
    protected VaadinSession getKeyFor(T info) {
        return info.getVaadinSession();
    }

    /**
     * Start tracking live sessions.
     *
     */
    public void connect() {
        this.listener.register();
    }

    /**
     * Stop tracking live sessions.
     */
    public void disconnect() {
        this.listener.unregister();
    }

    /**
     * Update this container after a {@link VaadinSession} has been added or removed.
     * This must <b>not</b> be invoked while any {@link VaadinSession} is locked.
     */
    protected void update() {

        // Sanity check
        if (VaadinSession.getCurrent() != null && VaadinSession.getCurrent().hasLock())
            throw new IllegalStateException("inside locked VaadinSession");

        // Create a VaadinSessionInfo object for each session, but doing so while that session is locked
        final ArrayList<T> sessionInfoList = new ArrayList<T>();
        for (VaadinSession otherSession : this.getSpringVaadinServlet().getSessions()) {
            otherSession.accessSynchronously(new Runnable() {
                @Override
                public void run() {
                    sessionInfoList.add(VaadinSessionContainer.this.createVaadinSessionInfo());
                }
            });
        }

        // Reload this container
        this.session.access(new Runnable() {
            @Override
            public void run() {
                VaadinSessionContainer.this.load(sessionInfoList);
            }
        });
    }

    protected SpringVaadinServlet getSpringVaadinServlet() {
        final VaadinService service = this.session.getService();
        if (!(service instanceof VaadinServletService))
            throw new IllegalStateException("there is no SpringVaadinServlet associated with this VaadinSession");
        final VaadinServlet servlet = ((VaadinServletService)service).getServlet();
        if (!(servlet instanceof SpringVaadinServlet))
            throw new IllegalStateException("there is no SpringVaadinServlet associated with this VaadinSession");
        return (SpringVaadinServlet)servlet;
    }

    /**
     * Create a {@link VaadinSessionInfo} backing object for the {@link VaadinSession} associated with the current thread.
     * The {@link VaadinSession} will be already locked.
     */
    protected abstract T createVaadinSessionInfo();

// SessionEventListener

    private class SessionEventListener extends VaadinExternalListener<VaadinService>
      implements SessionInitListener, SessionDestroyListener {

        public SessionEventListener() {
            super(VaadinSession.getCurrent().getService());
        }

        @Override
        public void sessionInit(SessionInitEvent event) {
            this.update();
        }

        @Override
        public void sessionDestroy(SessionDestroyEvent event) {
            this.update();
        }

        private void update() {
            new Thread("VaadinSessionContainerUpdate") {
                @Override
                public void run() {
                    try {
                        VaadinSessionContainer.this.update();
                    } catch (ThreadDeath t) {
                        throw t;
                    } catch (Throwable t) {
                        VaadinSessionContainer.this.log.error("error updating container " + this, t);
                    }
                }
            }.start();
        }

        @Override
        protected void register(VaadinService vaadinService) {
            vaadinService.addSessionInitListener(this);
            vaadinService.addSessionDestroyListener(this);
        }

        @Override
        protected void unregister(VaadinService vaadinService) {
            vaadinService.removeSessionInitListener(this);
            vaadinService.removeSessionDestroyListener(this);
        }
    }
}

