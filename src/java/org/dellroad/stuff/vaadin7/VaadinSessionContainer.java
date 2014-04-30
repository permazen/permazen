
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
import com.vaadin.server.VaadinSession;

import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container containing active {@link VaadinSession}s.
 *
 * <p>
 * This class is useful when you need to display sessions in your GUI, for example, a table showing all logged-in users.
 * Due to the fact that each {@link VaadinSession} has its own lock, building such a container without race conditions
 * and deadlocks is somewhat tricky. This class performs the locking required during updates and provides thread-safe
 * {@link #update} and {@link #reload} methods. Information about active {@link VaadinSession}s comes from the session
 * tracking feature of the {@link SpringVaadinServlet}.
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
 *  <li>This class only works with configurations where Vaadin sessions are stored in memory,
 *      and the {@link SpringVaadinServlet} must be used and configured with init parameter
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
public abstract class VaadinSessionContainer<T extends VaadinSessionInfo> extends SelfKeyedContainer<T> {

    /**
     * The {@link VaadinSession} that this container instance is associated with.
     * Determined by whatever {@link VaadinSession} is associated with the current thread at construction time.
     */
    protected final VaadinSession session;

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final SessionEventListener listener = new SessionEventListener();

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
        SpringVaadinServlet.getServlet(this.session);
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
        SpringVaadinServlet.getServlet(this.session);
    }

    /**
     * Connect this container and start tracking sessions.
     */
    @Override
    public void connect() {
        super.connect();
        this.listener.register();
        this.reload();
    }

    /**
     * Disconnect this container and stop tracking sessions.
     */
    @Override
    public void disconnect() {
        this.listener.unregister();
        super.disconnect();
    }

    /**
     * Asynchronously reload this container.
     *
     * <p>
     * This method can be invoked from any thread. It creates a new thread to do the actual
     * reloading via {@link #doReload} to avoid potential deadlocks.
     * </p>
     */
    public void reload() {
        new Thread("VaadinSessionContainer.reload()") {
            @Override
            public void run() {
                try {
                    VaadinSessionContainer.this.doReload();
                } catch (ThreadDeath t) {
                    throw t;
                } catch (Throwable t) {
                    VaadinSessionContainer.this.log.error("error reloading container " + this, t);
                }
            }
        }.start();
    }

    /**
     * Asynchronously update this container's items.
     *
     * <p>
     * This method can be invoked from any thread. It creates a new thread to do the actual
     * updating via {@link #doUpdate} to avoid potential deadlocks.
     * </p>
     */
    public void update() {
        new Thread("VaadinSessionContainer.update()") {
            @Override
            public void run() {
                try {
                    VaadinSessionContainer.this.doUpdate();
                } catch (ThreadDeath t) {
                    throw t;
                } catch (Throwable t) {
                    VaadinSessionContainer.this.log.error("error updating container " + this, t);
                }
            }
        }.start();
    }

    /**
     * Update each {@link VaadinSessionInfo} instance in this container.
     * Using this method is more efficient than reloading the entire container.
     *
     * <p>
     * This method handles the complicated locking required to avoid deadlocks: first, for each {@link VaadinSessionInfo},
     * {@link VaadinSessionInfo#updateInformation} is invoked while the {@link VaadinSession}
     * corresponding to <i>that {@link VaadinSessionInfo} instance</i> is locked, so that information from that session
     * can be safely gathered; then, {@link VaadinSessionInfo#makeUpdatesVisible} is invoked while the {@link VaadinSession}
     * associated with <i>this container</i> is locked, so item properties can be safely updated, etc.
     * </p>
     *
     * <p>
     * This method must <b>not</b> be invoked while any {@link VaadinSession} is locked.
     * For example, it may be invoked by a regular timer (only while this container is {@linkplain #connect connected}).
     * </p>
     *
     * @throws IllegalStateException if there is a locked {@link VaadinSession} associated with the current thread
     */
    protected void doUpdate() {

        // Sanity check
        if (VaadinSession.getCurrent() != null && VaadinSession.getCurrent().hasLock())
            throw new IllegalStateException("inside locked VaadinSession");

        // Snapshot the set of sessions in this container while holding the lock to this container's session
        final ArrayList<T> sessionInfoList = new ArrayList<>();
        VaadinSessionContainer.this.session.accessSynchronously(new Runnable() {
            @Override
            public void run() {
                sessionInfoList.addAll(VaadinSessionContainer.this.getItemIds());
            }
        });

        // Update each session's information while holding that session's lock
        for (T sessionInfo : sessionInfoList) {
            final T sessionInfo2 = sessionInfo;
            sessionInfo.getVaadinSession().accessSynchronously(new Runnable() {
                @Override
                public void run() {
                    sessionInfo2.updateInformation();
                }
            });
        }

        // Now update this container with the newly gathered information while again holding the lock to this container's session
        VaadinSessionContainer.this.session.accessSynchronously(new Runnable() {
            @Override
            public void run() {
                for (T sessionInfo : sessionInfoList)
                    sessionInfo.makeUpdatesVisible();
            }
        });
    }

    /**
     * Reload this container. Reloads this container with {@link VaadinSessionInfo} instances for each {@link VaadinSession}
     * (created via {@link #createVaadinSessionInfo}), and then invokes {@link #doUpdate}.
     *
     * <p>
     * This method must <b>not</b> be invoked while any {@link VaadinSession} is locked.
     * </p>
     *
     * @throws IllegalStateException if there is a locked {@link VaadinSession} associated with the current thread
     */
    protected void doReload() {

        // Sanity check
        if (VaadinSession.getCurrent() != null && VaadinSession.getCurrent().hasLock())
            throw new IllegalStateException("inside locked VaadinSession");

        // Create a VaadinSessionInfo object for each session, but doing so while that session is locked
        final ArrayList<T> sessionInfoList = new ArrayList<>();
        for (VaadinSession otherSession : SpringVaadinServlet.getServlet(this.session).getSessions()) {
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

        // Update this container
        this.doUpdate();
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
            super(VaadinUtil.getCurrentSession().getService());
        }

        @Override
        public void sessionInit(SessionInitEvent event) {
            VaadinSessionContainer.this.reload();
        }

        @Override
        public void sessionDestroy(SessionDestroyEvent event) {
            VaadinSessionContainer.this.reload();
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

