
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.server.VaadinSession;

import org.dellroad.stuff.spring.DelayedAction;
import org.springframework.scheduling.TaskScheduler;

/**
 * {@link DelayedAction} for actions that are associated with a Vaadin appliction.
 */
public abstract class VaadinDelayedAction extends DelayedAction {

    private final VaadinSession session;

    /**
     * Convenience constructor. Equivalent to:
     *  <blockquote><code>
     *  VaadinDelayedAction(taskScheduler, VaadinUtil.getCurrentSession())
     *  </code></blockquote>
     */
    public VaadinDelayedAction(TaskScheduler taskScheduler) {
        this(taskScheduler, VaadinUtil.getCurrentSession());
    }

    /**
     * Primary constructor.
     *
     * @param taskScheduler scheduler object
     * @param session the {@link VaadinSession} with which the action is associated
     * @throws NullPointerException if {@code session} is null
     */
    public VaadinDelayedAction(TaskScheduler taskScheduler, VaadinSession session) {
        super(session.getLockInstance(), taskScheduler);
        this.session = session;
    }

    /**
     * Invokes {@link #runInVaadin} in the context of the configured {@link VaadinSession}.
     */
    @Override
    public final void run() {
        VaadinUtil.invoke(this.session, new Runnable() {
            @Override
            public void run() {
                VaadinDelayedAction.this.runInVaadin();
            }
        });
    }

    /**
     * Perform the action. This method will be invoked in the context of the configured {@link VaadinSession}.
     */
    protected abstract void runInVaadin();
}

