
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import org.dellroad.stuff.spring.DelayedAction;
import org.springframework.scheduling.TaskScheduler;

/**
 * {@link DelayedAction} for actions that are associated with a Vaadin appliction.
 */
public abstract class VaadinDelayedAction extends DelayedAction {

    private final ContextApplication application;

    /**
     * Convenience constructor. Equivalent to:
     *  <blockquote><code>
     *  VaadinDelayedAction(taskScheduler, ContextApplication.get())
     *  </code></blockquote>
     */
    public VaadinDelayedAction(TaskScheduler taskScheduler) {
        this(taskScheduler, ContextApplication.get());
    }

    /**
     * Primary constructor.
     *
     * @param taskScheduler scheduler object
     * @param application Vaadin application with which the action is associated
     * @throws IllegalArgumentException if {@code application} is null
     */
    public VaadinDelayedAction(TaskScheduler taskScheduler, ContextApplication application) {
        super(application, taskScheduler);
        if (application == null)
            throw new IllegalArgumentException("null application");
        this.application = application;
    }

    /**
     * Invokes {@link #runInVaadin} in the context of the configured {@link Application}.
     */
    @Override
    public final void run() {
        this.application.invoke(new Runnable() {
            @Override
            public void run() {
                VaadinDelayedAction.this.runInVaadin();
            }
        });
    }

    /**
     * Perform the action. This method will be invoked in the context of the configured {@link Application}.
     */
    protected abstract void runInVaadin();
}

