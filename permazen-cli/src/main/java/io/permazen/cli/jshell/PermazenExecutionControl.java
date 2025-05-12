
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.jshell;

import com.google.common.base.Preconditions;

import io.permazen.cli.Session;

import jdk.jshell.execution.LoaderDelegate;
import jdk.jshell.execution.LocalExecutionControl;

import org.dellroad.javabox.execution.LocalContextExecutionControl;

/**
 * Permazen-aware {@link LocalExecutionControl}.
 *
 * <p>
 * When snippets are evaluated, the Permazen {@link Session} obtained from {@link PermazenJShellShellSession#getPermazenSession}
 * is used to create a transaction that stays open for the duration of the snippet execution and the subsequent formatting
 * of the returned result. In effect each JShell command executes within its own transaction, and the evaluated expressions
 * may directly access and manipulate database objects.
 */
public class PermazenExecutionControl extends LocalContextExecutionControl {

    private final PermazenJShellShellSession session;

// Constructor

    /**
     * Constructor.
     *
     * @param delegate loader delegate
     */
    public PermazenExecutionControl(LoaderDelegate delegate) {
        super(delegate);
        this.session = PermazenJShellShellSession.getCurrent();
        Preconditions.checkState(this.session != null, "no session");
    }

// LocalContextExecutionControl

    @Override
    protected void enterContext() {
        this.session.joinTransaction();
    }

    @Override
    protected void leaveContext(Object returnValue, Throwable error) {
        this.session.leaveTransaction(error == null);
    }
}
