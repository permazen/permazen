
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.jshell;

import com.google.common.base.Preconditions;

import io.permazen.cli.PermazenShellRequest;
import io.permazen.cli.PermazenShellSession;

import java.util.List;

import org.dellroad.jct.core.ShellSession;
import org.dellroad.jct.jshell.command.JShellCommand;

public class PermazenJShellCommand extends JShellCommand {

    /**
     * Constructor.
     */
    public PermazenJShellCommand() {
        this(new PermazenJShellShell());
    }

    /**
     * Constructor.
     *
     * @param shell command shell
     */
    public PermazenJShellCommand(PermazenJShellShell shell) {
        super(shell);
    }

    @Override
    protected PermazenShellRequest buildShellRequest(ShellSession session, String name, List<String> params) {
        Preconditions.checkArgument(session instanceof PermazenShellSession, "session is not a PermazenShellSession");
        return this.buildShellRequest((PermazenShellSession)session, name, params);
    }

    protected PermazenShellRequest buildShellRequest(PermazenShellSession session, String name, List<String> params) {
        return new PermazenShellRequest(session.getPermazenSession(),
          session.getRequest().getTerminal(), params, session.getRequest().getEnvironment());
    }
}
