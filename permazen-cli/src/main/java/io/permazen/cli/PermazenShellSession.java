
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

import java.io.IOException;

import org.dellroad.jct.core.ShellRequest;
import org.dellroad.jct.core.ShellSession;
import org.dellroad.jct.core.simple.SimpleShell;
import org.jline.reader.LineReader;

/**
 * A {@link ShellSession} that with an associated Permazen database.
 */
public class PermazenShellSession extends SimpleShell.Session implements PermazenConsoleSession {

    protected Session session;

// Constructor

    public PermazenShellSession(PermazenShell shell, ShellRequest request, LineReader reader) throws IOException {
        super(shell, request, reader);
    }

// AbstractConsoleSession

    @Override
    public PermazenShell getOwner() {
        return (PermazenShell)super.getOwner();
    }

// SimpleShell.Session

    @Override
    public String getGreeting() {
        return String.format("Welcome to Permazen. You are in %s mode.", this.session.getMode());
    }

// PermazenConsoleSession

    @Override
    public Session getPermazenSession() {
        return this.session;
    }
    void setPermazenSession(Session session) {
        if (session == null)
            throw new IllegalArgumentException("null session");
        this.session = session;
    }
}
