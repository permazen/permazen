
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

import com.google.common.base.Preconditions;

import java.io.IOException;

import org.dellroad.jct.core.ShellRequest;
import org.dellroad.jct.core.ShellSession;
import org.dellroad.jct.core.simple.SimpleShell;
import org.jline.reader.LineReader;

/**
 * A {@link ShellSession} that with an associated Permazen database.
 */
public class PermazenShellSession extends SimpleShell.Session implements HasPermazenSession {

    protected Session session;

// Constructor

    public PermazenShellSession(PermazenShell shell, ShellRequest request, LineReader reader) throws IOException {
        super(shell, request, reader);
        // Maybe: this.session = ((PermazenShellRequest)request).getPermazenSession() ?
    }

// AbstractConsoleSession

    @Override
    public PermazenShell getOwner() {
        return (PermazenShell)super.getOwner();
    }

// SimpleShell.Session

    @Override
    public String getGreeting() {
        String greeting = "Welcome to Permazen.";
        final SessionMode mode = this.session.getMode();
        if (!mode.equals(SessionMode.PERMAZEN))
            greeting += String.format(" You are in %s mode.", mode);
        return String.format("%n%s%n", greeting);
    }

// HasPermazenSession

    @Override
    public Session getPermazenSession() {
        return this.session;
    }
    void setPermazenSession(Session session) {
        Preconditions.checkArgument(session != null, "null session");
        this.session = session;
    }
}
