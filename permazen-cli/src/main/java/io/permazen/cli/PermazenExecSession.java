
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

import java.io.IOException;

import org.dellroad.jct.core.ExecRequest;
import org.dellroad.jct.core.simple.SimpleExec;

/**
 * A {@link SimpleExec.Session} with an associated Permazen database.
 */
public class PermazenExecSession extends SimpleExec.Session implements PermazenConsoleSession {

    protected Session session;

// Constructor

    public PermazenExecSession(PermazenExec exec, ExecRequest request, SimpleExec.FoundCommand command) throws IOException {
        super(exec, request, command);
    }

// AbstractConsoleSession

    @Override
    public PermazenExec getOwner() {
        return (PermazenExec)super.getOwner();
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
