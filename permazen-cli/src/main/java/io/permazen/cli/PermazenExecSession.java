
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

import com.google.common.base.Preconditions;

import java.io.IOException;

import org.dellroad.jct.core.ExecRequest;
import org.dellroad.jct.core.simple.SimpleExec;

/**
 * A {@link SimpleExec.Session} with an associated Permazen database.
 */
public class PermazenExecSession extends SimpleExec.Session implements HasPermazenSession {

    protected Session session;

// Constructor

    public PermazenExecSession(PermazenExec exec, ExecRequest request, SimpleExec.FoundCommand command) throws IOException {
        super(exec, request, command);
        // Maybe: this.session = ((PermazenExecRequest)request).getPermazenSession() ?
    }

// AbstractConsoleSession

    @Override
    public PermazenExec getOwner() {
        return (PermazenExec)super.getOwner();
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
