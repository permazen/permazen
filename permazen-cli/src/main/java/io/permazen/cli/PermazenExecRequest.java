
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

import com.google.common.base.Preconditions;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import org.dellroad.jct.core.simple.SimpleExecRequest;

/**
 * A {@link SimpleExecRequest} with an associated Permazen {@link Session}.
 */
public class PermazenExecRequest extends SimpleExecRequest implements HasPermazenSession {

    protected final Session session;

// Constructors

    public PermazenExecRequest(Session session, InputStream in,
      PrintStream out, PrintStream err, Map<String, String> env, String command) {
        super(in, out, err, env, command);
        Preconditions.checkArgument(session != null, "null session");
        this.session = session;
    }

    public PermazenExecRequest(Session session, InputStream in,
      PrintStream out, PrintStream err, Map<String, String> env, List<String> command) {
        super(in, out, err, env, command);
        Preconditions.checkArgument(session != null, "null session");
        this.session = session;
    }

// HasPermazenSession

    @Override
    public Session getPermazenSession() {
        return this.session;
    }
}
