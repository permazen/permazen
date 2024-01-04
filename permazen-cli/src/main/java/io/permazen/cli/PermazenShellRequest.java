
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

import org.dellroad.jct.core.simple.SimpleShellRequest;
import org.jline.terminal.Terminal;

/**
 * A {@link SimpleShellRequest} with an associated Permazen {@link Session}.
 */
public class PermazenShellRequest extends SimpleShellRequest implements HasPermazenSession {

    protected final Session session;

// Constructor

    public PermazenShellRequest(Session session, Terminal terminal, List<String> args, Map<String, String> env) {
        super(terminal, args, env);
        Preconditions.checkArgument(session != null, "null session");
        this.session = session;
    }

// HasPermazenSession

    @Override
    public Session getPermazenSession() {
        return this.session;
    }
}
