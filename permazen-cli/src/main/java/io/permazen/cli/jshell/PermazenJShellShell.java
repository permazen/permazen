
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.jshell;

import com.google.common.base.Preconditions;

import io.permazen.cli.PermazenShellRequest;

import org.dellroad.jct.core.ShellRequest;
import org.dellroad.jct.jshell.JShellShell;
import org.dellroad.jct.jshell.JShellShellSession;

public class PermazenJShellShell extends JShellShell {

    @Override
    public JShellShellSession newShellSession(ShellRequest request) {
        Preconditions.checkArgument(request instanceof PermazenShellRequest, "request is not a PermazenShellRequest");
        return new PermazenJShellShellSession(this, (PermazenShellRequest)request);
    }
}
