
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.jshell;

import org.dellroad.jct.jshell.LocalContextExecutionControlProvider;
import org.dellroad.jct.jshell.MemoryLoaderDelegate;

public class PermazenExecutionControlProvider extends LocalContextExecutionControlProvider {

    public static final String NAME = "permazen";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    protected PermazenExecutionControl createLocalExecutionControl(MemoryLoaderDelegate delegate) {
        return new PermazenExecutionControl(delegate);
    }
}
