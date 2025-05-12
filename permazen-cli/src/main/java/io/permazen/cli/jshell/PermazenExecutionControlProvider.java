
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.jshell;

import io.permazen.util.ApplicationClassLoader;

import org.dellroad.javabox.execution.LocalContextExecutionControlProvider;
import org.dellroad.javabox.execution.MemoryLoaderDelegate;
import org.dellroad.stuff.java.MemoryClassLoader;

/**
 * A {@link LocalContextExecutionControlProvider} that creates {@link PermazenExecutionControl}'s.
 */
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

    @Override
    protected MemoryClassLoader createMemoryClassLoader() {
        return new MemoryClassLoader(ApplicationClassLoader.getInstance());
    }
}
