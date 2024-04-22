
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

import io.permazen.kv.simple.MemoryKVDatabase;

import java.io.IOException;
import java.util.Collections;

import org.dellroad.jct.core.ShellSession;
import org.dellroad.jct.core.simple.SimpleShell;
import org.dellroad.jct.core.simple.SimpleShellRequest;
import org.jline.terminal.TerminalBuilder;

public class MockSession extends Session {

    public MockSession() {
        super(MockSession.mockShellSession(), new MemoryKVDatabase());
    }

    public static ShellSession mockShellSession() {
        try {
            return new SimpleShell().newShellSession(new SimpleShellRequest(
              TerminalBuilder.builder().build(), Collections.emptyList(), Collections.emptyMap()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
