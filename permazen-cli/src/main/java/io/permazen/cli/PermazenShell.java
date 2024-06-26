
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

import com.google.common.base.Preconditions;

import io.permazen.Permazen;
import io.permazen.core.Database;
import io.permazen.kv.KVDatabase;

import java.io.IOException;
import java.util.Objects;
import java.util.stream.Stream;

import org.dellroad.jct.core.ShellRequest;
import org.dellroad.jct.core.ShellSession;
import org.dellroad.jct.core.simple.SimpleShell;
import org.jline.reader.LineReader;

/**
 * A {@link ShellSession} that with an associated Permazen database.
 */
public class PermazenShell extends SimpleShell {

    protected final KVDatabase kvdb;
    protected final Database db;
    protected final Permazen pdb;

// Constructors

    public PermazenShell(KVDatabase kvdb) {
        this(kvdb, null, null);
    }

    public PermazenShell(Database db) {
        this(null, db, null);
    }

    public PermazenShell(Permazen pdb) {
        this(null, null, pdb);
    }

    /**
     * General constructor.
     *
     * <p>
     * Exactly one of the three parameters must be non-null.
     *
     * @param kvdb key/value database
     * @param db core API database
     * @param pdb Java database
     * @throws IllegalArgumentException if not exactly one parameter is non-null
     */
    public PermazenShell(KVDatabase kvdb, Database db, Permazen pdb) {
        Preconditions.checkArgument(Stream.of(kvdb, db, pdb).filter(Objects::nonNull).count() == 1,
          "exactly one parameter must be non-null");
        this.kvdb = kvdb;
        this.db = db;
        this.pdb = pdb;
    }

// SimpleShell

    @Override
    public PermazenShellSession newShellSession(ShellRequest request) throws IOException {
        return (PermazenShellSession)super.newShellSession(request);
    }

    @Override
    public final PermazenShellSession newShellSession(ShellRequest request, LineReader reader) throws IOException {
        final PermazenShellSession shellSession = this.createPermazenShellSession(request, reader);
        if (shellSession.getPermazenSession() == null)
            shellSession.setPermazenSession(this.createSession(shellSession));
        return shellSession;
    }

    @Override
    public String getNormalPrompt() {
        return "Permazen> ";
    }

    @Override
    public String getContinuationPrompt() {
        return "........> ";
    }

// Internal Methods

    protected PermazenShellSession createPermazenShellSession(ShellRequest request, LineReader reader) throws IOException {
        return new PermazenShellSession(this, request, reader);
    }

    protected io.permazen.cli.Session createSession(PermazenShellSession shellSession) {
        return new io.permazen.cli.Session(shellSession, this.pdb, this.db, this.kvdb);
    }
}
