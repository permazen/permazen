
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

import org.dellroad.jct.core.ExecRequest;
import org.dellroad.jct.core.simple.SimpleExec;

/**
 * A {@link SimpleExec} with an associated Permazen database.
 */
public class PermazenExec extends SimpleExec {

    protected final KVDatabase kvdb;
    protected final Database db;
    protected final Permazen pdb;

// Constructors

    public PermazenExec(KVDatabase kvdb) {
        this(kvdb, null, null);
    }

    public PermazenExec(Database db) {
        this(null, db, null);
    }

    public PermazenExec(Permazen pdb) {
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
    public PermazenExec(KVDatabase kvdb, Database db, Permazen pdb) {
        Preconditions.checkArgument(Stream.of(kvdb, db, pdb).filter(Objects::nonNull).count() == 1,
          "exactly one parameter must be non-null");
        this.kvdb = kvdb;
        this.db = db;
        this.pdb = pdb;
    }

// SimpleExec

    @Override
    public PermazenExecSession newExecSession(ExecRequest request) throws IOException {
        return (PermazenExecSession)super.newExecSession(request);
    }

    @Override
    public final PermazenExecSession newExecSession(ExecRequest request, FoundCommand command) throws IOException {
        final PermazenExecSession execSession = this.createPermazenExecSession(request, command);
        final io.permazen.cli.Session session = this.createSession(execSession);
        execSession.setPermazenSession(session);
        return execSession;
    }

// Internal Methods

    protected PermazenExecSession createPermazenExecSession(ExecRequest request, FoundCommand command) throws IOException {
        return new PermazenExecSession(this, request, command);
    }

    protected io.permazen.cli.Session createSession(PermazenExecSession execSession) {
        return new io.permazen.cli.Session(execSession, this.pdb, this.db, this.kvdb);
    }
}
