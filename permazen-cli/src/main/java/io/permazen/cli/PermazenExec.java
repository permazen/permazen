
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
    protected final Permazen jdb;

// Constructors

    public PermazenExec(KVDatabase kvdb) {
        this(kvdb, null, null);
    }

    public PermazenExec(Database db) {
        this(null, db, null);
    }

    public PermazenExec(Permazen jdb) {
        this(null, null, jdb);
    }

    /**
     * General constructor.
     *
     * <p>
     * Exactly one of the three parameters must be non-null.
     *
     * @param kvdb key/value database
     * @param db core API database
     * @param jdb Java database
     * @throws IllegalArgumentException if not exactly one parameter is non-null
     */
    public PermazenExec(KVDatabase kvdb, Database db, Permazen jdb) {
        Preconditions.checkArgument(Stream.of(kvdb, db, jdb).filter(Objects::nonNull).count() == 1,
          "exactly one parameter must be non-null");
        this.kvdb = kvdb;
        this.db = db;
        this.jdb = jdb;
    }

// SimpleExec

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
        return new io.permazen.cli.Session(execSession, this.jdb, this.db, this.kvdb);
    }
}