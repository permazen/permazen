
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.cmd;

import java.util.EnumSet;
import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.cli.cmd.AbstractCommand;
import org.jsimpledb.kv.raft.fallback.FallbackKVDatabase;
import org.jsimpledb.util.ParseContext;

public class RaftFallbackForceStandaloneCommand extends AbstractCommand {

    public RaftFallbackForceStandaloneCommand() {
        super("raft-fallback-force-standalone on:boolean");
    }

    @Override
    public String getHelpSummary() {
        return "Forces the Raft fallback database into standalone mode";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    public CliSession.Action getAction(CliSession session0, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final boolean on = (Boolean)params.get("on");
        return session -> {
            if (!(session.getKVDatabase() instanceof FallbackKVDatabase))
                throw new Exception("key/value store is not Raft fallback");
            final FallbackKVDatabase fallbackKV = (FallbackKVDatabase)session.getKVDatabase();
            fallbackKV.setMaximumTargetIndex(on ? -1 : Integer.MAX_VALUE);
        };
    }
}
