
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.cmd;

import java.util.EnumSet;
import java.util.Map;

import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.cli.cmd.AbstractCommand;
import io.permazen.kv.raft.fallback.FallbackKVDatabase;
import io.permazen.util.ParseContext;

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
