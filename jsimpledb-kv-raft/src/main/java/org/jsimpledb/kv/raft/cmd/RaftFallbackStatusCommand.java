
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.cmd;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.cli.cmd.AbstractCommand;
import org.jsimpledb.kv.raft.fallback.FallbackKVDatabase;
import org.jsimpledb.kv.raft.fallback.FallbackTarget;
import org.jsimpledb.util.ParseContext;

public class RaftFallbackStatusCommand extends AbstractCommand {

    public RaftFallbackStatusCommand() {
        super("raft-fallback-status");
    }

    @Override
    public String getHelpSummary() {
        return "Displays the status of the Raft fallback database";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return new CliSession.Action() {
            @Override
            public void run(CliSession session) throws Exception {
                if (!(session.getKVDatabase() instanceof FallbackKVDatabase))
                    throw new Exception("key/value store is not Raft fallback");
                RaftFallbackStatusCommand.this.displayStatus(session.getWriter(), (FallbackKVDatabase)session.getKVDatabase());
            }
        };
    }

    private void displayStatus(PrintWriter writer, FallbackKVDatabase db) {

    // TODO show configuration here...

        final List<FallbackTarget> targets = db.getFallbackTargets();
        final Date lastStandaloneActiveTime = db.getLastStandaloneActiveTime();

        // Show configuration
        writer.println();
        writer.println("Configuration");
        writer.println("=============");
        writer.println();
        writer.println(String.format("%15s: %s", "Standalone KV", db.getStandaloneTarget()));
        writer.println(String.format("%15s: %s", "State file", db.getStateFile()));
        writer.println();
        writer.println("Raft Targets");
        writer.println("============");
        writer.println();
        writer.println(String.format("  %5s %-10s %-10s %-9s %-11s %-20.20s %-20.20s %s",
          "Index", "Check Int.", "Tx Timeout", "Min Avail", "Min Unavail", "Merge", "Rejoin", "Description"));
        writer.println(String.format("  %5s %-10s %-10s %-9s %-11s %-20.20s %-20.20s %s",
          "-----", "----------", "----------", "---------", "-----------", "-----", "------", "-----------"));
        for (int i = 0; i < targets.size(); i++) {
            final FallbackTarget target = targets.get(i);
            writer.println(String.format("  %5s %10s %10s %9s %11s %-20.20s %-20.20s %.38s",
              i, target.getCheckInterval() + "ms", target.getTransactionTimeout() + "ms",
              target.getMinAvailableTime() + "ms", target.getMinUnavailableTime() + "ms",
              this.objString(target.getUnavailableMergeStrategy()), this.objString(target.getRejoinMergeStrategy()),
              target));
        }

        // Show status
        writer.println();
        writer.println("Fallback Status");
        writer.println("===============");
        writer.println();
        writer.println(String.format("  %5s %-6s %-9s %-20s %s", "Index", "Active", "Available", "Last Change", "Last Active"));
        writer.println(String.format("  %5s %-6s %-9s %-20s %s", "-----", "------", "---------", "-----------", "-----------"));
        for (int i = -1; i < targets.size(); i++) {
            final FallbackTarget target = i >= 0 ? targets.get(i) : null;
            final boolean active = i == db.getCurrentTargetIndex();
            final boolean available = target == null || target.isAvailable();
            writer.println(String.format("  %3d   %3s    %6s    %-20s %s",
              i, active ? "*" : "", available ? "Yes" : "No ",
              this.date(target != null ? target.getLastChangeTime() : null),
              active ? "Now" : this.date(target != null ? target.getLastActiveTime() : null)));
        }
        writer.println();
    }

    private String date(Date date) {
        if (date == null)
            return "N/A";
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
    }

    // Strip package name from default toString() output: "foo.bar.Jam@1234" -> "Jam@1234"
    private String objString(Object obj) {
        return String.valueOf(obj).replaceAll("^(\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*\\.)*"
          + "(\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*@)", "$2");
    }
}

