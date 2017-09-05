
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.cmd;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.cli.cmd.AbstractCommand;
import io.permazen.kv.raft.fallback.FallbackKVDatabase;
import io.permazen.kv.raft.fallback.FallbackTarget;
import io.permazen.util.ParseContext;

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
    public CliSession.Action getAction(CliSession session0, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return session -> {
            if (!(session.getKVDatabase() instanceof FallbackKVDatabase))
                throw new Exception("key/value store is not Raft fallback");
            RaftFallbackStatusCommand.printStatus(session.getWriter(), (FallbackKVDatabase)session.getKVDatabase());
        };
    }

    /**
     * Print the fallback status status to the given destination.
     *
     * @param writer destination for status
     * @param db Raft database
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void printStatus(PrintWriter writer, FallbackKVDatabase db) {

        final List<FallbackTarget> targets = db.getFallbackTargets();

        // Show configuration
        writer.println();
        writer.println("Configuration");
        writer.println("=============");
        writer.println();
        writer.println(String.format("%15s: %s", "Standalone KV", db.getStandaloneTarget()));
        writer.println(String.format("%15s: %s", "State file", db.getStateFile()));
        writer.println(String.format("%15s: %s", "Initial Target", db.getInitialTargetIndex()));
        writer.println(String.format("%15s: %s%s", "Maximum Target",
          Math.min(db.getFallbackTargets().size() - 1, db.getMaximumTargetIndex()),
          db.getMaximumTargetIndex() == -1 ? " [STANDALONE MODE]" : ""));
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
              RaftFallbackStatusCommand.objString(target.getUnavailableMergeStrategy()),
              RaftFallbackStatusCommand.objString(target.getRejoinMergeStrategy()), target));
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
              RaftFallbackStatusCommand.date(target != null ? target.getLastChangeTime() : null),
              active ? "Now" : RaftFallbackStatusCommand.date(target != null ? target.getLastActiveTime() : null)));
        }
        writer.println();
    }

    private static String date(Date date) {
        if (date == null)
            return "N/A";
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
    }

    // Strip package name from default toString() output: "foo.bar.Jam@1234" -> "Jam@1234"
    private static String objString(Object obj) {
        return String.valueOf(obj).replaceAll("^(\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*\\.)*"
          + "(\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*@)", "$2");
    }
}

