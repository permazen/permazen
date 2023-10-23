
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.cmd;

import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.cli.cmd.AbstractCommand;
import io.permazen.kv.raft.fallback.FallbackKVDatabase;
import io.permazen.kv.raft.fallback.FallbackTarget;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

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
    public Session.Action getAction(Session session0, Map<String, Object> params) {
        return session -> {
            if (!(session.getKVDatabase() instanceof FallbackKVDatabase))
                throw new Exception("key/value store is not Raft fallback");
            RaftFallbackStatusCommand.printStatus(session.getOutput(), (FallbackKVDatabase)session.getKVDatabase());
        };
    }

    /**
     * Print the fallback status status to the given destination.
     *
     * @param out destination for status
     * @param db Raft database
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void printStatus(PrintStream out, FallbackKVDatabase db) {

        final List<FallbackTarget> targets = db.getFallbackTargets();

        // Show configuration
        out.println();
        out.println("Configuration");
        out.println("=============");
        out.println();
        out.println(String.format("%15s: %s", "Standalone KV", db.getStandaloneTarget()));
        out.println(String.format("%15s: %s", "State file", db.getStateFile()));
        out.println(String.format("%15s: %s", "Initial Target", db.getInitialTargetIndex()));
        out.println(String.format("%15s: %s%s", "Maximum Target",
          Math.min(db.getFallbackTargets().size() - 1, db.getMaximumTargetIndex()),
          db.getMaximumTargetIndex() == -1 ? " [STANDALONE MODE]" : ""));
        out.println();
        out.println("Raft Targets");
        out.println("============");
        out.println();
        out.println(String.format("  %5s %-10s %-10s %-9s %-11s %-20.20s %-20.20s %s",
          "Index", "Check Int.", "Tx Timeout", "Min Avail", "Min Unavail", "Merge", "Rejoin", "Description"));
        out.println(String.format("  %5s %-10s %-10s %-9s %-11s %-20.20s %-20.20s %s",
          "-----", "----------", "----------", "---------", "-----------", "-----", "------", "-----------"));
        for (int i = 0; i < targets.size(); i++) {
            final FallbackTarget target = targets.get(i);
            out.println(String.format("  %5s %10s %10s %9s %11s %-20.20s %-20.20s %.38s",
              i, target.getCheckInterval() + "ms", target.getTransactionTimeout() + "ms",
              target.getMinAvailableTime() + "ms", target.getMinUnavailableTime() + "ms",
              RaftFallbackStatusCommand.objString(target.getUnavailableMergeStrategy()),
              RaftFallbackStatusCommand.objString(target.getRejoinMergeStrategy()), target));
        }

        // Show status
        out.println();
        out.println("Fallback Status");
        out.println("===============");
        out.println();
        out.println(String.format("  %5s %-6s %-9s %-20s %s", "Index", "Active", "Available", "Last Change", "Last Active"));
        out.println(String.format("  %5s %-6s %-9s %-20s %s", "-----", "------", "---------", "-----------", "-----------"));
        for (int i = -1; i < targets.size(); i++) {
            final FallbackTarget target = i >= 0 ? targets.get(i) : null;
            final boolean active = i == db.getCurrentTargetIndex();
            final boolean available = target == null || target.isAvailable();
            out.println(String.format("  %3d   %3s    %6s    %-20s %s",
              i, active ? "*" : "", available ? "Yes" : "No ",
              RaftFallbackStatusCommand.date(target != null ? target.getLastChangeTime() : null),
              active ? "Now" : RaftFallbackStatusCommand.date(target != null ? target.getLastActiveTime() : null)));
        }
        out.println();
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

