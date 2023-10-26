
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.cmd;

import com.google.common.base.Preconditions;

import io.permazen.cli.Session;
import io.permazen.kv.raft.CandidateRole;
import io.permazen.kv.raft.Follower;
import io.permazen.kv.raft.FollowerRole;
import io.permazen.kv.raft.LeaderRole;
import io.permazen.kv.raft.LogEntry;
import io.permazen.kv.raft.RaftKVDatabase;
import io.permazen.kv.raft.RaftKVTransaction;
import io.permazen.kv.raft.Role;
import io.permazen.kv.raft.Timestamp;

import java.io.PrintStream;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class RaftStatusCommand extends AbstractRaftCommand {

    public RaftStatusCommand() {
        super("raft-status");
    }

    @Override
    public String getHelpSummary() {
        return "Displays the Raft cluster state of the local node";
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {
        return new RaftAction() {
            @Override
            protected void run(Session session, RaftKVDatabase db) throws Exception {
                RaftStatusCommand.printStatus(session.getOutput(), db);
            }
        };
    }

    /**
     * Print the local Raft node's status to the given destination.
     *
     * @param out destination for status
     * @param db Raft database
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void printStatus(PrintStream out, RaftKVDatabase db) {

        // Sanity check
        Preconditions.checkArgument(out != null, "null out");
        Preconditions.checkArgument(db != null, "null db");

        // Configuration info
        out.println();
        out.println("Configuration");
        out.println("=============");
        out.println();
        out.println(String.format("%-24s: %s", "Log directory", db.getLogDirectory()));
        out.println(String.format("%-24s: %s", "Min election timeout",
          RaftStatusCommand.describeMillis(db.getMinElectionTimeout())));
        out.println(String.format("%-24s: %s", "Max election timeout",
          RaftStatusCommand.describeMillis(db.getMaxElectionTimeout())));
        out.println(String.format("%-24s: %s", "Heartbeat timeout",
          RaftStatusCommand.describeMillis(db.getHeartbeatTimeout())));
        out.println(String.format("%-24s: %s", "Commit timeout",
          RaftStatusCommand.describeMillis(db.getCommitTimeout())));
        out.println(String.format("%-24s: %s", "Max transaction duration",
          RaftStatusCommand.describeMillis(db.getMaxTransactionDuration())));
        out.println(String.format("%-24s: %s", "Follower probing enabled", db.isFollowerProbingEnabled()));

        // Cluster info
        out.println();
        out.println("Cluster Info");
        out.println("============");
        out.println();
        out.println(String.format("%-24s: \"%s\"", "Cluster identity", db.getIdentity()));
        out.println(String.format("%-24s: %s", "Cluster ID",
          db.getClusterId() != 0 ? String.format("0x%08x", db.getClusterId()) : "Unconfigured"));
        out.println(String.format("%-24s: %s", "Node is cluster member", db.isClusterMember() ? "Yes" : "No"));
        final Map<String, String> config = db.getCurrentConfig();
        if (config.isEmpty())
            out.println(String.format("%-24s: %s", "Cluster configuration", "Unconfigured"));
        else {
            out.println();
            out.println(String.format("Cluster configuration:"));
            out.println();
            out.println(String.format("%1s %-16s %s", "", "Identity", "Address"));
            out.println(String.format("%1s %-16s %s", "", "--------", "-------"));
            for (Map.Entry<String, String> entry : db.getCurrentConfig().entrySet()) {
                final String identity = entry.getKey();
                final String address = entry.getValue();
                out.println(String.format("%1s %-16s %s",
                  identity.equals(db.getIdentity()) ? "*" : "", "\"" + identity + "\"", address));
            }
        }

        // General Raft status
        out.println();
        out.println("Raft State");
        out.println("==========");
        out.println();
        out.println(String.format("%-24s: %dt%d", "Last applied log entry", db.getLastAppliedIndex(), db.getLastAppliedTerm()));
        out.println(String.format("%-24s: %d", "Commit Index", db.getCommitIndex()));
        out.println(String.format("%-24s: %d", "Current term", db.getCurrentTerm()));
        final long currentTermStartTime = db.getCurrentTermStartTime();
        out.println(String.format("%-24s: %s", "Term started", currentTermStartTime != 0 ?
          new Date(currentTermStartTime)
           + " (" + RaftStatusCommand.describeMillis(System.currentTimeMillis() - currentTermStartTime) + ")" :
          "Unknown"));
        final Role role = db.getCurrentRole();
        out.println(String.format("%-24s: %s", "Current Role",
          role instanceof LeaderRole ? "LEADER" :
          role instanceof FollowerRole ? "FOLLOWER" :
          role instanceof CandidateRole ? "CANDIDATE" : "?" + role));
        out.println(String.format("%-24s: %d", "Unapplied memory usage", db.getUnappliedLogMemoryUsage()));
        final List<LogEntry> log = db.getUnappliedLog();
        out.println(String.format("%-24s: %d", "Unapplied log entries", log.size()));
        if (!log.isEmpty()) {
            out.println();
            out.println(String.format("  %-13s %-6s %-10s %-8s %s", "Entry", "Commit", "Size", "Age", "Config Change"));
            out.println(String.format("  %-13s %-6s %-10s %-8s %s", "-----", "------", "----", "---", "-------------"));
            for (LogEntry entry : log) {
                out.println(String.format("  %-13s %-6s %-10d %-8s %s", entry.getIndex() + "t" + entry.getTerm(),
                  entry.getIndex() <= db.getCommitIndex() ? "Yes" : "No", entry.getFileSize(),
                  RaftStatusCommand.describeMillis(entry.getAge()), RaftStatusCommand.describe(entry.getConfigChange())));
            }
        }

        // Role-specific info
        if (role instanceof LeaderRole) {
            final LeaderRole leader = (LeaderRole)role;

            out.println();
            out.println("Leader Info");
            out.println("===========");
            out.println();
            out.println(String.format("%-24s: %s", "Lease Timeout", leader.getLeaseTimeout() != null ?
              String.format("%+dms", leader.getLeaseTimeout().offsetFromNow()) : "Not established"));
            final List<Follower> followers = leader.getFollowers();
            out.println(String.format("%-24s: %d", "Followers", followers.size()));
            if (!followers.isEmpty()) {
                out.println();
                out.println(String.format("  %-16s %-8s %-10s %-10s %-10s %s",
                  "Identity", "Status", "Match", "Next", "Commit", "Timestamp"));
                out.println(String.format("  %-16s %-8s %-10s %-10s %-10s %s",
                  "--------", "------", "-----", "----", "------", "---------"));
                for (Follower follower : leader.getFollowers()) {
                    out.println(String.format("  %-16s %-8s %-10s %-10s %-10s %s", follower.getIdentity(),
                      follower.isReceivingSnapshot() ? "Snapshot" : follower.isSynced() ? "Synced" : "No Sync",
                      follower.getMatchIndex(), follower.getNextIndex(), follower.getLeaderCommit(),
                      follower.getLeaderTimestamp() != null ?
                       String.format("%+dms", follower.getLeaderTimestamp().offsetFromNow()) : "None"));
                }
            }
        } else if (role instanceof FollowerRole) {
            final FollowerRole follower = (FollowerRole)role;

            out.println();
            out.println("Follower Info");
            out.println("=============");
            out.println();
            out.println(String.format("%-24s: %s", "Leader Identity",
              follower.getLeaderIdentity() != null ? "\"" + follower.getLeaderIdentity() + "\"" : "Unknown"));
            out.println(String.format("%-24s: %s", "Leader Address",
              follower.getLeaderAddress() != null ? follower.getLeaderAddress() : "Unknown"));
            out.println(String.format("%-24s: %s", "Voted For",
              follower.getVotedFor() != null ? "\"" + follower.getVotedFor() + "\"" : "Nobody"));
            out.println(String.format("%-24s: %s", "Installing snapshot", follower.isInstallingSnapshot() ? "Yes" : "No"));
            final Timestamp electionTimeout = follower.getElectionTimeout();
            out.println(String.format("%-24s: %s", "Election timer running",
              electionTimeout != null ? "Yes; expires in " + electionTimeout.offsetFromNow() + "ms" : "No"));
            final int probed = follower.getNodesProbed();
            out.println(String.format("%-24s: %s", "Election nodes probed",
              probed != -1 ? String.format("%d / %d", follower.getNodesProbed(), config.size()) : "Not probing"));
        } else if (role instanceof CandidateRole) {
            final CandidateRole candidate = (CandidateRole)role;

            out.println();
            out.println("Candidate Info");
            out.println("==============");
            out.println();
            out.println(String.format("%-24s: %d", "Votes Required", candidate.getVotesRequired()));
            out.println(String.format("%-24s: %d", "Votes Received", candidate.getVotesReceived()));
        }

        // Transactions
        out.println();
        out.println("Open Transactions");
        out.println("=================");
        out.println();
        out.println(String.format("%1s %-10s %-14s %-8s %-5s %-18s %-13s %-13s %s",
          "", "ID", "State", "Since", "Type", "Consistency", "Base", "Commit", "Config Change"));
        out.println(String.format("%1s %-10s %-14s %-8s %-5s %-18s %-13s %-13s %s",
          "", "--", "-----", "-----", "----", "-----------", "----", "------", "-------------"));
        for (RaftKVTransaction tx2 : db.getOpenTransactions()) {
            out.println(String.format("  %-10d %-14s %-8s %-5s %-18s %-13s %-13s %s", tx2.getTxId(),
              tx2.getState(), RaftStatusCommand.describeMillis(-tx2.getLastStateChangeTime().offsetFromNow()),
              tx2.isReadOnly() ? "R/O" : "R/W", tx2.getConsistency(), tx2.getBaseIndex() + "t" + tx2.getBaseTerm(),
              tx2.getCommitIndex() != 0 || tx2.getCommitTerm() != 0 ? tx2.getCommitIndex() + "t" + tx2.getCommitTerm() : "",
              RaftStatusCommand.describe(tx2.getConfigChange())));
        }
        out.println();
    }

    // Describe a config change
    private static String describe(String[] change) {
        return change != null ?
          (change[1] != null ? String.format("+\"%s\"@%s", change[0], change[1]) : "-\"" + change[0] + "\"") : "";
    }

    private static String describeMillis(long value) {
        StringBuilder b = new StringBuilder(32);
        if (value < 0) {
            b.append('-');
            value = -value;
        }
        long days = value / (24 * 60 * 60 * 1000);
        value = value % (24 * 60 * 60 * 1000);
        if (days > 0)
            b.append(days).append('d');
        long hours = value / (60 * 60 * 1000);
        value = value % (60 * 60 * 1000);
        if (hours > 0)
            b.append(hours).append('h');
        long minutes = value / (60 * 1000);
        value = value % (60 * 1000);
        if (minutes > 0)
            b.append(minutes).append('m');
        long millis = value;
        if (millis != 0 || b.length() == 0) {
            if (millis >= 1000 && millis % 1000 == 0)
                b.append(String.format("%ds", millis / 1000));
            else
                b.append(String.format("%.3fs", millis / 1000.0));
        }
        return b.toString();
    }
}
