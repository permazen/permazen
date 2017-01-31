
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft.cmd;

import com.google.common.base.Preconditions;

import java.io.PrintWriter;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.raft.CandidateRole;
import org.jsimpledb.kv.raft.Follower;
import org.jsimpledb.kv.raft.FollowerRole;
import org.jsimpledb.kv.raft.LeaderRole;
import org.jsimpledb.kv.raft.LogEntry;
import org.jsimpledb.kv.raft.RaftKVDatabase;
import org.jsimpledb.kv.raft.RaftKVTransaction;
import org.jsimpledb.kv.raft.Role;
import org.jsimpledb.kv.raft.Timestamp;
import org.jsimpledb.kv.raft.TxState;
import org.jsimpledb.util.ParseContext;

public class RaftStatusCommand extends AbstractRaftCommand {

    public RaftStatusCommand() {
        super("raft-status");
    }

    @Override
    public String getHelpSummary() {
        return "Displays the Raft cluster state of the local node";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        return new RaftAction() {
            @Override
            protected void run(CliSession session, RaftKVDatabase db) throws Exception {
                RaftStatusCommand.printStatus(session.getWriter(), db);
            }
        };
    }

    /**
     * Print the local Raft node's status to the given destination.
     *
     * @param writer destination for status
     * @param db Raft database
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void printStatus(PrintWriter writer, RaftKVDatabase db) {

        // Sanity check
        Preconditions.checkArgument(writer != null, "null writer");
        Preconditions.checkArgument(db != null, "null db");

        // Configuration info
        writer.println();
        writer.println("Configuration");
        writer.println("=============");
        writer.println();
        writer.println(String.format("%-24s: %s", "Log directory", db.getLogDirectory()));
        writer.println(String.format("%-24s: %s", "Min election timeout",
          RaftStatusCommand.describeMillis(db.getMinElectionTimeout())));
        writer.println(String.format("%-24s: %s", "Max election timeout",
          RaftStatusCommand.describeMillis(db.getMaxElectionTimeout())));
        writer.println(String.format("%-24s: %s", "Heartbeat timeout",
          RaftStatusCommand.describeMillis(db.getHeartbeatTimeout())));
        writer.println(String.format("%-24s: %s", "Commit timeout",
          RaftStatusCommand.describeMillis(db.getCommitTimeout())));
        writer.println(String.format("%-24s: %s", "Max transaction duration",
          RaftStatusCommand.describeMillis(db.getMaxTransactionDuration())));
        writer.println(String.format("%-24s: %s", "Follower probing enabled", db.isFollowerProbingEnabled()));

        // Cluster info
        writer.println();
        writer.println("Cluster Info");
        writer.println("============");
        writer.println();
        writer.println(String.format("%-24s: \"%s\"", "Cluster identity", db.getIdentity()));
        writer.println(String.format("%-24s: %s", "Cluster ID",
          db.getClusterId() != 0 ? String.format("0x%08x", db.getClusterId()) : "Unconfigured"));
        writer.println(String.format("%-24s: %s", "Node is cluster member", db.isClusterMember() ? "Yes" : "No"));
        final Map<String, String> config = db.getCurrentConfig();
        if (config.isEmpty())
            writer.println(String.format("%-24s: %s", "Cluster configuration", "Unconfigured"));
        else {
            writer.println();
            writer.println(String.format("Cluster configuration:"));
            writer.println();
            writer.println(String.format("%1s %-16s %s", "", "Identity", "Address"));
            writer.println(String.format("%1s %-16s %s", "", "--------", "-------"));
            for (Map.Entry<String, String> entry : db.getCurrentConfig().entrySet()) {
                final String identity = entry.getKey();
                final String address = entry.getValue();
                writer.println(String.format("%1s %-16s %s",
                  identity.equals(db.getIdentity()) ? "*" : "", "\"" + identity + "\"", address));
            }
        }

        // General Raft status
        writer.println();
        writer.println("Raft State");
        writer.println("==========");
        writer.println();
        writer.println(String.format("%-24s: %dt%d", "Last applied log entry", db.getLastAppliedIndex(), db.getLastAppliedTerm()));
        writer.println(String.format("%-24s: %d", "Commit Index", db.getCommitIndex()));
        writer.println(String.format("%-24s: %d", "Current term", db.getCurrentTerm()));
        final long currentTermStartTime = db.getCurrentTermStartTime();
        writer.println(String.format("%-24s: %s", "Term started", currentTermStartTime != 0 ?
          new Date(currentTermStartTime)
           + " (" + RaftStatusCommand.describeMillis(System.currentTimeMillis() - currentTermStartTime) + ")" :
          "Unknown"));
        final Role role = db.getCurrentRole();
        writer.println(String.format("%-24s: %s", "Current Role",
          role instanceof LeaderRole ? "LEADER" :
          role instanceof FollowerRole ? "FOLLOWER" :
          role instanceof CandidateRole ? "CANDIDATE" : "?" + role));
        writer.println(String.format("%-24s: %d", "Unapplied memory usage", db.getUnappliedLogMemoryUsage()));
        final List<LogEntry> log = db.getUnappliedLog();
        writer.println(String.format("%-24s: %d", "Unapplied log entries", log.size()));
        if (!log.isEmpty()) {
            writer.println();
            writer.println(String.format("  %-13s %-6s %-10s %-8s %s", "Entry", "Commit", "Size", "Age", "Config Change"));
            writer.println(String.format("  %-13s %-6s %-10s %-8s %s", "-----", "------", "----", "---", "-------------"));
            for (LogEntry entry : log) {
                writer.println(String.format("  %-13s %-6s %-10d %-8s %s", entry.getIndex() + "t" + entry.getTerm(),
                  entry.getIndex() <= db.getCommitIndex() ? "Yes" : "No", entry.getFileSize(),
                  RaftStatusCommand.describeMillis(entry.getAge()), RaftStatusCommand.describe(entry.getConfigChange())));
            }
        }

        // Role-specific info
        if (role instanceof LeaderRole) {
            final LeaderRole leader = (LeaderRole)role;

            writer.println();
            writer.println("Leader Info");
            writer.println("===========");
            writer.println();
            writer.println(String.format("%-24s: %s", "Lease Timeout", leader.getLeaseTimeout() != null ?
              String.format("%+dms", leader.getLeaseTimeout().offsetFromNow()) : "Not established"));
            final List<Follower> followers = leader.getFollowers();
            writer.println(String.format("%-24s: %d", "Followers", followers.size()));
            if (!followers.isEmpty()) {
                writer.println();
                writer.println(String.format("  %-16s %-8s %-6s %-6s %-6s %s",
                  "Identity", "Status", "Match", "Next", "Commit", "Timestamp"));
                writer.println(String.format("  %-16s %-8s %-6s %-6s %-6s %s",
                  "--------", "------", "-----", "----", "------", "---------"));
                for (Follower follower : leader.getFollowers()) {
                    writer.println(String.format("  %-16s %-8s %-6s %-6s %-6s %s", follower.getIdentity(),
                      follower.isReceivingSnapshot() ? "Snapshot" : follower.isSynced() ? "Synced" : "No Sync",
                      follower.getMatchIndex(), follower.getNextIndex(), follower.getLeaderCommit(),
                      follower.getLeaderTimestamp() != null ?
                       String.format("%+dms", follower.getLeaderTimestamp().offsetFromNow()) : "None"));
                }
            }
        } else if (role instanceof FollowerRole) {
            final FollowerRole follower = (FollowerRole)role;

            writer.println();
            writer.println("Follower Info");
            writer.println("=============");
            writer.println();
            writer.println(String.format("%-24s: %s", "Leader Identity",
              follower.getLeaderIdentity() != null ? "\"" + follower.getLeaderIdentity() + "\"" : "Unknown"));
            writer.println(String.format("%-24s: %s", "Leader Address",
              follower.getLeaderAddress() != null ? follower.getLeaderAddress() : "Unknown"));
            writer.println(String.format("%-24s: %s", "Voted For",
              follower.getVotedFor() != null ? "\"" + follower.getVotedFor() + "\"" : "Nobody"));
            writer.println(String.format("%-24s: %s", "Installing snapshot", follower.isInstallingSnapshot() ? "Yes" : "No"));
            final Timestamp electionTimeout = follower.getElectionTimeout();
            writer.println(String.format("%-24s: %s", "Election timer running",
              electionTimeout != null ? "Yes; expires in " + electionTimeout.offsetFromNow() + "ms" : "No"));
            final int probed = follower.getNodesProbed();
            writer.println(String.format("%-24s: %s", "Election nodes probed",
              probed != -1 ? String.format("%d / %d", follower.getNodesProbed(), config.size()) : "Not probing"));
        } else if (role instanceof CandidateRole) {
            final CandidateRole candidate = (CandidateRole)role;

            writer.println();
            writer.println("Candidate Info");
            writer.println("==============");
            writer.println();
            writer.println(String.format("%-24s: %d", "Votes Required", candidate.getVotesRequired()));
            writer.println(String.format("%-24s: %d", "Votes Received", candidate.getVotesReceived()));
        }

        // Transactions
        writer.println();
        writer.println("Open Transactions");
        writer.println("=================");
        writer.println();
        writer.println(String.format("%1s %-10s %-14s %-8s %-5s %-12s %-13s %-13s %s",
          "", "ID", "State", "Since", "Type", "Consistency", "Base", "Commit", "Config Change"));
        writer.println(String.format("%1s %-10s %-14s %-8s %-5s %-12s %-13s %-13s %s",
          "", "--", "-----", "-----", "----", "-----------", "----", "------", "-------------"));
        for (RaftKVTransaction tx2 : db.getOpenTransactions()) {
            writer.println(String.format("  %-10d %-14s %-8s %-5s %-12s %-13s %-13s %s", tx2.getTxId(),
              tx2.getState(), RaftStatusCommand.describeMillis(-tx2.getLastStateChangeTime().offsetFromNow()),
              tx2.isReadOnly() ? "R/O" : "R/W", tx2.getConsistency(), tx2.getBaseIndex() + "t" + tx2.getBaseTerm(),
              tx2.getState().compareTo(TxState.COMMIT_WAITING) >= 0 ? tx2.getCommitIndex() + "t" + tx2.getCommitTerm() : "",
              RaftStatusCommand.describe(tx2.getConfigChange())));
        }
        writer.println();
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

