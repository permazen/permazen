
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.raft.Follower;
import org.jsimpledb.kv.raft.LogEntry;
import org.jsimpledb.kv.raft.RaftKVDatabase;
import org.jsimpledb.kv.raft.RaftKVTransaction;
import org.jsimpledb.kv.raft.Timestamp;
import org.jsimpledb.kv.raft.TxState;
import org.jsimpledb.parse.ParseContext;

@Command(modes = { SessionMode.KEY_VALUE, SessionMode.CORE_API, SessionMode.JSIMPLEDB })
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
            protected void run(CliSession session, RaftKVTransaction tx) throws Exception {
                session.setRollbackOnly();
                RaftStatusCommand.this.displayStatus(session.getWriter(), tx, tx.getKVDatabase());
            }
        };
    }

    private void displayStatus(PrintWriter writer, RaftKVTransaction tx, RaftKVDatabase db) {

        // Configuration info
        writer.println();
        writer.println("Configuration");
        writer.println("=============");
        writer.println();
        writer.println(String.format("%-24s: %s", "Log directory", db.getLogDirectory()));
        writer.println(String.format("%-24s: %d.%03d sec", "Min election timeout",
          db.getMinElectionTimeout() / 1000, db.getMinElectionTimeout() % 1000));
        writer.println(String.format("%-24s: %d.%03d sec", "Max election timeout",
          db.getMaxElectionTimeout() / 1000, db.getMaxElectionTimeout() % 1000));
        writer.println(String.format("%-24s: %d.%03d sec", "Heartbeat timeout",
          db.getHeartbeatTimeout() / 1000, db.getHeartbeatTimeout() % 1000));
        writer.println(String.format("%-24s: %d.%03d sec", "Commit timeout",
          db.getCommitTimeout() / 1000, db.getCommitTimeout() % 1000));
        writer.println(String.format("%-24s: %d.%03d sec", "Max transaction duration",
          db.getMaxTransactionDuration() / 1000, db.getMaxTransactionDuration() % 1000));

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
        final RaftKVDatabase.Role role = db.getCurrentRole();
        writer.println(String.format("%-24s: %s", "Current Role",
          role instanceof RaftKVDatabase.LeaderRole ? "Leader" :
          role instanceof RaftKVDatabase.FollowerRole ? "Follower" :
          role instanceof RaftKVDatabase.CandidateRole ? "Candidate" : "None"));
        writer.println(String.format("%-24s: %d", "Unapplied memory usage", db.getUnappliedLogMemoryUsage()));
        final List<LogEntry> log = db.getUnappliedLog();
        writer.println(String.format("%-24s: %d", "Unapplied log entries", log.size()));
        if (!log.isEmpty()) {
            writer.println();
            writer.println(String.format("%-10s %-6s %-10s %-8s %s", "Entry", "Commit", "Size", "Age", "Config Change"));
            writer.println(String.format("%-10s %-6s %-10s %-8s %s", "-----", "------", "----", "---", "-------------"));
            for (LogEntry entry : log) {
                writer.println(String.format("%-10s %-6s %-10d %-8s %s", entry.getIndex() + "t" + entry.getTerm(),
                  entry.getIndex() <= db.getCommitIndex() ? "Yes" : "No",
                  entry.getFileSize(), String.format("%d.%03ds", entry.getAge() / 1000, entry.getAge() % 1000),
                  this.describe(entry.getConfigChange())));
            }
        }

        // Role-specific info
        if (role instanceof RaftKVDatabase.LeaderRole) {
            final RaftKVDatabase.LeaderRole leader = (RaftKVDatabase.LeaderRole)role;

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
        } else if (role instanceof RaftKVDatabase.FollowerRole) {
            final RaftKVDatabase.FollowerRole follower = (RaftKVDatabase.FollowerRole)role;

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
        } else if (role instanceof RaftKVDatabase.CandidateRole) {
            final RaftKVDatabase.CandidateRole candidate = (RaftKVDatabase.CandidateRole)role;

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
        writer.println(String.format("%1s %-6s %-10s %-5s %-12s %-8s %-8s %s",
          "", "ID", "State", "Type", "Consistency", "Base", "Commit", "Config Change"));
        writer.println(String.format("%1s %-6s %-10s %-5s %-12s %-8s %-8s %s",
          "", "--", "-----", "----", "-----------", "----", "------", "-------------"));
        for (RaftKVTransaction tx2 : db.getOpenTransactions()) {
            writer.println(String.format("%1s %-6d %-10s %-5s %-12s %-8s %-8s %s", tx2 == tx ? "*" : "", tx2.getTxId(),
              tx2.getState(), tx2.isReadOnly() ? "R/O" : "R/W", tx2.getConsistency(), tx2.getBaseIndex() + "t" + tx2.getBaseTerm(),
              tx2.getState().compareTo(TxState.COMMIT_WAITING) >= 0 ? tx2.getCommitIndex() + "t" + tx2.getCommitTerm() : "",
              this.describe(tx2.getConfigChange())));
        }
        writer.println();
    }

    // Describe a config change
    private String describe(String[] change) {
        return change != null ?
          (change[1] != null ? String.format("+\"%s\"@%s", change[0], change[1]) : "-\"" + change[0] + "\"") : "";
    }
}

