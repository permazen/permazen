
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayDeque;

import org.dellroad.stuff.net.TCPNetwork;
import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.kv.raft.fallback.FallbackKVDatabase;
import io.permazen.kv.raft.fallback.FallbackTarget;
import io.permazen.kv.raft.fallback.MergeStrategy;
import io.permazen.kv.raft.fallback.NullMergeStrategy;
import io.permazen.kv.raft.fallback.OverwriteMergeStrategy;

public class RaftKVImplementation extends KVImplementation {

    @Override
    public String[][] getCommandLineOptions() {
        return new String[][] {
            { "--raft directory",
              "Use Raft key/value database in specified directory" },
            { "--raft-min-election-timeout",
              "Specify Raft minimum election timeout in ms (default " + RaftKVDatabase.DEFAULT_MIN_ELECTION_TIMEOUT + ")" },
            { "--raft-max-election-timeout",
              "Specify Raft maximum election timeout in ms (default " + RaftKVDatabase.DEFAULT_MAX_ELECTION_TIMEOUT + ")" },
            { "--raft-heartbeat-timeout",
              "Specify Raft leader heartbeat timeout in ms (default " + RaftKVDatabase.DEFAULT_HEARTBEAT_TIMEOUT + ")" },
            { "--raft-identity",
              "Specify Raft identity" },
            { "--raft-address address",
              "Specify Specify local Raft node's IP address" },
            { "--raft-port",
              "Specify Specify local Raft node's TCP port (default " + RaftKVDatabase.DEFAULT_TCP_PORT + ")" },
            { "--raft-fallback statefile",
              "Use Raft fallback database with specified state file" },
            { "--raft-fallback-check-interval",
              "Specify Raft fallback check interval in milliseconds (default " + FallbackTarget.DEFAULT_CHECK_INTERVAL + ")" },
            { "--raft-fallback-min-available",
              "Specify Raft fallback min available time in milliseconds (default "
                + FallbackTarget.DEFAULT_MIN_AVAILABLE_TIME + ")" },
            { "--raft-fallback-min-unavailable",
              "Specify Raft fallback min unavailable time in milliseconds (default "
                + FallbackTarget.DEFAULT_MIN_UNAVAILABLE_TIME + ")" },
            { "--raft-fallback-check-timeout",
              "Specify Raft fallback availability check TX timeout in milliseconds (default "
                + FallbackTarget.DEFAULT_TRANSACTION_TIMEOUT + ")" },
            { "--raft-fallback-unavailable-merge",
              "Specify Raft fallback unavailable merge strategy class name (default `"
                + OverwriteMergeStrategy.class.getName() + "')" },
            { "--raft-fallback-rejoin-merge",
              "Specify Raft fallback rejoin merge strategy class name (default `"
                + NullMergeStrategy.class.getName() + "')" },
        };
    }

    @Override
    public String getUsageText() {
        return "Raft requires its own internal key/value store, which should also be specified along with `--raft'.\n"
          + "For Raft fallback, specify `--raft-fallback' in addition.";
    }

    @Override
    public Config parseCommandLineOptions(ArrayDeque<String> options) {

        // Raft options
        String arg = this.parseCommandLineOption(options, "--raft");
        if (arg == null)
            return null;
        final Config config = new Config(new File(arg));
        if ((arg = this.parseCommandLineOption(options, "--raft-identity")) != null)
            config.getRaft().setIdentity(arg);
        if ((arg = this.parseCommandLineOption(options, "--raft-address")) != null) {
            config.setAddress(TCPNetwork.parseAddressPart(arg));
            config.setPort(TCPNetwork.parsePortPart(arg, config.getPort()));
        }
        if ((arg = this.parseCommandLineOption(options, "--raft-port")) != null) {
            final int port = TCPNetwork.parsePortPart("x:" + arg, -1);
            if (port == -1)
                throw new IllegalArgumentException("invalid TCP port `" + arg + "'");
            config.setPort(port);
        }
        int value;
        if ((value = this.parseMillisecondsOption(options, "min-election-timeout")) != -1)
            config.getRaft().setMinElectionTimeout(value);
        if ((value = this.parseMillisecondsOption(options, "max-election-timeout")) != -1)
            config.getRaft().setMaxElectionTimeout(value);
        if ((value = this.parseMillisecondsOption(options, "heartbeat-timeout")) != -1)
            config.getRaft().setHeartbeatTimeout(value);
        if ((value = this.parseMillisecondsOption(options, "fallback-check-interval")) != -1)
            config.getFallbackTarget().setCheckInterval(value);
        if ((value = this.parseMillisecondsOption(options, "fallback-check-timeout")) != -1)
            config.getFallbackTarget().setTransactionTimeout(value);
        if ((value = this.parseMillisecondsOption(options, "fallback-min-available")) != -1)
            config.getFallbackTarget().setMinAvailableTime(value);
        if ((value = this.parseMillisecondsOption(options, "fallback-min-unavailable")) != -1)
            config.getFallbackTarget().setMinUnavailableTime(value);

        // Raft fallback options
        if ((arg = this.parseCommandLineOption(options, "--raft-fallback")) != null) {
            final File stateFile = new File(arg);
            if (stateFile.exists() && !stateFile.isFile())
                throw new IllegalArgumentException("file `" + arg + "' is not a regular file");
            config.getFallback().setStateFile(stateFile);
        }
        MergeStrategy mergeStrategy;
        if ((mergeStrategy = this.parseMergeStrategy(options, "unavailable")) != null)
            config.getFallbackTarget().setUnavailableMergeStrategy(mergeStrategy);
        if ((mergeStrategy = this.parseMergeStrategy(options, "rejoin")) != null)
            config.getFallbackTarget().setRejoinMergeStrategy(mergeStrategy);

        // Done
        return config;
    }

    private MergeStrategy parseMergeStrategy(ArrayDeque<String> options, String name) {
        final String className = this.parseCommandLineOption(options, "--raft-fallback-" + name + "-merge");
        if (className == null)
            return null;
        try {
            return (MergeStrategy)Class.forName(className, false, Thread.currentThread().getContextClassLoader()).newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid Raft fallback merge strategy `" + className + "': " + e.getMessage(), e);
        }
    }

    private int parseMillisecondsOption(ArrayDeque<String> options, String name) {
        final String arg = this.parseCommandLineOption(options, "--raft-" + name);
        if (arg == null)
            return -1;
        try {
            final int value = Integer.parseInt(arg, 10);
            if (value < 0)
                throw new NumberFormatException("value cannot be negative");
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid milliseconds value `"
              + arg + "' for `--raft-" + name + "': " + e.getMessage(), e);
        }
    }

    @Override
    public KVDatabase createKVDatabase(Object configuration, KVDatabase kvdb, AtomicKVStore kvstore) {
        final Config config = (Config)configuration;
        final RaftKVDatabase raft = config.configureRaft(kvstore);
        return config.isFallback() ? config.configureFallback(kvdb) : raft;
    }

    @Override
    public boolean requiresAtomicKVStore(Object configuration) {
        return true;
    }

    @Override
    public boolean requiresKVDatabase(Object configuration) {
        return ((Config)configuration).isFallback();
    }

    @Override
    public String getDescription(Object configuration) {
        final Config config = (Config)configuration;
        return "Raft " + config.getRaft().getLogDirectory().getName() + (config.isFallback() ? "/Fallback" : "");
    }

// Config

    private static class Config {

        private final RaftKVDatabase raft = new RaftKVDatabase();
        private final FallbackTarget fallbackTarget = new FallbackTarget();
        private final FallbackKVDatabase fallback = new FallbackKVDatabase();

        private String address;
        private int port = RaftKVDatabase.DEFAULT_TCP_PORT;

        Config(File dir) {
            if (dir == null)
                throw new IllegalArgumentException("null dir");
            this.raft.setLogDirectory(dir);
            this.fallbackTarget.setRaftKVDatabase(this.raft);
            this.fallback.setFallbackTarget(this.fallbackTarget);
        }

        public RaftKVDatabase getRaft() {
            return this.raft;
        }

        public FallbackKVDatabase getFallback() {
            return this.fallback;
        }

        public FallbackTarget getFallbackTarget() {
            return this.fallbackTarget;
        }

        public boolean isFallback() {
            return this.fallback.getStateFile() != null;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public int getPort() {
            return this.port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public RaftKVDatabase configureRaft(AtomicKVStore kvstore) {
            this.raft.setKVStore(kvstore);
            final TCPNetwork network = new TCPNetwork(RaftKVDatabase.DEFAULT_TCP_PORT);
            try {
                network.setListenAddress(this.address != null ?
                  new InetSocketAddress(InetAddress.getByName(this.address), this.port) : new InetSocketAddress(this.port));
            } catch (UnknownHostException e) {
                throw new RuntimeException("can't resolve local Raft address `" + this.address + "'", e);
            }
            this.raft.setNetwork(network);
            return this.raft;
        }

        public FallbackKVDatabase configureFallback(KVDatabase standaloneKV) {
            this.fallback.setStandaloneTarget(standaloneKV);
            return this.fallback;
        }
    }
}
