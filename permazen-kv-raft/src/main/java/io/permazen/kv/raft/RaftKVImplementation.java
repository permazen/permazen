
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVDatabase;
import io.permazen.kv.KVImplementation;
import io.permazen.kv.mvcc.AtomicKVStore;
import io.permazen.kv.raft.fallback.FallbackKVDatabase;
import io.permazen.kv.raft.fallback.FallbackTarget;
import io.permazen.kv.raft.fallback.MergeStrategy;
import io.permazen.kv.raft.fallback.NullMergeStrategy;
import io.permazen.kv.raft.fallback.OverwriteMergeStrategy;
import io.permazen.util.ApplicationClassLoader;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Optional;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.dellroad.stuff.net.TCPNetwork;

public class RaftKVImplementation implements KVImplementation<RaftKVImplementation.Config> {

    private OptionSpec<File> directoryOption;
    private OptionSpec<String> minElectionTimeoutOption;
    private OptionSpec<String> maxElectionTimeoutOption;
    private OptionSpec<String> heartbeatTimeoutOption;
    private OptionSpec<String> identityOption;
    private OptionSpec<String> addressOption;
    private OptionSpec<String> portOption;
    private OptionSpec<File> fallbackOption;
    private OptionSpec<String> fallbackCheckIntervalOption;
    private OptionSpec<String> fallbackCheckTimeoutOption;
    private OptionSpec<String> fallbackMinAvailableOption;
    private OptionSpec<String> fallbackMinUnavailableOption;
    private OptionSpec<String> fallbackUnavailableMergeOption;
    private OptionSpec<String> fallbackUnavailableRejoinOption;

    @Override
    public void addOptions(OptionParser parser) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.directoryOption == null, "duplicate option");
        Preconditions.checkState(this.minElectionTimeoutOption == null, "duplicate option");
        Preconditions.checkState(this.maxElectionTimeoutOption == null, "duplicate option");
        Preconditions.checkState(this.heartbeatTimeoutOption == null, "duplicate option");
        Preconditions.checkState(this.identityOption == null, "duplicate option");
        Preconditions.checkState(this.addressOption == null, "duplicate option");
        Preconditions.checkState(this.portOption == null, "duplicate option");
        Preconditions.checkState(this.fallbackOption == null, "duplicate option");
        Preconditions.checkState(this.fallbackCheckIntervalOption == null, "duplicate option");
        Preconditions.checkState(this.fallbackCheckTimeoutOption == null, "duplicate option");
        Preconditions.checkState(this.fallbackMinAvailableOption == null, "duplicate option");
        Preconditions.checkState(this.fallbackMinUnavailableOption == null, "duplicate option");
        Preconditions.checkState(this.fallbackUnavailableMergeOption == null, "duplicate option");
        Preconditions.checkState(this.fallbackUnavailableRejoinOption == null, "duplicate option");
        this.directoryOption = parser.accepts("raft", "Use Raft key/value database (requires key/value store)")
          .withRequiredArg()
          .describedAs("directory")
          .ofType(File.class);
        this.minElectionTimeoutOption = parser.accepts("raft-min-election-timeout",
            String.format("Specify Raft minimum election timeout in ms (default %d)", RaftKVDatabase.DEFAULT_MIN_ELECTION_TIMEOUT))
          .availableIf(this.directoryOption)
          .withRequiredArg()
          .describedAs("millis");
        this.maxElectionTimeoutOption = parser.accepts("raft-max-election-timeout",
            String.format("Specify Raft maximum election timeout in ms (default %d)", RaftKVDatabase.DEFAULT_MAX_ELECTION_TIMEOUT))
          .availableIf(this.directoryOption)
          .withRequiredArg()
          .describedAs("millis");
        this.heartbeatTimeoutOption = parser.accepts("raft-heartbeat-timeout",
            String.format("Specify Raft leader heartbeat timeout in ms (default %d)", RaftKVDatabase.DEFAULT_HEARTBEAT_TIMEOUT))
          .availableIf(this.directoryOption)
          .withRequiredArg()
          .describedAs("millis");
        this.identityOption = parser.accepts("raft-identity", "Specify Raft identity string")
          .availableIf(this.directoryOption)
          .withRequiredArg()
          .describedAs("string");
        this.addressOption = parser.accepts("raft-address", "Specify Specify local Raft node's IP address")
          .availableIf(this.directoryOption)
          .withRequiredArg()
          .describedAs("ipaddr[:port]");
        this.portOption = parser.accepts("raft-port",
            String.format("Specify Specify local Raft node's TCP port (default %d)", RaftKVDatabase.DEFAULT_TCP_PORT))
          .availableIf(this.directoryOption)
          .withRequiredArg()
          .describedAs("port");
        this.fallbackOption = parser.accepts("raft-fallback", "Use Raft fallback database with specified state file")
          .availableIf(this.directoryOption)
          .withRequiredArg()
          .describedAs("file")
          .ofType(File.class);
        this.fallbackCheckIntervalOption = parser.accepts("raft-fallback-check-interval",
            String.format("Specify Raft fallback check interval in milliseconds (default %d)",
              FallbackTarget.DEFAULT_CHECK_INTERVAL))
          .availableIf(this.fallbackOption)
          .withRequiredArg()
          .describedAs("millis");
        this.fallbackCheckTimeoutOption = parser.accepts("raft-fallback-check-timeout",
            String.format("Specify Raft fallback availability check TX timeout in milliseconds (default %d)",
             FallbackTarget.DEFAULT_TRANSACTION_TIMEOUT))
          .availableIf(this.fallbackOption)
          .withRequiredArg()
          .describedAs("millis");
        this.fallbackMinAvailableOption = parser.accepts("raft-fallback-min-available",
            String.format("Specify Raft fallback min available time in milliseconds (default %d)",
              FallbackTarget.DEFAULT_MIN_AVAILABLE_TIME))
          .availableIf(this.fallbackOption)
          .withRequiredArg()
          .describedAs("millis");
        this.fallbackMinUnavailableOption = parser.accepts("raft-fallback-min-unavailable",
            String.format("Specify Raft fallback min unavailable time in milliseconds (default %d)",
              FallbackTarget.DEFAULT_MIN_UNAVAILABLE_TIME))
          .availableIf(this.fallbackOption)
          .withRequiredArg()
          .describedAs("millis");
        this.fallbackUnavailableMergeOption = parser.accepts("raft-fallback-unavailable-merge",
            String.format("Specify Raft fallback unavailable merge strategy class name (default \"%s\")",
              OverwriteMergeStrategy.class.getName()))
          .availableIf(this.fallbackOption)
          .withRequiredArg()
          .describedAs("class-name");
        this.fallbackUnavailableRejoinOption = parser.accepts("raft-fallback-rejoin-merge",
            String.format("Specify Raft fallback rejoin merge strategy class name (default \"%s\")",
              NullMergeStrategy.class.getName()))
          .availableIf(this.fallbackOption)
          .withRequiredArg()
          .describedAs("class-name");
    }

    @Override
    public Config buildConfig(OptionSet options) {
        final File dir = options.valueOf(this.directoryOption);
        if (dir == null)
            return null;
        if (dir.exists() && !dir.isDirectory())
            throw new IllegalArgumentException(String.format("file \"%s\" is not a directory", dir));
        final Config config = new Config(dir);
        Optional.ofNullable(options.valueOf(this.identityOption))
          .ifPresent(config.getRaft()::setIdentity);
        Optional.ofNullable(options.valueOf(this.addressOption))
          .ifPresent(address -> {
            config.setAddress(TCPNetwork.parseAddressPart(address));
            config.setPort(TCPNetwork.parsePortPart(address, config.getPort()));
          });
        Optional.ofNullable(options.valueOf(this.portOption))
          .ifPresent(arg -> {
            final int port = TCPNetwork.parsePortPart("x:" + arg, -1);
            if (port == -1)
                throw new IllegalArgumentException("invalid TCP port \"" + arg + "\"");
            config.setPort(port);
          });
        Optional.ofNullable(options.valueOf(this.minElectionTimeoutOption))
          .map(this::parseMillisecondsOption)
          .ifPresent(config.getRaft()::setMinElectionTimeout);
        Optional.ofNullable(options.valueOf(this.maxElectionTimeoutOption))
          .map(this::parseMillisecondsOption)
          .ifPresent(config.getRaft()::setMaxElectionTimeout);
        Optional.ofNullable(options.valueOf(this.heartbeatTimeoutOption))
          .map(this::parseMillisecondsOption)
          .ifPresent(config.getRaft()::setHeartbeatTimeout);
        final File fallbackFile = options.valueOf(this.fallbackOption);
        if (fallbackFile != null) {
            if (fallbackFile.exists() && !fallbackFile.isFile())
                throw new IllegalArgumentException(String.format("file \"%s\" is not a regular file", fallbackFile));
            config.getFallback().setStateFile(fallbackFile);
        }
        Optional.ofNullable(options.valueOf(this.fallbackCheckIntervalOption))
          .map(this::parseMillisecondsOption)
          .ifPresent(config.getFallbackTarget()::setCheckInterval);
        Optional.ofNullable(options.valueOf(this.fallbackCheckTimeoutOption))
          .map(this::parseMillisecondsOption)
          .ifPresent(config.getFallbackTarget()::setTransactionTimeout);
        Optional.ofNullable(options.valueOf(this.fallbackMinAvailableOption))
          .map(this::parseMillisecondsOption)
          .ifPresent(config.getFallbackTarget()::setMinAvailableTime);
        Optional.ofNullable(options.valueOf(this.fallbackMinUnavailableOption))
          .map(this::parseMillisecondsOption)
          .ifPresent(config.getFallbackTarget()::setMinUnavailableTime);
        Optional.ofNullable(options.valueOf(this.fallbackUnavailableMergeOption))
          .map(this::parseMergeStrategy)
          .ifPresent(config.getFallbackTarget()::setUnavailableMergeStrategy);
        Optional.ofNullable(options.valueOf(this.fallbackUnavailableRejoinOption))
          .map(this::parseMergeStrategy)
          .ifPresent(config.getFallbackTarget()::setRejoinMergeStrategy);
        return config;
    }

    private MergeStrategy parseMergeStrategy(String className) {
        try {
            return (MergeStrategy)Class.forName(className, false, ApplicationClassLoader.getInstance())
              .getConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid Raft fallback strategy class \"" + className + "\": " + e.getMessage(), e);
        }
    }

    private int parseMillisecondsOption(String string) {
        try {
            final int value = Integer.parseInt(string, 10);
            if (value < 0)
                throw new NumberFormatException("value cannot be negative");
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("invalid milliseconds value \"%s\": %s", string, e.getMessage()), e);
        }
    }

    @Override
    public boolean providesKVDatabase(Config config) {
        return true;
    }

    @Override
    public KVDatabase createKVDatabase(Config config, KVDatabase kvdb, AtomicKVStore kvstore) {
        final RaftKVDatabase raft = config.configureRaft(kvstore);
        return config.isFallback() ? config.configureFallback(kvdb) : raft;
    }

    @Override
    public boolean requiresAtomicKVStore(Config config) {
        return true;
    }

    @Override
    public boolean requiresKVDatabase(Config config) {
        return config.isFallback();
    }

    @Override
    public String getDescription(Config config) {
        return "Raft " + config.getRaft().getLogDirectory().getName() + (config.isFallback() ? "/Fallback" : "");
    }

// Config

    public static class Config {

        private final RaftKVDatabase raft = new RaftKVDatabase();
        private final FallbackTarget fallbackTarget = new FallbackTarget();
        private final FallbackKVDatabase fallback = new FallbackKVDatabase();

        private String address;
        private int port = RaftKVDatabase.DEFAULT_TCP_PORT;

        public Config(File dir) {
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
                throw new RuntimeException("can't resolve local Raft address \"" + this.address + "\"", e);
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
