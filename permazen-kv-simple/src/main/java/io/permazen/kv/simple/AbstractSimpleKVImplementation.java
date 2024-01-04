
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.simple;

import com.google.common.base.Preconditions;

import io.permazen.kv.KVImplementation;

import java.util.Optional;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

abstract class AbstractSimpleKVImplementation<C extends AbstractSimpleKVImplementation.Config> implements KVImplementation<C> {

    private OptionSpec<String> waitTimeoutOption;
    private OptionSpec<String> holdTimeoutOption;

    protected void addSimpleOptions(OptionParser parser, OptionSpec<?> enablingOption, String prefix) {
        Preconditions.checkArgument(parser != null, "null parser");
        Preconditions.checkState(this.waitTimeoutOption == null, "duplicate option");
        Preconditions.checkState(this.holdTimeoutOption == null, "duplicate option");
        this.waitTimeoutOption = parser.accepts(prefix + "-wait-timeout",
            String.format("Specify locking wait timeout in ms (default %d)", SimpleKVDatabase.DEFAULT_WAIT_TIMEOUT))
          .availableIf(enablingOption)
          .withRequiredArg()
          .describedAs("millis");
        this.holdTimeoutOption = parser.accepts(prefix + "-hold-timeout",
            String.format("Specify locking hold timeout in ms (default %d)", SimpleKVDatabase.DEFAULT_HOLD_TIMEOUT))
          .availableIf(enablingOption)
          .withRequiredArg()
          .describedAs("millis");
    }

    protected void applySimpleOptions(OptionSet options, C config) {
        Optional.ofNullable(options.valueOf(this.waitTimeoutOption))
          .map(this::parseMillisecondsOption)
          .ifPresent(config::setWaitTimeout);
        Optional.ofNullable(options.valueOf(this.holdTimeoutOption))
          .map(this::parseMillisecondsOption)
          .ifPresent(config::setHoldTimeout);
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

// Config

    public static class Config {

        private int holdTimeout = -1;
        private int waitTimeout = -1;

        public void setHoldTimeout(int holdTimeout) {
            this.holdTimeout = holdTimeout;
        }

        public void setWaitTimeout(int waitTimeout) {
            this.waitTimeout = waitTimeout;
        }

        public void applyTo(SimpleKVDatabase kvdb) {
            if (this.holdTimeout != -1)
                kvdb.setHoldTimeout(this.holdTimeout);
            if (this.waitTimeout != -1)
                kvdb.setWaitTimeout(this.waitTimeout);
        }
    }
}
