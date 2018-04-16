
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.xodus;

import com.google.common.base.Preconditions;

import java.util.function.Function;

import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Transaction;

/**
 * Xodus transaction types.
 */
public enum TransactionType implements Function<Environment, Transaction> {

    /**
     * Read-only transaction.
     *
     * @see Environment#beginReadonlyTransaction
     */
    READ_ONLY(true, false, Environment::beginReadonlyTransaction),

    /**
     * Normal read-write transaction.
     *
     * @see Environment#beginTransaction
     */
    READ_WRITE(false, false, Environment::beginTransaction),

    /**
     * Exclusive read-write transaction.
     *
     * @see Environment#beginExclusiveTransaction
     */
    READ_WRITE_EXCLUSIVE(false, true, Environment::beginExclusiveTransaction);

    private final boolean readOnly;
    private final boolean exclusive;
    private final Function<Environment, Transaction> creator;

    TransactionType(boolean readOnly, boolean exclusive, Function<Environment, Transaction> creator) {
        this.readOnly = readOnly;
        this.exclusive = exclusive;
        this.creator = creator;
    }

    public boolean isReadOnly() {
        return this.readOnly;
    }

    public boolean isExclusive() {
        return this.exclusive;
    }

    @Override
    public Transaction apply(Environment env) {
        Preconditions.checkArgument(env != null, "null env");
        return this.creator.apply(env);
    }
}
