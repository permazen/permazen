
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

/**
 * Service instance invoked by the Raft service executor.
 */
abstract class Service implements Runnable {

    protected final Role role;
    protected final String desc;

    /**
     * Constructor.
     */
    Service(String desc) {
        this(null, desc);
    }

    /**
     * Constructor.
     */
    Service(final Role role, final String desc) {
        assert desc != null;
        this.role = role;
        this.desc = desc;
    }

    public Role getRole() {
        return this.role;
    }

    @Override
    public String toString() {
        return this.desc;
    }
}

