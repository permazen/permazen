
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

/**
 * Service instance invoked by the Raft service executor.
 */
abstract class Service implements Runnable {

    protected final Role role;
    protected final String desc;

    /**
     * Constructor.
     */
    public Service(String desc) {
        this(null, desc);
    }

    /**
     * Constructor.
     */
    public Service(Role role, String desc) {
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

