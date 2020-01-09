
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

/**
 * Service instance invoked by the Raft service executor.
 */
class Service implements Runnable {

    protected final Role role;
    protected final String desc;
    protected final Runnable action;

    Service(String desc, Runnable action) {
        this(null, desc, action);
    }

    Service(Role role, String desc) {
        this(role, desc, null);
    }

    Service(final Role role, final String desc, Runnable action) {
        assert desc != null;
        this.role = role;
        this.desc = desc;
        this.action = action;
    }

    public Role getRole() {
        return this.role;
    }

    @Override
    public void run() {
        assert this.action != null;
        this.action.run();
    }

    @Override
    public String toString() {
        return this.desc;
    }
}
