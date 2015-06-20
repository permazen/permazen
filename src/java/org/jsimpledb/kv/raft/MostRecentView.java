
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.raft;

import java.util.HashMap;
import java.util.Map;

import org.jsimpledb.kv.CloseableKVStore;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.mvcc.MutableView;
import org.jsimpledb.kv.mvcc.Writes;
import org.jsimpledb.kv.util.PrefixKVStore;

/**
 * A view of the database based on the most recent log entry, if any, otherwise directly on the committed key/value store.
 * Caller is responsible for eventually closing the snapshot.
 */
class MostRecentView {

    private final CloseableKVStore snapshot;
    private final long term;
    private final long index;
    private final HashMap<String, String> config;
    private final MutableView view;

    public MostRecentView(RaftKVDatabase raft, boolean committed) {
        assert raft != null;
        assert Thread.holdsLock(raft);

        // Grab a snapshot of the key/value store
        this.snapshot = raft.kv.snapshot();

        // Create a view of just the state machine keys and values and successively layer unapplied log entries
        // If we require a committed view, then stop when we get to the first uncomitted log entry
        KVStore kview = PrefixKVStore.create(snapshot, RaftKVDatabase.STATE_MACHINE_PREFIX);
        this.config = new HashMap<>(raft.lastAppliedConfig);
        long viewIndex = raft.lastAppliedIndex;
        long viewTerm = raft.lastAppliedTerm;
        for (LogEntry logEntry : raft.raftLog) {
            if (committed && logEntry.getIndex() > raft.commitIndex)
                break;
            final Writes writes = logEntry.getWrites();
            if (!writes.isEmpty())
                kview = new MutableView(kview, null, logEntry.getWrites());
            logEntry.applyConfigChange(this.config);
            viewIndex = logEntry.getIndex();
            viewTerm = logEntry.getTerm();
        }

        // Finalize
        this.view = new MutableView(kview);
        this.term = viewTerm;
        this.index = viewIndex;
    }

    public long getTerm() {
        return this.term;
    }

    public long getIndex() {
        return this.index;
    }

    public Map<String, String> getConfig() {
        return this.config;
    }

    public CloseableKVStore getSnapshot() {
        return this.snapshot;
    }

    public MutableView getView() {
        return this.view;
    }
}

