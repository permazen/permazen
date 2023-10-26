
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft;

import io.permazen.kv.CloseableKVStore;
import io.permazen.kv.KVStore;
import io.permazen.kv.mvcc.MutableView;
import io.permazen.kv.mvcc.Writes;
import io.permazen.kv.util.PrefixKVStore;

import java.util.HashMap;
import java.util.Map;

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

    MostRecentView(final RaftKVDatabase raft, final long maxIndex) {

        // Sanity check
        assert raft != null;
        assert Thread.holdsLock(raft);
        assert maxIndex >= 0;

        // Grab a snapshot of the key/value store
        this.snapshot = raft.kv.snapshot();

        // Create a view of just the state machine keys and values and successively layer unapplied log entries up to maxIndex
        KVStore kview = PrefixKVStore.create(snapshot, raft.getStateMachinePrefix());
        this.config = new HashMap<>(raft.log.getLastAppliedConfig());
        long viewIndex = raft.log.getLastAppliedIndex();
        long viewTerm = raft.log.getLastAppliedTerm();
        for (LogEntry logEntry : raft.log.getUnapplied()) {
            if (logEntry.getIndex() > maxIndex)
                break;
            final Writes writes = logEntry.getWrites();
            if (!writes.isEmpty())
                kview = new MutableView(kview, null, writes);
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
