
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

/**
 * A partition-tolerant {@link org.jsimpledb.kv.KVDatabase} that automatically migrates between a clustered
 * {@link org.jsimpledb.kv.raft.RaftKVDatabase} and a private non-clustered "standalone mode"
 * {@link org.jsimpledb.kv.KVDatabase}, based on availability of the Raft cluster.
 *
 * @see org.jsimpledb.kv.raft.fallback.FallbackKVDatabase
 */
package org.jsimpledb.kv.raft.fallback;
