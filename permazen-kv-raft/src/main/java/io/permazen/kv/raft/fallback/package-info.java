
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

/**
 * A partition-tolerant {@link io.permazen.kv.KVDatabase} that automatically migrates between a clustered
 * {@link io.permazen.kv.raft.RaftKVDatabase} and a private non-clustered "standalone mode"
 * {@link io.permazen.kv.KVDatabase}, based on availability of the Raft cluster.
 *
 * @see io.permazen.kv.raft.fallback.FallbackKVDatabase
 */
package io.permazen.kv.raft.fallback;
