
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.kv.KVTransaction;

import java.util.EnumSet;
import java.util.Map;

public class KVRemoveCommand extends AbstractKVCommand {

    public KVRemoveCommand() {
        super("kvremove -range:range key:bytes maxKey:bytes?");
    }

    @Override
    public String getHelpSummary() {
        return "Deletes one, or a range, of raw database key/value pairs";
    }

    @Override
    public String getHelpDetail() {
        return "Deletes a single raw database key/value pair, or a range of key/value pairs. If `-range' is not given,"
          + " the specified key/value pair is deleted. Otherwise, `key' is the minimum key (inclusive) and `maxKey'"
          + " is the maximum key (exclusive) if given, otherwise there is no maximum key. `key' and `maxKey' may be given"
          + " as hexadecimal strings or C-style doubly-quoted strings."
          + "\n\nWARNING: this command can corrupt a Permazen database.";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.of(SessionMode.KEY_VALUE);
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {
        final boolean range = params.containsKey("range");
        final byte[] key = (byte[])params.get("key");
        final byte[] maxKey = (byte[])params.get("maxKey");
        if (maxKey != null && !range)
            throw new IllegalArgumentException("`-range' must be specified to delete a range of keys");
        return new RemoveAction(range, key, maxKey);
    }

    private static class RemoveAction implements Session.RetryableTransactionalAction {

        private final boolean range;
        private final byte[] key;
        private final byte[] maxKey;

        RemoveAction(boolean range, byte[] key, byte[] maxKey) {
            this.range = range;
            this.key = key;
            this.maxKey = maxKey;
        }

        @Override
        public void run(Session session) throws Exception {
            final KVTransaction kvt = session.getKVTransaction();
            if (!this.range)
                kvt.remove(this.key);
            else
                kvt.removeRange(this.key, this.maxKey);
        }
    }
}
