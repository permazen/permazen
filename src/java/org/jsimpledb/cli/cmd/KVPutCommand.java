
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.io.PrintWriter;
import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.parse.ParseContext;

@Command(modes = { SessionMode.KEY_VALUE })
public class KVPutCommand extends AbstractKVCommand {

    public KVPutCommand() {
        super("kvput key:bytes value:bytes");
    }

    @Override
    public String getHelpSummary() {
        return "Write a raw database key/value pair";
    }

    @Override
    public String getHelpDetail() {
        return "Writes a raw database key/value pair. The `key' and `value' parameters may be given as a hexadecimal value"
          + " or C-style doubly-quoted string."
          + "\n\nWARNING: this command can corrupt a JSimpleDB database.";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final byte[] key = (byte[])params.get("key");
        final byte[] value = (byte[])params.get("value");
        return new CliSession.Action() {
            @Override
            public void run(CliSession session) throws Exception {
                final PrintWriter writer = session.getWriter();
                final KVTransaction kvt = session.getKVTransaction();
                kvt.put(key, value);
            }
        };
    }
}

