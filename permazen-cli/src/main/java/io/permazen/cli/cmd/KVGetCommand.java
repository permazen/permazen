
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.Session;
import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVTransaction;
import io.permazen.parse.ParseException;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.ParseContext;

import java.io.PrintWriter;
import java.util.EnumSet;
import java.util.Map;

public class KVGetCommand extends AbstractKVCommand {

    public KVGetCommand() {
        super("kvget -n:novals -s:cstrings -range:range key:bytes maxKey:bytes? limit:int?");
    }

    @Override
    public String getHelpSummary() {
        return "Retrieves one, or a range, of raw database key/value pairs to the console";
    }

    @Override
    public String getHelpDetail() {
        return "Retrieves a single raw database key/value pair, or a range of key/value pairs, to the console.\nIf `-range' is"
          + " not given, a single key/value pair is retrieved. Otherwise, `key' is the minimum key (inclusive) and `maxKey'"
          + " is the maximum key (exclusive) if given, otherwise there is no maximum key. `key' and `maxKey' may be given"
          + " as hexadecimal strings or C-style doubly-quoted strings.\nThe `limit' parameter limits the total number of"
          + " key/value pairs displayed.\nBy default, keys and values are displayed in hexadecimal with an ASCII decoding;"
          + " use the `-s' flag to display keys and values directly as C-style doubly-quoted strings.\nThe `-n' flag causes"
          + " only keys (not values) to be displayed.";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final boolean cstrings = params.containsKey("cstrings");
        final boolean range = params.containsKey("range");
        final boolean novals = params.containsKey("novals");
        final byte[] key = (byte[])params.get("key");
        final byte[] maxKey = (byte[])params.get("maxKey");
        final Integer limit = (Integer)params.get("limit");
        if (maxKey != null && !range)
            throw new ParseException(ctx, "`-range' must be specified to retrieve a range of keys");
        return new GetAction(cstrings, range, novals, key, maxKey, limit);
    }

    private static class GetAction implements CliSession.Action, Session.TransactionalAction {

        private final boolean cstrings;
        private final boolean range;
        private final boolean novals;
        private final byte[] key;
        private final byte[] maxKey;
        private final Integer limit;

        GetAction(boolean cstrings, boolean range, boolean novals, byte[] key, byte[] maxKey, Integer limit) {
            this.cstrings = cstrings;
            this.range = range;
            this.novals = novals;
            this.key = key;
            this.maxKey = maxKey;
            this.limit = limit;
        }

        @Override
        public void run(CliSession session) throws Exception {
            final PrintWriter writer = session.getWriter();
            final KVTransaction kvt = session.getKVTransaction();

            // Handle single key
            if (!this.range) {
                final byte[] value = kvt.get(this.key);
                writer.println(value != null && this.cstrings ? AbstractKVCommand.toCString(value) : ByteUtil.toString(value));
                return;
            }

            // Handle range of keys
            long count = 0;
            try (CloseableIterator<KVPair> i = kvt.getRange(this.key, this.maxKey)) {
                while (i.hasNext()) {
                    final KVPair pair = i.next();
                    if (this.limit != null && count >= this.limit)
                        break;
                    if (this.cstrings) {
                        writer.println("K " + AbstractKVCommand.toCString(pair.getKey()));
                        if (!this.novals)
                            writer.println("V " + AbstractKVCommand.toCString(pair.getValue()));
                    } else {
                        KVGetCommand.decode(writer, "K ", pair.getKey());
                        if (!this.novals)
                            KVGetCommand.decode(writer, "V ", pair.getValue());
                    }
                    count++;
                }
            }
            writer.println("Displayed " + count + " key/value pairs");
        }
    }

    private static void decode(PrintWriter writer, String prefix, byte[] value) {
        for (int i = 0; i < value.length; i += 32) {
            writer.print(prefix);
            for (int j = 0; j < 32; j++) {
                writer.print(i + j < value.length ? String.format("%02x", value[i + j] & 0xff) : "  ");
                if (j % 4 == 3)
                    writer.print(' ');
            }
            writer.print("   ");
            for (int j = 0; j < 32 && i + j < value.length; j++) {
                final int ch = value[i + j] & 0xff;
                writer.print(i + j < value.length ? (ch < 0x20 || ch > 0x7f ? '.' : (char)ch) : ' ');
            }
            writer.println();
        }
    }
}

