
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ParseContext;

@Command(modes = { SessionMode.KEY_VALUE, SessionMode.CORE_API, SessionMode.JSIMPLEDB })
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
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final boolean cstrings = params.containsKey("cstrings");
        final boolean range = params.containsKey("range");
        final boolean novals = params.containsKey("novals");
        final byte[] key = (byte[])params.get("key");
        final byte[] maxKey = (byte[])params.get("maxKey");
        final Integer limit = (Integer)params.get("limit");
        if (maxKey != null && !range)
            throw new ParseException(ctx, "`-range' must be specified to retrieve a range of keys");
        return new CliSession.TransactionalAction() {
            @Override
            public void run(CliSession session) throws Exception {
                final PrintWriter writer = session.getWriter();
                final KVTransaction kvt = session.getKVTransaction();

                // Handle single key
                if (!range) {
                    final byte[] value = kvt.get(key);
                    writer.println(value != null && cstrings ? AbstractKVCommand.toCString(value) : ByteUtil.toString(value));
                    return;
                }

                // Handle range of keys
                long count = 0;
                for (Iterator<KVPair> i = kvt.getRange(key, maxKey, false); i.hasNext(); ) {
                    final KVPair pair = i.next();
                    if (limit != null && count >= limit)
                        break;
                    if (cstrings) {
                        writer.println("K " + AbstractKVCommand.toCString(pair.getKey()));
                        if (!novals)
                            writer.println("V " + AbstractKVCommand.toCString(pair.getValue()));
                    } else {
                        KVGetCommand.this.decode(writer, "K ", pair.getKey());
                        if (!novals)
                            KVGetCommand.this.decode(writer, "V ", pair.getValue());
                    }
                    count++;
                }
                writer.println("Displayed " + count + " key/value pairs");
            }
        };
    }

    private void decode(PrintWriter writer, String prefix, byte[] value) {
        for (int i = 0; i < value.length; i += 32) {
            writer.print(prefix);
            for (int j = 0; j < 32; j++) {
                writer.print(i + j < value.length ? String.format("%02x", value[i + j] & 0xff) : "  ");
                if (j % 4 == 3)
                    writer.print(' ');
            }
            writer.print("   ");
            for (int j = 0; j < 32 && i + j < value.length; j++) {
                final int ch = value[i + j];
                writer.print(i + j < value.length ? (ch < 0x20 || ch > 0x7f ? '.' : (char)ch) : ' ');
            }
            writer.println();
        }
    }
}

