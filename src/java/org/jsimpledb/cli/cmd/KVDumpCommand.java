
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.util.ByteUtil;

@Command(modes = { SessionMode.KEY_VALUE, SessionMode.CORE_API, SessionMode.JSIMPLEDB })
public class KVDumpCommand extends AbstractCommand {

    public KVDumpCommand() {
        super("kvdump minKey? maxKey? limit:int?");
    }

    @Override
    public String getHelpSummary() {
        return "Dumps raw database key/value pairs to the console";
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final String minKeyString = (String)params.get("minKey");
        final String maxKeyString = (String)params.get("maxKey");
        final Integer limit = (Integer)params.get("limit");
        final byte[] minKey = minKeyString != null ? ByteUtil.parse(minKeyString) : null;
        final byte[] maxKey = maxKeyString != null ? ByteUtil.parse(maxKeyString) : null;
        return new CliSession.Action() {
            @Override
            public void run(CliSession session) throws Exception {
                final PrintWriter writer = session.getWriter();
                final KVTransaction kvt = session.getKVTransaction();
                long count = 0;
                for (Iterator<KVPair> i = kvt.getRange(minKey, maxKey, false); i.hasNext(); ) {
                    final KVPair pair = i.next();
                    if (limit != null && count >= limit)
                        break;
                    KVDumpCommand.this.print(writer, "K ", pair.getKey());
                    KVDumpCommand.this.print(writer, "V ", pair.getValue());
                    count++;
                }
                writer.println("Displayed " + count + " key/value pairs");
            }
        };
    }

    private void print(PrintWriter writer, String prefix, byte[] value) {
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

