
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import com.google.common.base.Preconditions;

import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVTransaction;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;

import java.io.PrintStream;
import java.util.EnumSet;
import java.util.Iterator;
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
        return
            "Retrieves a single raw database key/value pair, or a range of key/value pairs, to the console.\n"
         + "\n"
         + "Without \"-range\", a single key/value pair is retrieved. Otherwise, \"key\" is the minimum key (inclusive)\n"
         + "and \"maxKey\", if any, is the maximum key (exclusive), otherwise \"key\" because the range prefix.\n"
         + "\n"
         + "The \"key\" and \"maxKey\" may be given as hexadecimal strings or C-style doubly-quoted strings.\n"
         + "\n"
         + "The \"limit\" parameter limits the total number of key/value pairs displayed.\n"
         + "\n"
         + "By default, keys and values are displayed in hexadecimal with an ASCII decoding; use the \"-s\" flag\n"
         + "to display keys and values directly as C-style doubly-quoted strings.\n"
         + "\n"
         + "The \"-n\" flag causes only keys to be displayed (i.e., no values).";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {
        final boolean cstrings = params.containsKey("cstrings");
        final boolean range = params.containsKey("range");
        final boolean novals = params.containsKey("novals");
        final ByteData key = (ByteData)params.get("key");
        final ByteData maxKey = (ByteData)params.get("maxKey");
        final Integer limit = (Integer)params.get("limit");
        if (maxKey != null && !range)
            throw new IllegalArgumentException("\"-range\" must be specified to retrieve a range of keys");
        return new GetAction(cstrings, range, novals, key, maxKey, limit);
    }

    private static class GetAction implements KVAction {

        private final boolean cstrings;
        private final boolean range;
        private final boolean novals;
        private final ByteData key;
        private final ByteData maxKey;
        private final Integer limit;

        GetAction(boolean cstrings, boolean range, boolean novals, ByteData key, ByteData maxKey, Integer limit) {
            this.cstrings = cstrings;
            this.range = range;
            this.novals = novals;
            this.key = key;
            this.maxKey = maxKey != null || !range ? maxKey :
              !key.isEmpty() ? ByteUtil.getKeyAfterPrefix(key) : null;
            this.limit = limit;
        }

        @Override
        public void run(Session session) throws Exception {
            final PrintStream writer = session.getOutput();
            final KVTransaction kvt = session.getKVTransaction();

            // Handle single key
            if (!this.range) {
                final ByteData value = kvt.get(this.key);
                writer.println(value != null && this.cstrings ? AbstractKVCommand.toCString(value) : ByteUtil.toString(value));
                return;
            }

            // Handle range of keys
            final long count;
            try (CloseableIterator<KVPair> i = kvt.getRange(this.key, this.maxKey)) {
                count = KVGetCommand.dumpRange(writer, i, this.cstrings, this.novals,
                  this.limit != null ? this.limit : Long.MAX_VALUE);
            }
            writer.println("Displayed " + count + " key/value pairs");
        }
    }

    /**
     * Print a hex and ASCII decode of the given key/value pairs.
     *
     * @param writer print output
     * @param iter key/value pair iteration
     * @return the number of key/value pairs printed
     * @throws IllegalArgumentException if {@code writer} or {@code iter} is null
     */
    public static long dumpRange(PrintStream writer, Iterator<KVPair> iter) {
        return KVGetCommand.dumpRange(writer, iter, false, false, Long.MAX_VALUE);
    }

    /**
     * Print a hex and ASCII decode of the given key/value pairs.
     *
     * @param writer print output
     * @param iter key/value pair iteration
     * @param cstrings decode keys and values as strings
     * @param novalues only print the keys, omitting the values
     * @param limit stop after this many key/value pairs
     * @return the number of key/value pairs printed
     * @throws IllegalArgumentException if {@code limit} is negative
     * @throws IllegalArgumentException if {@code writer} or {@code iter} is null
     */
    public static long dumpRange(PrintStream writer, Iterator<KVPair> iter, boolean cstrings, boolean novalues, long limit) {
        Preconditions.checkArgument(writer != null, "null writer");
        Preconditions.checkArgument(iter != null, "null iter");
        Preconditions.checkArgument(limit >= 0, "negative limit");
        long count = 0;
        while (iter.hasNext()) {
            final KVPair pair = iter.next();
            if (count >= limit)
                break;
            if (cstrings) {
                writer.println("K " + AbstractKVCommand.toCString(pair.getKey()));
                if (!novalues)
                    writer.println("V " + AbstractKVCommand.toCString(pair.getValue()));
            } else {
                KVGetCommand.decode(writer, "K ", pair.getKey());
                if (!novalues)
                    KVGetCommand.decode(writer, "V ", pair.getValue());
            }
            count++;
        }
        return count;
    }

    private static void decode(PrintStream writer, String prefix, ByteData value) {
        for (int i = 0; i < value.size(); i += 32) {
            writer.print(prefix);
            for (int j = 0; j < 32; j++) {
                writer.print(i + j < value.size() ? String.format("%02x", value.ubyteAt(i + j)) : "  ");
                if (j % 4 == 3)
                    writer.print(' ');
            }
            writer.print("   ");
            for (int j = 0; j < 32 && i + j < value.size(); j++) {
                final int ch = value.ubyteAt(i + j);
                writer.print(i + j < value.size() ? (ch < 0x20 || ch > 0x7f ? '.' : (char)ch) : ' ');
            }
            writer.println();
        }
    }
}
