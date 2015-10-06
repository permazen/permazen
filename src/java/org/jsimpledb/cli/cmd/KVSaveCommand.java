
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.dellroad.stuff.io.AtomicUpdateFileOutputStream;
import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.kv.util.XMLSerializer;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.Parser;

@Command(modes = { SessionMode.KEY_VALUE, SessionMode.CORE_API, SessionMode.JSIMPLEDB })
public class KVSaveCommand extends AbstractCommand {

    public KVSaveCommand() {
        super("kvsave -i:indent file.xml:file minKey? maxKey? limit?");
    }

    @Override
    public String getHelpSummary() {
        return "Exports key/value pairs to an XML file";
    }

    @Override
    public String getHelpDetail() {
        return "Writes all key/value pairs to the specified XML file. Data can be read back in later via `kvload'."
          + "\n\nIf `minKey', `maxKey', and/or `limit' are specified, the keys are restricted to the specified range"
          + " and/or count limit. `minKey' and `maxKey' may be given as hexadecimal strings or C-style doubly-quoted strings."
          + " The `-i' flag causes the output XML to be indented.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "file".equals(typeName) ? new OutputFileParser() : super.getParser(typeName);
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {

        // Parse parameters
        final File file = (File)params.get("file.xml");
        final boolean indent = params.containsKey("indent");
        final byte[] key = (byte[])params.get("key");
        final byte[] maxKey = (byte[])params.get("maxKey");
        final Integer limit = (Integer)params.get("limit");

        // Return action
        return new CliSession.TransactionalAction() {
            @Override
            public void run(CliSession session) throws Exception {
                final AtomicUpdateFileOutputStream updateOutput = new AtomicUpdateFileOutputStream(file);
                final BufferedOutputStream output = new BufferedOutputStream(updateOutput);
                boolean success = false;
                final int count;
                try {
                    final XMLSerializer serializer = new XMLSerializer(session.getKVTransaction());
                    count = serializer.write(output, indent);
                    output.flush();
                    success = true;
                } finally {
                    if (success) {
                        try {
                            output.close();
                        } catch (IOException e) {
                            // ignore
                        }
                    } else
                        updateOutput.cancel();
                }
                session.getWriter().println("Wrote " + count + " key/value pairs to `" + file + "'");
            }
        };
    }
}

