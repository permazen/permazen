
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.io.File;
import java.util.EnumSet;
import java.util.Map;

import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.util.ParseContext;

public class SetHistoryFileCommand extends AbstractCommand {

    public SetHistoryFileCommand() {
        super("set-history-file file.txt:file");
    }

    @Override
    public String getHelpSummary() {
        return "Configures the history file used for command line tab-completion.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "file".equals(typeName) ? new InputFileParser() : super.getParser(typeName);
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final File file = (File)params.get("file.txt");
        return new CliSession.TransactionalAction() {
            @Override
            public void run(CliSession session) throws Exception {
                session.getConsole().setHistoryFile(file);
                session.getWriter().println("Updated history file to " + file);
            }
        };
    }
}

