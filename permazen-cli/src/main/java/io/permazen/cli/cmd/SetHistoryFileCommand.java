
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.parse.Parser;
import io.permazen.util.ParseContext;

import java.io.File;
import java.util.EnumSet;
import java.util.Map;

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
        return "file".equals(typeName) ? new OutputFileParser() : super.getParser(typeName);
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    public CliSession.Action getAction(CliSession session0, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final File file = (File)params.get("file.txt");
        return session -> {
            session.getConsole().setHistoryFile(file);
            session.getWriter().println("Updated history file to " + file);
        };
    }
}

