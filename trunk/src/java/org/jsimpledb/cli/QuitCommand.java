
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

public class QuitCommand extends AbstractSimpleCommand<Void> {

    public QuitCommand() {
        super("quit");
    }

    @Override
    public String getHelpSummary() {
        return "Quits out of the JSimpleDB command line";
    }

    @Override
    protected String getResult(Session session, Channels channels, Void params) {
        session.setDone(true);
        return "Bye";
    }
}

