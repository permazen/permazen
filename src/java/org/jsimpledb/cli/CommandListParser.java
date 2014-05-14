
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.ArrayList;
import java.util.List;

import org.jsimpledb.util.ParseContext;

public class CommandListParser {

    public List<Action> parse(Session session, ParseContext ctx) {
        final ArrayList<Action> actions = new ArrayList<>();
        while (true) {
            final Action action = CommandParser.parse(session, ctx);
            if (action == null)
                break;
            actions.add(action);
        }
        return actions;
    }
}

