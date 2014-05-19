
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Map;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.util.ParseContext;

public class GetCommand extends Command {

    public GetCommand() {
        super("get fieldname");
    }

    @Override
    public String getHelpSummary() {
        return "gets a field from incoming objects";
    }

    @Override
    public String getHelpDetail() {
        return "The 'get' command takes one argument which is a field name. The top channel containing objects is replaced"
          + " by a channel containing the content of the specified field in those objects.";
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final String fieldName = (String)params.get("fieldname");
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                final Channel<? extends ObjId> channel = GetCommand.this.pop(session, ObjId.class);
                throw new UnsupportedOperationException("get command not implemented yet");
            }
        };
    }
}

