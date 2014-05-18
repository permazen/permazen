
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.util.ParseContext;

public class ObjectCommand extends Command {

    public ObjectCommand() {
        super("object");
    }

    @Override
    public String getUsage() {
        return this.name + " id ...";
    }

    @Override
    public String getHelpSummary() {
        return "access specific objects by object ID";
    }

    @Override
    public String getHelpDetail() {
        return "Pushes a channel onto the stack containing the specified objects.";
    }

    @Override
    public Action parseParameters(Session session, ParseContext ctx, boolean complete) {

        // Parse parameters
        final Map<String, Object> params = new ParamParser(this, "id:objid+").parseParameters(session, ctx, complete);
        final TreeSet<ObjId> ids = new TreeSet<>(Lists.transform((List<?>)params.get("id"), new CastFunction<ObjId>(ObjId.class)));

        // Return instances
        return new Action() {
            @Override
            public void run(Session session) throws Exception {
                ObjectCommand.this.push(session, new ObjectChannel(session) {
                    @Override
                    public TreeSet<ObjId> getItems(Session session) {
                        return ids;
                    }
                });
            }
        };
    }
}

