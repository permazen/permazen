
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.func;

import org.jsimpledb.JTransaction;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.parse.ObjTypeParser;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.SpaceParser;
import org.jsimpledb.parse.expr.AssignmentExprParser;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.expr.Value;

@Function
public class CreateFunction extends AbstractFunction {

    private final SpaceParser spaceParser = new SpaceParser();

    public CreateFunction() {
        super("create");
    }

    @Override
    public String getHelpSummary() {
        return "create a new object instance";
    }

    @Override
    public String getUsage() {
        return "create(type [, version ])";
    }

    @Override
    public String getHelpDetail() {
        return "Creates and returns a new instance of the specified type. The optional `version' parameter forces"
          + " a specific schema version.";
    }

    @Override
    public ParamInfo parseParams(ParseSession session, ParseContext ctx, boolean complete) {

        // Get object type
        if (ctx.tryLiteral(")"))
            throw new ParseException(ctx, "type parameter required");
        final int storageId = new ObjTypeParser().parse(session, ctx, complete).getStorageId();

        // Check for optional version
        this.spaceParser.parse(ctx, complete);
        if (ctx.tryLiteral(")"))
            return new ParamInfo(storageId);
        if (!ctx.tryLiteral(","))
            throw new ParseException(ctx, "expected `,' between function parameters").addCompletion(", ");

        // Get version
        this.spaceParser.parse(ctx, complete);
        final Node version = AssignmentExprParser.INSTANCE.parse(session, ctx, complete);
        this.spaceParser.parse(ctx, complete);

        // Finish parse
        ctx.skipWhitespace();
        if (!ctx.tryLiteral(")"))
            throw new ParseException(ctx, "expected `)'").addCompletion(") ");

        // Done
        return new ParamInfo(storageId, version);
    }

    @Override
    public Value apply(ParseSession session, Object params) {
        final ParamInfo info = (ParamInfo)params;
        final int storageId = info.getStorageId();
        final Node version = info.getVersion();

        // Create object
        final Transaction tx = session.getTransaction();
        final ObjId id = version != null ?
          tx.create(storageId, version.evaluate(session).checkIntegral(session, "create()")) : tx.create(storageId);
        return new Value(session.hasJSimpleDB() ? JTransaction.getCurrent().getJObject(id) : id);
    }

// ParamInfo

    private static class ParamInfo {

        private final int storageId;
        private final Node version;

        ParamInfo(int storageId) {
            this(storageId, null);
        }

        ParamInfo(int storageId, Node version) {
            this.storageId = storageId;
            this.version = version;
        }

        public int getStorageId() {
            return this.storageId;
        }

        public Node getVersion() {
            return this.version;
        }
    }
}

